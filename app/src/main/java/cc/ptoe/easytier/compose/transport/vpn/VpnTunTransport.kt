package cc.ptoe.easytier.compose.transport.vpn

import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.net.VpnService
import android.util.Log
import cc.ptoe.easytier.compose.R
import androidx.core.content.ContextCompat
import cc.ptoe.easytier.compose.data.EasyTierProfile
import cc.ptoe.easytier.compose.data.GlobalSettings
import cc.ptoe.easytier.compose.data.RuntimeState
import cc.ptoe.easytier.compose.data.RuntimeStatus
import cc.ptoe.easytier.compose.transport.RuntimeTransport
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class VpnTunTransport(
    private val context: Context,
    permissionRequester: VpnPermissionRequester,
) : RuntimeTransport {
    // @Volatile: the requester is swapped at runtime when the app moves from a
    // background (boot, no-op) to a foreground (MainActivity, consent dialog)
    // context, so a late start() from the boot receiver must see the latest one.
    @Volatile private var permissionRequester: VpnPermissionRequester = permissionRequester
    private val mutableStatus = MutableStateFlow(RuntimeStatus.Stopped)
    override val status: StateFlow<RuntimeStatus> = mutableStatus.asStateFlow()

    /** Replaces the permission requester (e.g. swap boot no-op for the activity one). */
    fun updatePermissionRequester(requester: VpnPermissionRequester) {
        permissionRequester = requester
    }

    // Guards against establishWhenResolved racing with stop(): once running flips to
    // false, no further startForegroundService intents are posted, so stopService()
    // cannot be re-activated by a late poll iteration.
    @Volatile private var running: Boolean = false
    // Tracks the last established VPN state so we only re-establish when the address
    // or routes actually change, instead of re-calling startForegroundService every
    // 2 seconds from the poll loop.
    private var establishedIpv4Cidr: String? = null
    private var establishedRoutes: List<String> = emptyList()
    // MTU lives on the (effective) profile; cached at start() so the poll loop's
    // establishWhenResolved() calls can reuse it without threading settings through.
    private var activeMtu: Int = DEFAULT_MTU

    override suspend fun start(profile: EasyTierProfile, toml: String, globalSettings: GlobalSettings): RuntimeStatus {
        running = true
        activeMtu = profile.mtu
        establishedIpv4Cidr = null
        establishedRoutes = emptyList()
        val ipv4Cidr = profile.virtualIpv4?.takeIf { !profile.dhcp }
        val prepared = VpnService.prepare(context)
        if (prepared != null) {
            // Synchronously request VPN permission. In the foreground this launches
            // the system consent dialog via the injected requester; from a background
            // context (e.g. boot) the no-op requester returns false and we surface
            // an ERROR — the user must open the app once to grant permission.
            mutableStatus.value = RuntimeStatus(RuntimeState.STARTING, profile.id, profile.tunMode, null, null, null)
            val granted = permissionRequester.request(prepared)
            if (!granted) {
                return RuntimeStatus(RuntimeState.ERROR, profile.id, profile.tunMode, null, null, context.getString(R.string.error_vpn_permission_denied))
                    .also { running = false; mutableStatus.value = it }
            }
        }
        // 静态 IP 场景首次建接口就带上出口节点的默认路由，避免轮询修正前的
        // 短暂窗口里流量走本机网络（查 IP 仍是本机 IP / IPv6 泄漏）。
        val initialRoutes = if (profile.exitNodes.any { it.isNotBlank() }) listOf(DEFAULT_ROUTE_V4) else emptyList()
        return startPrepared(profile, ipv4Cidr, initialRoutes)
    }

    /**
     * Adopts an already-running VPN session started by another coordinator instance
     * (e.g. BootCompletedReceiver). EasyTierVpnService FGS is already running with the
     * VPN interface established, so we restore [running] and the established address/
     * routes to prevent establishWhenResolved() from redundantly re-launching the FGS
     * with the same parameters.
     */
    fun adopt(profile: EasyTierProfile, ipv4Cidr: String?, routes: List<String>): RuntimeStatus {
        running = true
        establishedIpv4Cidr = ipv4Cidr
        establishedRoutes = routes
        val state = if (ipv4Cidr.isNullOrBlank()) RuntimeState.STARTING else RuntimeState.RUNNING
        val tunDevice = if (ipv4Cidr.isNullOrBlank()) null else context.getString(R.string.vpn_device_name)
        return RuntimeStatus(state, profile.id, profile.tunMode, ipv4Cidr, tunDevice, null)
            .also { mutableStatus.value = it }
    }

    fun establishWhenResolved(profile: EasyTierProfile, ipv4Cidr: String, routes: List<String>): RuntimeStatus {
        if (!running) return mutableStatus.value
        // Only re-establish the VPN interface when the address or routes actually change,
        // avoiding redundant startForegroundService calls that race with stop().
        if (ipv4Cidr == establishedIpv4Cidr && routes == establishedRoutes) {
            return mutableStatus.value
        }
        establishedIpv4Cidr = ipv4Cidr
        establishedRoutes = routes
        ContextCompat.startForegroundService(
            context,
            // 「启用 IPv6」关闭时整机禁用 IPv6（::/0 收进隧道丢弃）；开启时若处于
            // 出口节点模式，VpnService 内部也会按默认路由自动接管 IPv6。
            EasyTierVpnService.intent(context, profile.id, ipv4Cidr, routes, profile.enableMagicDns, activeMtu, !profile.enableIpv6),
        )
        return RuntimeStatus(RuntimeState.RUNNING, profile.id, profile.tunMode, ipv4Cidr, context.getString(R.string.vpn_device_name), null)
            .also { mutableStatus.value = it }
    }

    override suspend fun stop() {
        // 没有建立过接口时无需做残留检测（例如 start() 内部先调一次 stopLocked 清理旧会话）。
        val hadSession = establishedIpv4Cidr != null || establishedRoutes.isNotEmpty()
        running = false
        establishedIpv4Cidr = null
        establishedRoutes = emptyList()
        Log.i(TAG, "stop: stopping EasyTierVpnService (hadSession=$hadSession)")
        context.stopService(Intent(context, EasyTierVpnService::class.java))
        // stopService() 是异步的，而且关键问题在于：TUN fd 通过 setTunFd() 交给了
        // 原生 EasyTier，原生侧对 fd 另有引用。仅关闭 Java 侧 ParcelFileDescriptor
        // 时，内核 tun 设备可能仍然存活，VPN 网络配置（0.0.0.0/0 默认路由、DNS
        // 覆盖）就被保留下来 —— 用户看到的现象就是「断开后整机断网，只有杀掉 App
        // 进程才恢复」（进程退出才释放原生 fd）。
        //
        // 这里不依赖原生层：轮询确认 EasyTier 的 VPN 接口是否真的消失，没消失就
        // 下发 ACTION_PURGE，让 Service 重新 establish 一个空接口强制注销旧配置。
        if (!hadSession) {
            mutableStatus.value = RuntimeStatus.Stopped
            return
        }
        repeat(PURGE_ATTEMPTS) { attempt ->
            delay(PURGE_INTERVAL_MS)
            if (!hasOwnVpnInterface()) {
                Log.i(TAG, "stop: VPN interface released after ${(attempt + 1) * PURGE_INTERVAL_MS}ms")
                mutableStatus.value = RuntimeStatus.Stopped
                return
            }
            Log.w(TAG, "stop: EasyTier VPN interface still present (attempt ${attempt + 1}/$PURGE_ATTEMPTS), forcing purge")
            runCatching {
                ContextCompat.startForegroundService(
                    context,
                    Intent(context, EasyTierVpnService::class.java).setAction(EasyTierVpnService.ACTION_PURGE),
                )
            }.onFailure { Log.w(TAG, "stop: purge request failed: ${it.message}") }
        }
        mutableStatus.value = RuntimeStatus.Stopped
    }

    /**
     * 判断本 App 的 VPN 接口是否仍然挂在系统上。
     *
     * 用 fd00::1 作为指纹：EasyTierVpnService.createInterface() 总会给接口加
     * fd00::1/128，而其他 VPN（Tailscale / Clash 等）不会，因此不会误判。
     */
    private fun hasOwnVpnInterface(): Boolean = runCatching {
        val manager = context.getSystemService(ConnectivityManager::class.java) ?: return false
        manager.allNetworks.any { network ->
            val capabilities = manager.getNetworkCapabilities(network) ?: return@any false
            if (!capabilities.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) return@any false
            val linkProperties = manager.getLinkProperties(network) ?: return@any false
            linkProperties.linkAddresses.any { it.address.hostAddress?.trim() == VPN_MARKER_ADDRESS }
        }
    }.getOrDefault(false)

    private fun startPrepared(profile: EasyTierProfile, ipv4Cidr: String?, routes: List<String>): RuntimeStatus {
        if (ipv4Cidr == null) return RuntimeStatus(RuntimeState.STARTING, profile.id, profile.tunMode, null, null, null)
            .also { mutableStatus.value = it }
        return establishWhenResolved(profile, ipv4Cidr, routes)
    }

    private companion object {
        const val TAG = "VpnTunTransport"
        const val DEFAULT_ROUTE_V4 = "0.0.0.0/0"
        const val DEFAULT_MTU = 1380
        const val VPN_MARKER_ADDRESS = "fd00::1"
        const val PURGE_ATTEMPTS = 4
        const val PURGE_INTERVAL_MS = 250L
    }
}
