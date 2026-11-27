package cc.ptoe.easytier.compose.core
import androidx.annotation.StringRes

import android.content.Context
import android.util.Log
import cc.ptoe.easytier.compose.data.EasyTierProfile
import cc.ptoe.easytier.compose.R
import cc.ptoe.easytier.compose.data.GlobalSettings
import cc.ptoe.easytier.compose.data.RuntimeState
import cc.ptoe.easytier.compose.data.RuntimeStatus
import cc.ptoe.easytier.compose.data.TunMode
import cc.ptoe.easytier.compose.data.mergeInto
import cc.ptoe.easytier.compose.transport.root.RootTunTransport
import cc.ptoe.easytier.compose.transport.vpn.VpnPermissionRequester
import cc.ptoe.easytier.compose.transport.vpn.VpnTunTransport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class EasyTierRuntimeCoordinator private constructor(
    private val context: Context,
    permissionRequester: VpnPermissionRequester,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutex = Mutex()
    private val vpn = VpnTunTransport(context, permissionRequester)
    private val root = RootTunTransport(context)
    private val mutableStatus = MutableStateFlow(RuntimeStatus.Stopped)
    val status: StateFlow<RuntimeStatus> = mutableStatus.asStateFlow()
    private var activeProfile: EasyTierProfile? = null
    private var pollJob: Job? = null

    /**
     * Swaps the VPN permission requester. BootCompletedReceiver binds a no-op requester
     * (background context can't launch the consent dialog); when MainActivity comes up it
     * swaps in the real activity-backed requester so a foreground connect() can prompt.
     */
    fun updatePermissionRequester(requester: VpnPermissionRequester) {
        vpn.updatePermissionRequester(requester)
    }

    companion object {
        private const val TAG = "EasyTierCoordinator"

        // 出口节点模式下为 VpnService 补的默认路由（见 routesWithExitNode）。
        private const val DEFAULT_ROUTE_V4 = "0.0.0.0/0"

        // 传给 retainNetworkInstance 的哨兵名：一个必然不存在的实例名，
        // 用来避开 native 层 "null => 保留全部" 的语义（见 stopNativeInstances）。
        private const val STOP_SENTINEL = "__easytier_stopped__"

        private const val PREFS_NAME = "easytier_runtime"
        private const val KEY_USER_STOPPED = "user_stopped"

        // Process-wide singleton: BootCompletedReceiver and MainActivity must share one
        // coordinator. Two instances would each run their own poll loops and transports
        // against the same global native instance / root daemon, so a stop() from the
        // activity would be overridden by the boot receiver's still-polling loop, leaving
        // the VPN service running with no way to disconnect it.
        @Volatile private var instance: EasyTierRuntimeCoordinator? = null

        @Synchronized
        fun getInstance(context: Context, permissionRequester: VpnPermissionRequester): EasyTierRuntimeCoordinator {
            val current = instance
            if (current != null) {
                // Adopt the caller's requester so a boot-created singleton can prompt
                // once the activity is foregrounded.
                current.updatePermissionRequester(permissionRequester)
                return current
            }
            return EasyTierRuntimeCoordinator(context.applicationContext, permissionRequester).also { instance = it }
        }
    }

    suspend fun start(profile: EasyTierProfile, globalSettings: GlobalSettings = GlobalSettings()): RuntimeStatus = mutex.withLock {
        // Auto-fill hostname from the Android device name when the profile leaves it blank,
        // so peers see a recognizable identity without forcing the user to configure it.
        // Global overrides are merged on top afterwards; build/validate merge again
        // internally, which is idempotent.
        val effectiveProfile = globalSettings.mergeInto(profile.withDeviceHostnameIfBlank(context))
        val errors = ProfileValidator().validate(effectiveProfile, globalSettings)
        if (errors.isNotEmpty()) return@withLock failure(effectiveProfile, errors.values.first().resolve(context))
        val toml = TomlConfigBuilder.build(effectiveProfile, globalSettings)
        stopLocked()
        markUserStopped(false)
        activeProfile = effectiveProfile
        mutableStatus.value = RuntimeStatus(RuntimeState.STARTING, effectiveProfile.id, effectiveProfile.tunMode, null, null, null)
        return@withLock when (effectiveProfile.tunMode) {
            // No TUN mode: run the EasyTier core with no TUN device at all, in
            // the app process. VpnService and RootTun are both skipped.
            TunMode.NO_TUN -> startNoTun(effectiveProfile, toml)
            TunMode.VPN_SERVICE -> startVpn(effectiveProfile, toml, globalSettings)
            TunMode.ROOT_TUN -> startRoot(effectiveProfile, toml, globalSettings)
        }
    }

    /**
     * Launches [start] in the coordinator's own [scope] (SupervisorJob + Dispatchers.IO),
     * decoupled from the caller's coroutine context. The returned [Job] can be awaited
     * for completion but cancelling it does NOT abort the start operation — the
     * SupervisorJob keeps [start] running until it settles on its own.
     *
     * Used by [BootCompletedReceiver] so its 8s `withTimeoutOrNull` wait only abandons
     * *waiting* for the result, not the start operation itself. This ensures `bind()`
     * and `remote.start(...)` complete even when boot-time root process startup is slow.
     */
    fun startDetached(profile: EasyTierProfile, globalSettings: GlobalSettings = GlobalSettings()): Job =
        scope.launch { start(profile, globalSettings) }

    suspend fun stop(): RuntimeStatus = mutex.withLock { stopLocked() }

    /**
     * Attempts to reconnect to a root daemon left running by a previous app process
     * (orphan process). If the daemon reports a running EasyTier instance, matches
     * its [profileId] against [profileLookup] and, on a match, takes over: sets the
     * active profile, publishes the daemon's status, and resumes polling. If the
     * profile no longer exists, stops the orphan so it doesn't linger.
     *
     * Safe to call on every app launch: it is a no-op when no daemon is running
     * (state STOPPED) and when the active transport is VPN_SERVICE.
     */
    suspend fun attachOrphanRoot(profileLookup: suspend (String) -> EasyTierProfile?) = mutex.withLock {
        // Skip if a session is already active in this process.
        if (activeProfile != null) return@withLock
        val rootStatus = root.attach() ?: return@withLock
        val profileId = rootStatus.profileId
        val profile = if (profileId != null) profileLookup(profileId) else null
        if (profile == null) {
            // Profile was deleted while the app was gone — stop the orphan.
            root.stop()
            return@withLock
        }
        activeProfile = profile
        val status = root.adopt(rootStatus, profile)
        mutableStatus.value = status
        if (status.state == RuntimeState.RUNNING || status.state == RuntimeState.STARTING) pollRoot()
    }

    /**
     * Attempts to adopt an EasyTier instance already running in-process (started by
     * BootCompletedReceiver in VPN_SERVICE or no_tun mode). Detects a running instance
     * via collectNetworkInfos, matches its profileId via [profileLookup], and resumes
     * polling without re-establishing the TUN/VPN interface.
     *
     * Safe to call on every app launch: it is a no-op when no in-process instance is
     * running (collectNetworkInfos returns null/empty) and when the active transport
     * is ROOT_TUN (handled by [attachOrphanRoot] instead, since the EasyTier core in
     * ROOT_TUN mode lives in the root daemon process, not in the app process).
     */
    suspend fun attachRunningInstance(
        globalSettings: GlobalSettings,
        profileLookup: suspend (String) -> EasyTierProfile?,
    ) = mutex.withLock {
        if (activeProfile != null) return@withLock
        if (isUserStopped()) {
            // 用户已主动断开。此时若原生实例因为 TUN fd 引用未释放而残留，重新接管会
            // 立刻把带 0.0.0.0/0 的 VPN 接口再建起来 —— 用户看到的就是“断开后依然断网”。
            // 这里只做清理，绝不接管。
            Log.w(TAG, "attachRunningInstance: skipped, user explicitly disconnected; releasing leftovers")
            stopNativeInstances()
            return@withLock
        }
        val raw = runCatching { EasyTierJni.collectNetworkInfos(1) }.getOrNull() ?: return@withLock
        val map = runCatching {
            Json.parseToJsonElement(raw).jsonObject["map"]?.jsonObject
        }.getOrNull() ?: return@withLock
        if (map.isEmpty()) return@withLock
        // Find the first running instance without an error.
        val entry = map.entries.firstOrNull { (_, value) ->
            val obj = value.jsonObject
            obj["error_msg"]?.jsonPrimitive?.content.isNullOrBlank()
        } ?: return@withLock
        val profileId = entry.key
        val profile = profileLookup(profileId) ?: run {
            // Instance running but profile deleted — release it so it doesn't linger.
            stopNativeInstances()
            return@withLock
        }
        activeProfile = profile
        val info = raw.networkInfo(profileId)
        when (profile.tunMode) {
            // In-process no_tun adoption only applies to NO_TUN sessions.
            TunMode.NO_TUN -> {
                mutableStatus.value = RuntimeStatus(
                    state = RuntimeState.RUNNING,
                    profileId = profile.id,
                    tunMode = profile.tunMode,
                    virtualIpv4 = info?.virtualIpv4,
                    tunDevice = null,
                    error = null,
                    peers = info?.peers.orEmpty(),
                    hostname = info?.hostname,
                    natType = info?.natType,
                    exitNodeCandidates = info?.exitNodeCandidates.orEmpty(),
                )
                pollNoTun()
            }
            TunMode.VPN_SERVICE -> {
                val status = vpn.adopt(profile, info?.virtualIpv4, profile.routesWithExitNode(info?.routes.orEmpty()))
                mutableStatus.value = status
                if (status.state == RuntimeState.RUNNING || status.state == RuntimeState.STARTING) pollVpn()
            }
            else -> {
                // ROOT_TUN should not reach here (EasyTier runs in the root daemon
                // process, not in-process). Clear state so attachOrphanRoot can handle it.
                activeProfile = null
            }
        }
    }

    private suspend fun startVpn(profile: EasyTierProfile, toml: String, globalSettings: GlobalSettings): RuntimeStatus = try {
        stopNativeInstances()
        require(EasyTierJni.runNetworkInstance(toml) == 0) { nativeError(R.string.error_easytier_start_failed) }
        vpn.start(profile, toml, globalSettings).also {
            mutableStatus.value = it
            if (it.state == RuntimeState.STARTING || it.state == RuntimeState.RUNNING) pollVpn()
        }
    } catch (cancellation: CancellationException) {
        // External cancellation (e.g. BootCompletedReceiver's 8s budget): the EasyTier
        // core is already running in-process — do NOT release it or stop the VPN. The
        // caller intentionally bailed without waiting; a later app launch re-attaches
        // via attachRunningInstance().
        throw cancellation
    } catch (error: Throwable) {
        vpn.stop()
        stopNativeInstances()
        failure(profile, error.message ?: nativeError(R.string.error_easytier_start_failed))
    }

    private suspend fun startRoot(profile: EasyTierProfile, toml: String, globalSettings: GlobalSettings): RuntimeStatus {
        val status = root.start(profile, toml, globalSettings)
        mutableStatus.value = status
        if (status.state == RuntimeState.RUNNING || status.state == RuntimeState.STARTING) pollRoot()
        return status
    }

    /**
     * no_tun path: start the EasyTier core only, with no VpnService/RootTun attached.
     * The core still publishes virtual_ipv4 / peers / hostname / natType via
     * collectNetworkInfos, so we poll those for UI updates — but never call
     * establishWhenResolved() or root.setTunFd(), leaving the system without a TUN.
     */
    private suspend fun startNoTun(profile: EasyTierProfile, toml: String): RuntimeStatus = try {
        stopNativeInstances()
        require(EasyTierJni.runNetworkInstance(toml) == 0) { nativeError(R.string.error_easytier_start_failed) }
        mutableStatus.value = RuntimeStatus(RuntimeState.RUNNING, profile.id, profile.tunMode, null, null, null)
        pollNoTun()
        mutableStatus.value
    } catch (cancellation: CancellationException) {
        // External cancellation (e.g. BootCompletedReceiver's 8s budget): the EasyTier
        // core is already running in-process — do NOT release it. The caller
        // intentionally bailed without waiting; a later app launch re-attaches via
        // attachRunningInstance().
        throw cancellation
    } catch (error: Throwable) {
        stopNativeInstances()
        failure(profile, error.message ?: nativeError(R.string.error_easytier_start_failed))
    }

    private suspend fun stopLocked(): RuntimeStatus {
        // cancelAndJoin (instead of cancel) ensures the poll coroutine has fully
        // terminated before we call stopService(). Otherwise a late establishWhenResolved()
        // call could post a startForegroundService intent AFTER stopService(), causing
        // EasyTierVpnService to be re-activated and leaving the system VPN up.
        pollJob?.cancelAndJoin()
        pollJob = null
        val profile = activeProfile
        // 记下“用户主动断开”，阻止 attachRunningInstance 在实例残留时重新接管并
        // 重建带默认路由的接口（那是断开后再次断网的直接原因）。
        markUserStopped(true)
        if (profile != null) mutableStatus.value = mutableStatus.value.copy(state = RuntimeState.STOPPING)
        when (profile?.tunMode) {
            // no_tun sessions started in-process (NO_TUN mode) release the
            // EasyTier instance here; ROOT_TUN sessions always live in the root
            // daemon and are stopped via root.stop() below.
            TunMode.NO_TUN -> stopNativeInstances()
            TunMode.VPN_SERVICE -> {
                // 顺序至关重要：先把 VPN 接口/默认路由从系统上摘掉，再停 native 实例。
                // 反序（先停实例、后关接口）时，只要原生层对 TUN fd 的引用还没释放，
                // 接口就会继续挂着 0.0.0.0/0，用户一直断网 —— 这就是“断开连接后整机
                // 断网、只能杀进程”的直接原因。vpn.stop() 内部会强制注销 VPN 配置。
                vpn.stop()
                stopNativeInstances()
                awaitNativeStopped(profile)
            }
            TunMode.ROOT_TUN -> root.stop()
            null -> {
                // activeProfile 丢失（进程被回收、attach 失败等）但 VPN 可能仍在运行。
                // 无条件清一遍，避免孤儿 VPN 服务永久劫持流量、造成整机断网。
                Log.w(TAG, "stopLocked: no active profile, forcing VPN/native teardown")
                vpn.stop()
                stopNativeInstances()
            }
        }
        activeProfile = null
        return RuntimeStatus.Stopped.also { mutableStatus.value = it }
    }

    private fun prefs() = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    private fun markUserStopped(stopped: Boolean) {
        runCatching { prefs().edit().putBoolean(KEY_USER_STOPPED, stopped).apply() }
    }

    private fun isUserStopped(): Boolean =
        runCatching { prefs().getBoolean(KEY_USER_STOPPED, false) }.getOrDefault(false)

    private fun pollVpn() {
        pollJob?.cancel()
        val profile = activeProfile ?: return
        pollJob = scope.launch {
            val exitDetector = CoreExitDetector()
            while (mutableStatus.value.state in setOf(RuntimeState.STARTING, RuntimeState.RUNNING)) {
                // A null collect result (JNI threw) feeds the exit detector too:
                // silently skipping it previously left the VPN up forever when the
                // core was gone.
                val info = runCatching { EasyTierJni.collectNetworkInfos(1)?.networkInfo(profile.id) }
                    .onFailure { Log.w(TAG, "collectNetworkInfos failed: ${it.message}") }
                    .getOrNull()
                if (exitDetector.onPoll(info)) {
                    Log.w(TAG, "core for profile ${profile.id} is gone; tearing down VPN")
                    failure(profile, context.getString(R.string.error_core_stopped))
                    vpn.stop()
                    stopNativeInstances()
                    return@launch
                }
                if (info != null) {
                    if (!info.error.isNullOrBlank()) {
                        failure(profile, info.error)
                        vpn.stop()
                        stopNativeInstances()
                        return@launch
                    }
                    if (!info.virtualIpv4.isNullOrBlank()) {
                        val status = vpn.establishWhenResolved(profile, info.virtualIpv4, profile.routesWithExitNode(info.routes))
                        mutableStatus.value = status.copy(
                            peers = info.peers,
                            hostname = info.hostname,
                            natType = info.natType,
                            exitNodeCandidates = info.exitNodeCandidates,
                        )
                    } else {
                        // 虚拟 IP 尚未解析出来时也要刷新节点/出口候选，避免首屏选择器
                        // 长时间空白（peer 与路由信息与本机 IP 无关）。
                        mutableStatus.value = mutableStatus.value.copy(
                            peers = info.peers,
                            hostname = info.hostname,
                            natType = info.natType,
                            exitNodeCandidates = info.exitNodeCandidates,
                        )
                    }
                    refreshWireGuardPortal(profile)
                }
                // STARTING polls faster to resolve the virtual IPv4 quickly; steady-state
                // peers/latency change slowly, so 5s is enough and cuts JNI + JSON overhead.
                delay(if (mutableStatus.value.state == RuntimeState.RUNNING) 5_000 else 2_000)
            }
        }
    }

    private fun pollRoot() {
        pollJob?.cancel()
        pollJob = scope.launch { root.status.collect { mutableStatus.value = it } }
    }

    /**
     * Refreshes the WireGuard VPN portal info for profiles that configured one.
     * collectNetworkInfos does not include the portal client config, so it is
     * queried via VpnPortalRpc and attached to the current status. No-op when
     * the portal is not configured or not started yet (fetch returns null).
     */
    private fun refreshWireGuardPortal(profile: EasyTierProfile) {
        if (profile.vpnPortal == null) return
        if (mutableStatus.value.state != RuntimeState.RUNNING) return
        WireGuardPortalClient.fetch(profile.id)?.let { portal ->
            mutableStatus.value = mutableStatus.value.copy(wireguardPortal = portal)
        }
    }

    private fun pollNoTun() {
        pollJob?.cancel()
        val profile = activeProfile ?: return
        pollJob = scope.launch {
            val exitDetector = CoreExitDetector()
            while (mutableStatus.value.state in setOf(RuntimeState.STARTING, RuntimeState.RUNNING)) {
                val info = runCatching { EasyTierJni.collectNetworkInfos(1)?.networkInfo(profile.id) }
                    .onFailure { Log.w(TAG, "collectNetworkInfos failed: ${it.message}") }
                    .getOrNull()
                if (exitDetector.onPoll(info)) {
                    Log.w(TAG, "core for profile ${profile.id} is gone; clearing no-tun session")
                    failure(profile, context.getString(R.string.error_core_stopped))
                    stopNativeInstances()
                    return@launch
                }
                // No TUN to establish: just surface runtime metadata for the UI.
                // EasyTier still assigns a virtual IPv4 even without a TUN device,
                // so propagate it (the poll only sets it when resolved, preserving
                // the previous value when collectNetworkInfos returns null).
                info?.let {
                    it.virtualIpv4?.let { ipv4 ->
                        mutableStatus.value = mutableStatus.value.copy(virtualIpv4 = ipv4)
                    }
                    mutableStatus.value = mutableStatus.value.copy(
                        peers = it.peers,
                        hostname = it.hostname,
                        natType = it.natType,
                        exitNodeCandidates = it.exitNodeCandidates,
                    )
                    refreshWireGuardPortal(profile)
                }
                delay(5_000)
            }
        }
    }

    private fun failure(profile: EasyTierProfile, error: String): RuntimeStatus =
        RuntimeStatus(RuntimeState.ERROR, profile.id, profile.tunMode, null, null, error).also { mutableStatus.value = it }

    /**
     * EasyTier 的 exit_nodes 只是 core 数据面的“兜底下一跳”：只有当报文已经进入 TUN、
     * 且目的地址不在虚拟网段/已知路由中时，才会被交给出口节点。它本身不会产生任何路由
     * 条目，所以不会出现在 [NetworkInfo.routes]（那里只有对端通告的 proxy_cidrs 子网代理）。
     *
     * 免 root 的 VpnService 路由完全由 App 通过 Builder.addRoute 决定，因此配置了出口节点
     * 却不补默认路由时，上网流量根本不会进隧道，出口节点永远收不到包（表现为查 IP 仍是本机 IP）。
     * 这里在 exit_nodes 非空且对端未通告 0.0.0.0/0 时补一条默认路由，行为对齐 Tailscale 的
     * exit node：选中出口节点即接管全部 IPv4 流量。
     */
    private fun EasyTierProfile.routesWithExitNode(routes: List<String>): List<String> {
        if (exitNodes.none { it.isNotBlank() }) return routes
        if (routes.any { it.trim() == DEFAULT_ROUTE_V4 }) return routes
        return routes + DEFAULT_ROUTE_V4
    }


    /**
     * 真正停止 in-process 的 EasyTier 实例。
     *
     * 关键坑：JNI 的 retainNetworkInstance(null) 在 native 层会走 "retain_all" 分支
     * （空数组/ null => 全部保留），并不会停止任何实例。实例会继续持有 VpnService
     * TUN fd 的副本，断开连接后 VPN 接口依旧挂着 0.0.0.0/0 默认路由，所有流量被
     * 黑洞 —— 表现为"断开后整机断网，只有杀掉 App 进程才恢复"。
     *
     * 这里传一个不存在的实例名，强制走"保留列表、停止其余"分支，把实例真正停掉。
     */
    private fun stopNativeInstances() {
        runCatching { EasyTierJni.retainNetworkInstance(arrayOf(STOP_SENTINEL)) }
            .onFailure { Log.w(TAG, "stopNativeInstances failed: ${it.message}") }
    }

    /**
     * 校验实例确实已停止。若重试后仍在运行（TUN fd 仍被持有 -> 会持续黑洞流量），
     * 结束本进程兜底释放所有 fd，避免手机持续断网（这也是用户唯一可行的自救手段）。
     */
    private suspend fun awaitNativeStopped(profile: EasyTierProfile) {
        repeat(4) { attempt ->
            val alive = runCatching { EasyTierJni.collectNetworkInfos(1)?.networkInfo(profile.id) }
                .getOrNull() != null
            if (!alive) {
                Log.i(TAG, "native instance ${profile.id} stopped (attempt ${attempt + 1})")
                return
            }
            Log.w(TAG, "native instance ${profile.id} still alive (attempt ${attempt + 1}), retrying retain")
            stopNativeInstances()
            delay(250)
        }
        Log.w(TAG, "native instance ${profile.id} survived stop; VPN config already revoked so traffic is not blackholed")
    }

    private fun nativeError(@StringRes fallbackRes: Int) = EasyTierJni.getLastError() ?: context.getString(fallbackRes)
}
