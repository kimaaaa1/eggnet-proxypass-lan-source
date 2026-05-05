package org.cloudburstmc.proxypass.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.kastle.netty.channel.nethernet.NetherNetChannelFactory;
import dev.kastle.netty.channel.nethernet.NetherNetClientChannel;
import dev.kastle.netty.channel.nethernet.NetherNetServerChannel;
import dev.kastle.netty.channel.nethernet.config.NetherChannelOption;
import dev.kastle.netty.channel.nethernet.config.NetherNetAddress;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetClientSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetDiscoverySignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetXboxRpcSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetXboxSignaling;
import dev.kastle.webrtc.PeerConnectionFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import org.cloudburstmc.proxypass.EggnetCommunitySupport;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class NetherNetConnectorCli {
    private static final String COMMAND_ACK_PREFIX = "__eggnet_cmd_ack__:";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(EggnetCommunitySupport.CONNECT_TIMEOUT)
            .build();
    private static final long DEFAULT_REFRESH_MS = 1_000L;
    private static final long DEFAULT_AUTH_REFRESH_MS = 10 * 60 * 1_000L;
    private static final long AUTH_REFRESH_LEAD_MS = 120_000L;
    private static final long AUTH_MIN_ACTIVE_MS = 5_000L;
    private static final int LISTEN_PORT = 7551;
    private static final long ROUTE_MISS_GRACE_MS = 30_000L;
    private static final long DYNAMIC_LOCAL_ID_START = 4_000_000_000_000_000_000L;
    private NetherNetConnectorCli() {
    }

    public static void main(String[] args) throws Exception {
        Args parsed = Args.parse(args);
        ConnectorRuntime runtime = new ConnectorRuntime(parsed);
        runtime.start();
        runtime.await();
    }

    private static final class ConnectorRuntime {
        private final Args args;
        private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        private final PeerConnectionFactory peerFactory = new PeerConnectionFactory();
        private final ScheduledExecutorService refreshExecutor;
        private final CountDownLatch exitLatch = new CountDownLatch(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicLong dynamicLocalIdCursor = new AtomicLong(DYNAMIC_LOCAL_ID_START);
        private final Object serverLifecycleLock = new Object();
        private final Map<String, Long> dynamicLocalIdByNetherId = new HashMap<>();
        private final Map<String, Long> routeMissingSinceByNetherId = new HashMap<>();
        private final Map<String, TargetRoute> routesByLocalNetworkId = new HashMap<>();
        private final Map<String, PrimedRouteState> primedStatesByLocalNetworkId = new HashMap<>();
        private volatile Channel server;
        private volatile NetherNetDiscoverySignaling discoverySignaling;
        private volatile String currentDesktopXuid = "";
        private volatile String currentSignalAuth = "";
        private volatile long currentSignalAuthAtMs = 0L;
        private volatile long currentSignalAuthExpiresAtMs = 0L;
        private volatile String lastRouteSignature = "";
        private volatile String lastLoginState = "";
        private EggnetCommunitySupport.LiveServerSnapshot liveServerSnapshot = EggnetCommunitySupport.LiveServerSnapshot.empty();

        private ConnectorRuntime(Args args) {
            this.args = args;
            ThreadFactory threadFactory = r -> {
                Thread t = new Thread(r, "nethernet-connector-refresh");
                t.setDaemon(true);
                return t;
            };
            this.refreshExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        }

        void start() {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "nethernet-connector-shutdown"));
            refreshExecutor.scheduleWithFixedDelay(this::refreshSafely, 0L, Math.max(500L, args.refreshMs), TimeUnit.MILLISECONDS);
            if (args.desktopManaged) {
                startCommandLoop();
            }
        }

        void await() throws InterruptedException {
            exitLatch.await();
        }

        private void startCommandLoop() {
            Thread thread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                    while (!closed.get()) {
                        String line = reader.readLine();
                        if (line == null) {
                            break;
                        }
                        handleCommand(line.trim());
                    }
                } catch (Exception e) {
                    warn("command loop failed: %s", e.getMessage());
                }
            }, "nethernet-connector-command");
            thread.setDaemon(true);
            thread.start();
        }

        private void handleCommand(String command) {
            if (command == null || command.isBlank()) {
                return;
            }
            if ("refresh".equalsIgnoreCase(command)) {
                triggerRefreshAsync();
                return;
            }
            if (command.regionMatches(true, 0, "prime ", 0, 6)) {
                String encoded = command.substring(6).trim();
                if (encoded.isEmpty()) {
                    return;
                }
                try {
                    String json = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payload = JSON.readValue(json, Map.class);
                    boolean primed = primeRandomActorJoin(payload);
                    emitCommandAck("prime", primed ? "ok" : "fail");
                } catch (Exception e) {
                    warn("random actor join command failed: %s", e.getMessage());
                    emitCommandAck("prime", "fail");
                }
            }
        }

        void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            clearRoutes();
            closeServer();
            refreshExecutor.shutdownNow();
            try {
                peerFactory.dispose();
            } catch (Exception ignored) {
            }
            eventLoopGroup.shutdownGracefully();
            exitLatch.countDown();
        }

        TargetRoute routeForLocalNetworkId(String localNetworkId) {
            synchronized (routesByLocalNetworkId) {
                String key = localNetworkId == null ? "" : localNetworkId.trim();
                return routesByLocalNetworkId.get(key);
            }
        }

        String signalAuthForLocalNetworkId(String localNetworkId) {
            String key = localNetworkId == null ? "" : localNetworkId.trim();
            PrimedRouteState primedState;
            TargetRoute route;
            synchronized (routesByLocalNetworkId) {
                primedState = primedStatesByLocalNetworkId.get(key);
                route = routesByLocalNetworkId.get(key);
            }
            if (primedState == null) {
                return ensureSignalAuthorization();
            }
            long now = System.currentTimeMillis();
            if (route != null && isPrimedStateUsable(primedState, now, AUTH_MIN_ACTIVE_MS)) {
                return primedState.authorization();
            }
            synchronized (routesByLocalNetworkId) {
                TargetRoute removedRoute = routesByLocalNetworkId.remove(key);
                info("Removed primed advertisement localNetworkId=%s reason=auth_unusable_on_click nether=%s target=%s actor=%s expiresAtMs=%s nowMs=%s leadMs=%s",
                        key,
                        removedRoute == null ? "" : removedRoute.nethernetId(),
                        removedRoute == null ? "" : removedRoute.remoteTargetId(),
                        primedState.actorXuid(),
                        primedState.authExpiresAtMs(),
                        now,
                        AUTH_MIN_ACTIVE_MS);
                primedStatesByLocalNetworkId.remove(key);
                applyAdvertisementsLocked();
                logRouteSummaryLocked();
            }
            return "";
        }

        void invalidateSignalAuth() {
            currentSignalAuth = "";
            currentSignalAuthAtMs = 0L;
            currentSignalAuthExpiresAtMs = 0L;
        }

        boolean dropPrimedRoute(TargetRoute route) {
            if (route == null || !EggnetCommunitySupport.hasText(route.nethernetId())) {
                return false;
            }
            synchronized (routesByLocalNetworkId) {
                Set<String> keysToRemove = new LinkedHashSet<>();
                for (Map.Entry<String, PrimedRouteState> entry : primedStatesByLocalNetworkId.entrySet()) {
                    TargetRoute currentRoute = routesByLocalNetworkId.get(entry.getKey());
                    if (currentRoute != null && route.nethernetId().equals(currentRoute.nethernetId())) {
                        keysToRemove.add(entry.getKey());
                    }
                }
                boolean removed = !keysToRemove.isEmpty();
                for (String key : keysToRemove) {
                    primedStatesByLocalNetworkId.remove(key);
                    routesByLocalNetworkId.remove(key);
                }
                if (removed) {
                    applyAdvertisementsLocked();
                    logRouteSummaryLocked();
                }
                return removed;
            }
        }

        void triggerRefreshAsync() {
            if (closed.get()) {
                return;
            }
            refreshExecutor.execute(this::refreshSafely);
        }

        private boolean primeRandomActorJoin(Map<String, Object> payload) {
            if (payload == null) {
                return false;
            }
            String actorXuid = stringValue(payload.get("actorXuid"));
            String authorization = stringValue(payload.get("authorization"));
            Map<?, ?> routeMap = payload.get("route") instanceof Map<?, ?> route ? route : null;
            if (!EggnetCommunitySupport.hasText(actorXuid)
                    || !EggnetCommunitySupport.hasText(authorization)
                    || routeMap == null) {
                return false;
            }
            TargetRoute route = TargetRoute.fromMap(routeMap);
            long authExpiresAtMs = longValue(payload.get("authExpiresAtMs"), 0L);
            try {
                ensureServerRunning();
                upsertPrimedRoute(route, new PrimedRouteState(
                        actorXuid,
                        authorization,
                        Math.max(0L, authExpiresAtMs)
                ));
                return true;
            } catch (Exception e) {
                warn("random actor join prime failed: %s", e.getMessage());
                return false;
            }
        }

        private void upsertPrimedRoute(TargetRoute route, PrimedRouteState primedState) {
            if (primedState == null || route == null || !EggnetCommunitySupport.hasText(route.nethernetId())) {
                return;
            }
            synchronized (routesByLocalNetworkId) {
                long localNetworkId = resolveDynamicLocalNetworkId(route.nethernetId());
                String key = Long.toUnsignedString(localNetworkId);
                primedStatesByLocalNetworkId.put(key, primedState);
                routesByLocalNetworkId.put(key, route);
                routeMissingSinceByNetherId.remove(route.nethernetId());
                applyAdvertisementsLocked();
                logRouteSummaryLocked();
            }
        }

        private void emitCommandAck(String command, String status) {
            System.err.println(COMMAND_ACK_PREFIX + command + ":" + status);
            System.err.flush();
        }

        void dropRoute(TargetRoute route) {
            if (route == null || !EggnetCommunitySupport.hasText(route.nethernetId())) {
                return;
            }
            synchronized (routesByLocalNetworkId) {
                routeMissingSinceByNetherId.remove(route.nethernetId());
                routesByLocalNetworkId.entrySet().removeIf(entry -> route.nethernetId().equals(entry.getValue().nethernetId()));
                primedStatesByLocalNetworkId.entrySet().removeIf(entry -> !routesByLocalNetworkId.containsKey(entry.getKey()));
                applyAdvertisementsLocked();
                logRouteSummaryLocked();
            }
        }

        private void refreshSafely() {
            if (closed.get()) {
                return;
            }
            try {
                refreshOnce();
            } catch (Throwable t) {
                warn("refresh failed: %s", t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
            }
        }

        private void refreshOnce() throws Exception {
            long now = System.currentTimeMillis();
            int activePrimedCount = pruneExpiredPrimedStates(now);
            String selfXuid = resolveSelfXuid();
            if (EggnetCommunitySupport.hasText(selfXuid)) {
                activePrimedCount = clearPrimedStatesForLoggedInUser();
            }
            if (!EggnetCommunitySupport.hasText(selfXuid) && activePrimedCount == 0) {
                if (!"logged_out".equals(lastLoginState)) {
                    info("No desktop XUID found. Connector idle.");
                    lastLoginState = "logged_out";
                }
                currentDesktopXuid = "";
                invalidateSignalAuth();
                clearRoutes();
                closeServer();
                return;
            }

            Map<String, EggnetCommunitySupport.LiveServerInfo> liveByNether = Map.of();
            if (EggnetCommunitySupport.hasText(selfXuid) || activePrimedCount > 0) {
                EggnetCommunitySupport.LiveServerSnapshot snapshot = EggnetCommunitySupport.fetchLiveServerSnapshot(
                        JSON,
                        HTTP,
                        EggnetCommunitySupport.LIST3_URL_DEFAULT,
                        liveServerSnapshot
                );
                liveServerSnapshot = snapshot;
                liveByNether = snapshot.serversByNetherId();
            }
            if (activePrimedCount > 0) {
                activePrimedCount = syncPrimedRoutesWithLive(liveByNether);
            }
            if (!EggnetCommunitySupport.hasText(selfXuid) && activePrimedCount == 0) {
                if (!"logged_out".equals(lastLoginState)) {
                    info("No desktop XUID found. Connector idle.");
                    lastLoginState = "logged_out";
                }
                currentDesktopXuid = "";
                invalidateSignalAuth();
                clearRoutes();
                closeServer();
                return;
            }

            String actorXuid = EggnetCommunitySupport.hasText(selfXuid)
                    ? selfXuid
                    : "random_actor";
            currentDesktopXuid = actorXuid;
            if (!actorXuid.equals(lastLoginState)) {
                info("Desktop actor xuid=%s", actorXuid);
                lastLoginState = actorXuid;
            }

            ensureServerRunning();

            List<TargetRoute> routes = new ArrayList<>();
            if (EggnetCommunitySupport.hasText(selfXuid)) {
                Set<String> following = EggnetCommunitySupport.fetchCommunityXuids(
                        JSON,
                        HTTP,
                        EggnetCommunitySupport.COMMUNITY_FOLLOWING_URL_DEFAULT,
                        selfXuid
                );

                for (EggnetCommunitySupport.LiveServerInfo live : liveByNether.values()) {
                    if (!EggnetCommunitySupport.hasText(live.ownerXuid()) || !following.contains(live.ownerXuid())) {
                        continue;
                    }
                    routes.add(TargetRoute.fromLive(live));
                }
            }
            sortRoutesForLanTab(routes, resolvePreferredLanguage(), captureStableRouteOrder());

            applyRoutes(routes);
        }

        private String resolvePreferredLanguage() {
            return EggnetCommunitySupport.normalizeLanguageCode(Locale.getDefault().toLanguageTag());
        }

        private Map<String, Integer> captureStableRouteOrder() {
            synchronized (routesByLocalNetworkId) {
                Map<String, Integer> stableOrder = new HashMap<>();
                int index = 0;
                for (TargetRoute route : routesByLocalNetworkId.values()) {
                    if (!EggnetCommunitySupport.hasText(route.nethernetId())) {
                        continue;
                    }
                    stableOrder.putIfAbsent(route.nethernetId(), index++);
                }
                return stableOrder;
            }
        }

        private void sortRoutesForLanTab(
                List<TargetRoute> routes,
                String preferredLanguage,
                Map<String, Integer> stableOrder
        ) {
            final String normalizedPreferredLanguage = EggnetCommunitySupport.normalizeLanguageCode(preferredLanguage);
            final int fallbackStableRank = 1 << 30;
            record SortMeta(
                    TargetRoute route,
                    int regionRank,
                    int memberCount,
                    boolean hasStableRank,
                    int stableRank,
                    String normalizedTitle,
                    int originalIndex
            ) { }

            List<SortMeta> metas = new ArrayList<>(routes.size());
            for (int i = 0; i < routes.size(); i++) {
                TargetRoute route = routes.get(i);
                Integer stableRank = stableOrder.get(route.nethernetId());
                int regionRank = 1;
                if (EggnetCommunitySupport.hasText(normalizedPreferredLanguage)
                        && EggnetCommunitySupport.hasText(route.primaryLanguage())
                        && normalizedPreferredLanguage.equals(route.primaryLanguage())) {
                    regionRank = 0;
                }
                metas.add(new SortMeta(
                        route,
                        regionRank,
                        route.memberCount(),
                        stableRank != null,
                        stableRank == null ? fallbackStableRank : stableRank,
                        route.displayTitle().trim().toLowerCase(Locale.ROOT),
                        i
                ));
            }

            metas.sort((left, right) -> {
                if (left.regionRank() != right.regionRank()) {
                    return Integer.compare(left.regionRank(), right.regionRank());
                }
                if (left.memberCount() != right.memberCount()) {
                    return Integer.compare(right.memberCount(), left.memberCount());
                }
                if (left.hasStableRank() && right.hasStableRank() && left.stableRank() != right.stableRank()) {
                    return Integer.compare(left.stableRank(), right.stableRank());
                }
                int titleCmp = left.normalizedTitle().compareTo(right.normalizedTitle());
                if (titleCmp != 0) {
                    return titleCmp;
                }
                if (left.hasStableRank() != right.hasStableRank()) {
                    return left.hasStableRank() ? -1 : 1;
                }
                return Integer.compare(left.originalIndex(), right.originalIndex());
            });

            routes.clear();
            for (SortMeta meta : metas) {
                routes.add(meta.route());
            }
        }

        private String resolveSelfXuid() {
            String xuid = EggnetCommunitySupport.resolveDesktopXuid(JSON);
            if (EggnetCommunitySupport.hasText(args.xuidOverride)) {
                return EggnetCommunitySupport.normalizeXuid(args.xuidOverride);
            }
            return xuid;
        }

        private void ensureServerRunning() throws Exception {
            synchronized (serverLifecycleLock) {
                Channel existing = this.server;
                if (existing != null && existing.isActive() && discoverySignaling != null) {
                    return;
                }

                closeServerLocked();
                killPortHoldersIfNeeded(LISTEN_PORT);

                NetherNetDiscoverySignaling nextDiscovery = new NetherNetDiscoverySignaling();
                ChannelFuture bindFuture;
                try {
                    bindFuture = createServerBootstrap(nextDiscovery)
                            .bind(new InetSocketAddress("0.0.0.0", LISTEN_PORT))
                            .sync();
                } catch (Exception firstBindError) {
                    warn("Initial bind to %s failed: %s", LISTEN_PORT, firstBindError.getMessage());
                    killPortHoldersIfNeeded(LISTEN_PORT);
                    try {
                        nextDiscovery.close();
                    } catch (Exception ignored) {
                    }
                    nextDiscovery = new NetherNetDiscoverySignaling();
                    bindFuture = createServerBootstrap(nextDiscovery)
                            .bind(new InetSocketAddress("0.0.0.0", LISTEN_PORT))
                            .sync();
                }

                this.discoverySignaling = nextDiscovery;
                this.server = bindFuture.channel();
                InetSocketAddress bound = (InetSocketAddress) bindFuture.channel().localAddress();
                info("Connector listening on %s", bound);
            }
        }

        private ServerBootstrap createServerBootstrap(NetherNetDiscoverySignaling discovery) {
            return new ServerBootstrap()
                    .group(eventLoopGroup)
                    .channelFactory(NetherNetChannelFactory.server(peerFactory, discovery))
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast("connector-local", new LocalInboundHandler(ConnectorRuntime.this));
                        }
                    });
        }

        private void closeServer() {
            synchronized (serverLifecycleLock) {
                closeServerLocked();
            }
        }

        private void closeServerLocked() {
            Channel existing = this.server;
            this.server = null;
            NetherNetDiscoverySignaling existingDiscovery = this.discoverySignaling;
            this.discoverySignaling = null;
            if (existing != null) {
                try {
                    existing.close().syncUninterruptibly();
                } catch (Exception ignored) {
                }
            }
            if (existingDiscovery != null) {
                try {
                    existingDiscovery.close();
                } catch (Exception ignored) {
                }
            }
        }

        private void clearRoutes() {
            synchronized (routesByLocalNetworkId) {
                if (routesByLocalNetworkId.isEmpty() && primedStatesByLocalNetworkId.isEmpty() && lastRouteSignature.isEmpty()) {
                    return;
                }
                routesByLocalNetworkId.clear();
                primedStatesByLocalNetworkId.clear();
                routeMissingSinceByNetherId.clear();
                applyAdvertisementsLocked();
                lastRouteSignature = "";
            }
        }

        private void applyRoutes(List<TargetRoute> routes) {
            List<TargetRoute> stableRoutes = stabilizeRoutes(routes);
            synchronized (routesByLocalNetworkId) {
                routesByLocalNetworkId.entrySet().removeIf(entry -> !primedStatesByLocalNetworkId.containsKey(entry.getKey()));
                for (TargetRoute route : stableRoutes) {
                    long localNetworkId = resolveDynamicLocalNetworkId(route.nethernetId());
                    routesByLocalNetworkId.put(Long.toUnsignedString(localNetworkId), route);
                }
                applyAdvertisementsLocked();
                logRouteSummaryLocked();
            }
        }

        private List<TargetRoute> stabilizeRoutes(List<TargetRoute> freshRoutes) {
            Map<String, TargetRoute> mergedByNetherId = new LinkedHashMap<>();
            for (TargetRoute route : freshRoutes) {
                mergedByNetherId.put(route.nethernetId(), route);
            }

            synchronized (routesByLocalNetworkId) {
                for (Map.Entry<String, TargetRoute> entry : routesByLocalNetworkId.entrySet()) {
                    if (primedStatesByLocalNetworkId.containsKey(entry.getKey())) {
                        continue;
                    }
                    TargetRoute currentRoute = entry.getValue();
                    if (mergedByNetherId.containsKey(currentRoute.nethernetId())) {
                        routeMissingSinceByNetherId.remove(currentRoute.nethernetId());
                        continue;
                    }

                    long missingSince = routeMissingSinceByNetherId.getOrDefault(currentRoute.nethernetId(), System.currentTimeMillis());
                    if (System.currentTimeMillis() - missingSince < ROUTE_MISS_GRACE_MS) {
                        routeMissingSinceByNetherId.put(currentRoute.nethernetId(), missingSince);
                        mergedByNetherId.put(currentRoute.nethernetId(), currentRoute);
                        continue;
                    }

                    routeMissingSinceByNetherId.remove(currentRoute.nethernetId());
                }

                for (TargetRoute route : freshRoutes) {
                    routeMissingSinceByNetherId.remove(route.nethernetId());
                }
            }

            List<TargetRoute> stableRoutes = new ArrayList<>(mergedByNetherId.values());
            sortRoutesForLanTab(stableRoutes, resolvePreferredLanguage(), captureStableRouteOrder());
            return stableRoutes;
        }

        private void applyAdvertisementsLocked() {
            NetherNetDiscoverySignaling signaling = this.discoverySignaling;
            if (signaling == null) {
                return;
            }
            Map<Long, NetherNetServerSignaling.PongData> nextAdvertisements = new LinkedHashMap<>();
            Set<String> advertisedNetherIds = new LinkedHashSet<>();
            for (Map.Entry<String, TargetRoute> entry : routesByLocalNetworkId.entrySet()) {
                TargetRoute route = entry.getValue();
                if (!advertisedNetherIds.add(route.nethernetId())) {
                    continue;
                }
                long localNetworkId = Long.parseUnsignedLong(entry.getKey());
                nextAdvertisements.put(localNetworkId, buildAdvertisement(route));
            }
            signaling.syncAdvertisementData(nextAdvertisements);
        }

        private void logRouteSummaryLocked() {
            Map<String, TargetRoute> advertisedByNetherId = new LinkedHashMap<>();
            for (TargetRoute route : routesByLocalNetworkId.values()) {
                advertisedByNetherId.put(route.nethernetId(), route);
            }
            List<TargetRoute> advertisedRoutes = new ArrayList<>(advertisedByNetherId.values());
            String signature = buildRouteSignature(advertisedRoutes);
            if (signature.equals(lastRouteSignature)) {
                return;
            }
            lastRouteSignature = signature;
            if (advertisedRoutes.isEmpty()) {
                info("No followed live worlds currently advertised.");
                return;
            }
            info("Advertising %s followed live worlds.", advertisedRoutes.size());
            int shown = 0;
            for (TargetRoute route : advertisedRoutes) {
                info("advertised world=%s host=%s members=%s/%s target=%s",
                        route.displayTitle(),
                        route.hostName(),
                        route.memberCount(),
                        route.maxMemberCount(),
                        route.remoteTargetId());
                shown++;
                if (shown >= 8) {
                    break;
                }
            }
        }

        private String buildRouteSignature(List<TargetRoute> routes) {
            StringBuilder sb = new StringBuilder(currentDesktopXuid).append('|').append(routes.size());
            for (TargetRoute route : routes) {
                sb.append('|')
                        .append(route.remoteTargetId()).append('/')
                        .append(route.displayTitle()).append('/')
                        .append(route.hostName()).append('/')
                        .append(route.memberCount()).append('/')
                        .append(route.maxMemberCount()).append('/')
                        .append(route.gameType());
            }
            return sb.toString();
        }

        private long resolveDynamicLocalNetworkId(String nethernetId) {
            Long existing = dynamicLocalIdByNetherId.get(nethernetId);
            if (existing != null) {
                return existing;
            }
            synchronized (dynamicLocalIdByNetherId) {
                existing = dynamicLocalIdByNetherId.get(nethernetId);
                if (existing != null) {
                    return existing;
                }
                long allocated = dynamicLocalIdCursor.getAndIncrement();
                if (allocated == 0L) {
                    allocated = dynamicLocalIdCursor.getAndIncrement();
                }
                dynamicLocalIdByNetherId.put(nethernetId, allocated);
                return allocated;
            }
        }

        private NetherNetServerSignaling.PongData buildAdvertisement(TargetRoute route) {
            String serverName = truncate(EggnetCommunitySupport.firstNonBlank(
                    route.hostName(),
                    route.ownerGamertag(),
                    route.displayTitle(),
                    "Server"
            ), 48);
            String levelName = truncate(EggnetCommunitySupport.firstNonBlank(
                    route.worldName(),
                    route.displayTitle(),
                    "World"
            ), 48);
            return new NetherNetServerSignaling.PongData.Builder()
                    .setServerName(serverName)
                    .setLevelName(levelName)
                    .setGameType(Math.max(0, route.gameType()))
                    .setPlayerCount(Math.max(0, route.memberCount()))
                    .setMaxPlayerCount(Math.max(1, Math.max(route.memberCount(), route.maxMemberCount())))
                    .setTransportLayer(EggnetCommunitySupport.ADVERTISEMENT_TRANSPORT_LAYER)
                    .setConnectionType(EggnetCommunitySupport.ADVERTISEMENT_CONNECTION_TYPE)
                    .build();
        }

        private String ensureSignalAuthorization() {
            long now = System.currentTimeMillis();
            if (isCurrentSignalAuthUsable(now, AUTH_MIN_ACTIVE_MS)) {
                return currentSignalAuth;
            }

            SignalAuthorization signalAuth = fetchRandomActorAuthorization();
            if (signalAuth == null || !EggnetCommunitySupport.hasText(signalAuth.authorization())) {
                if (isCurrentSignalAuthUsable(now, AUTH_MIN_ACTIVE_MS)) {
                    return currentSignalAuth;
                }
                invalidateSignalAuth();
                return "";
            }

            currentSignalAuth = signalAuth.authorization();
            currentSignalAuthAtMs = now;
            currentSignalAuthExpiresAtMs = Math.max(0L, signalAuth.expiresAtMs());
            return currentSignalAuth;
        }

        private int syncPrimedRoutesWithLive(Map<String, EggnetCommunitySupport.LiveServerInfo> liveByNether) {
            synchronized (routesByLocalNetworkId) {
                boolean changed = false;
                var iterator = primedStatesByLocalNetworkId.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, PrimedRouteState> entry = iterator.next();
                    PrimedRouteState state = entry.getValue();
                    TargetRoute currentRoute = routesByLocalNetworkId.get(entry.getKey());
                    if (currentRoute == null) {
                        iterator.remove();
                        changed = true;
                        continue;
                    }
                    EggnetCommunitySupport.LiveServerInfo live = liveByNether.get(currentRoute.nethernetId());
                    if (live == null) {
                        info("Removed primed advertisement localNetworkId=%s reason=not_in_list3_on_refresh nether=%s target=%s actor=%s",
                                entry.getKey(),
                                currentRoute.nethernetId(),
                                currentRoute.remoteTargetId(),
                                state.actorXuid());
                        routeMissingSinceByNetherId.remove(currentRoute.nethernetId());
                        routesByLocalNetworkId.remove(entry.getKey());
                        iterator.remove();
                        changed = true;
                        continue;
                    }

                    TargetRoute refreshedRoute = TargetRoute.fromLive(live);
                    if (!sameRouteSnapshot(currentRoute, refreshedRoute)) {
                        info("Updated primed advertisement from list3 localNetworkId=%s nether=%s target=%s title=%s->%s members=%s/%s->%s/%s",
                                entry.getKey(),
                                currentRoute.nethernetId(),
                                currentRoute.remoteTargetId(),
                                currentRoute.displayTitle(),
                                refreshedRoute.displayTitle(),
                                currentRoute.memberCount(),
                                currentRoute.maxMemberCount(),
                                refreshedRoute.memberCount(),
                                refreshedRoute.maxMemberCount());
                        routesByLocalNetworkId.put(entry.getKey(), refreshedRoute);
                        changed = true;
                    }
                }
                if (changed) {
                    applyAdvertisementsLocked();
                    logRouteSummaryLocked();
                }
                return primedStatesByLocalNetworkId.size();
            }
        }

        private boolean sameRouteSnapshot(TargetRoute left, TargetRoute right) {
            if (left == right) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            return Objects.equals(left.nethernetId(), right.nethernetId())
                    && Objects.equals(left.pmsgId(), right.pmsgId())
                    && Objects.equals(left.ownerXuid(), right.ownerXuid())
                    && Objects.equals(left.ownerGamertag(), right.ownerGamertag())
                    && Objects.equals(left.worldName(), right.worldName())
                    && Objects.equals(left.hostName(), right.hostName())
                    && Objects.equals(left.displayTitle(), right.displayTitle())
                    && left.gameType() == right.gameType()
                    && left.memberCount() == right.memberCount()
                    && left.maxMemberCount() == right.maxMemberCount()
                    && left.connectionType() == right.connectionType()
                    && left.transportLayer() == right.transportLayer();
        }

        private int pruneExpiredPrimedStates(long now) {
            synchronized (routesByLocalNetworkId) {
                boolean changed = false;
                var iterator = primedStatesByLocalNetworkId.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, PrimedRouteState> entry = iterator.next();
                    PrimedRouteState state = entry.getValue();
                    boolean usable = isPrimedStateUsable(state, now, AUTH_MIN_ACTIVE_MS);
                    if (usable) {
                        continue;
                    }
                    TargetRoute route = routesByLocalNetworkId.remove(entry.getKey());
                    info("Removed primed advertisement localNetworkId=%s reason=auth_unusable_on_refresh nether=%s target=%s actor=%s expiresAtMs=%s nowMs=%s leadMs=%s",
                            entry.getKey(),
                            route == null ? "" : route.nethernetId(),
                            route == null ? "" : route.remoteTargetId(),
                            state.actorXuid(),
                            state.authExpiresAtMs(),
                            now,
                            AUTH_MIN_ACTIVE_MS);
                    iterator.remove();
                    changed = true;
                }
                if (changed) {
                    applyAdvertisementsLocked();
                    logRouteSummaryLocked();
                }
                return primedStatesByLocalNetworkId.size();
            }
        }

        private boolean isCurrentSignalAuthUsable(long now, long leadMs) {
            return isAuthUsable(currentSignalAuth, currentSignalAuthAtMs, currentSignalAuthExpiresAtMs, now, leadMs);
        }

        private int clearPrimedStatesForLoggedInUser() {
            synchronized (routesByLocalNetworkId) {
                if (primedStatesByLocalNetworkId.isEmpty()) {
                    return 0;
                }
                int removedCount = primedStatesByLocalNetworkId.size();
                for (String key : new ArrayList<>(primedStatesByLocalNetworkId.keySet())) {
                    TargetRoute route = routesByLocalNetworkId.remove(key);
                    if (route != null) {
                        routeMissingSinceByNetherId.remove(route.nethernetId());
                    }
                }
                primedStatesByLocalNetworkId.clear();
                applyAdvertisementsLocked();
                logRouteSummaryLocked();
                info("Cleared %s primed advertisements reason=logged_in_source_of_truth", removedCount);
                return 0;
            }
        }

        private boolean isPrimedStateUsable(PrimedRouteState primedState, long now, long leadMs) {
            return primedState != null
                    && EggnetCommunitySupport.hasText(primedState.actorXuid())
                    && isAuthUsable(
                    primedState.authorization(),
                    0L,
                    primedState.authExpiresAtMs(),
                    now,
                    leadMs
            );
        }

        private boolean isAuthUsable(String authorization, long issuedAtMs, long expiresAtMs, long now, long leadMs) {
            if (!EggnetCommunitySupport.hasText(authorization)) {
                return false;
            }
            if (expiresAtMs > 0L) {
                return expiresAtMs > now + Math.max(0L, leadMs);
            }
            return issuedAtMs > 0L
                    && now - issuedAtMs < Math.max(30_000L, args.authRefreshMs);
        }

        private SignalAuthorization fetchRandomActorAuthorization() {
            try {
                EggnetCommunitySupport.ActorSessionAuthorization signalAuth =
                        EggnetCommunitySupport.fetchRandomActorAuthorization(
                                JSON,
                                HTTP,
                                30_000L
                        );
                return new SignalAuthorization(signalAuth.authorization(), signalAuth.expiresAtMs());
            } catch (Exception e) {
                warn("random actor auth fetch failed: %s", e.getMessage());
            }
            return null;
        }

        private String firstNonBlank(String... candidates) {
            for (String candidate : candidates) {
                if (EggnetCommunitySupport.hasText(candidate)) {
                    return candidate.trim();
                }
            }
            return "";
        }

        private void killPortHoldersIfNeeded(int port) {
            if (!isWindows()) {
                return;
            }
            long currentPid = ProcessHandle.current().pid();
            String command = "$ids=@(); "
                    + "try { $ids += Get-NetTCPConnection -State Listen -LocalPort " + port + " -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess } catch {} ; "
                    + "try { $ids += Get-NetUDPEndpoint -LocalPort " + port + " -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess } catch {} ; "
                    + "$ids = $ids | Where-Object { $_ -and $_ -ne " + currentPid + " } | Select-Object -Unique; "
                    + "foreach ($id in $ids) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue; Write-Output ('killed=' + $id) }";
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy", "Bypass",
                    "-Command", command
            );
            processBuilder.redirectErrorStream(true);
            try {
                Process process = processBuilder.start();
                String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                process.waitFor();
                for (String line : output.split("\\R")) {
                    String trimmed = line == null ? "" : line.trim();
                    if (trimmed.startsWith("killed=")) {
                        info("Killed existing port %s holder pid=%s", port, trimmed.substring("killed=".length()).trim());
                    }
                }
            } catch (Exception e) {
                warn("port %s cleanup failed: %s", port, e.getMessage());
            }
        }
    }

    private static final class LocalInboundHandler extends ChannelInboundHandlerAdapter {
        private final ConnectorRuntime runtime;
        private RelaySession relay;

        private LocalInboundHandler(ConnectorRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String localNetworkId = ctx.channel().attr(NetherNetServerChannel.ROUTE_LOCAL_NETWORK_ID_ATTR).get();
            TargetRoute route = runtime.routeForLocalNetworkId(localNetworkId);
            String authHeader = runtime.signalAuthForLocalNetworkId(localNetworkId);
            if (route == null) {
                warn("No target route for clicked LAN localNetworkId=%s", localNetworkId);
                ctx.close();
                return;
            }
            if (!EggnetCommunitySupport.hasText(authHeader)) {
                warn("Missing current signal auth for route %s", route.remoteTargetId());
                ctx.close();
                return;
            }
            relay = new RelaySession(runtime, route, authHeader, runtime.eventLoopGroup, runtime.peerFactory);
            relay.onActive(ctx.channel(), RelaySide.LOCAL);
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                if (relay != null && msg instanceof ByteBuf buf) {
                    relay.forward(buf, RelaySide.LOCAL);
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (relay != null) {
                relay.onInactive(RelaySide.LOCAL);
            }
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (relay != null) {
                relay.onException(cause, RelaySide.LOCAL);
            }
            ctx.close();
        }
    }

        private enum RelaySide {
            LOCAL,
            REMOTE
        }

        private record SignalAuthorization(String authorization, long expiresAtMs) {
        }

        private static String stringValue(Object raw) {
            return raw == null ? "" : String.valueOf(raw).trim();
        }

        private static long longValue(Object raw, long fallback) {
            if (raw instanceof Number number) {
                return number.longValue();
            }
            try {
                String text = stringValue(raw);
                if (text.isEmpty()) {
                    return fallback;
                }
                return Long.parseLong(text);
            } catch (Exception ignored) {
                return fallback;
            }
        }

    private record TargetRoute(
            String nethernetId,
            String pmsgId,
            String ownerXuid,
            String ownerGamertag,
            String worldName,
            String hostName,
            String displayTitle,
            int gameType,
            String primaryLanguage,
            int memberCount,
            int maxMemberCount,
            int connectionType,
            int transportLayer
    ) {
        static TargetRoute fromLive(EggnetCommunitySupport.LiveServerInfo live) {
            return new TargetRoute(
                    live.nethernetId(),
                    live.pmsgId(),
                    live.ownerXuid(),
                    live.ownerGamertag(),
                    live.worldName(),
                    live.hostName(),
                    live.displayTitle(),
                    live.gameType(),
                    EggnetCommunitySupport.normalizeLanguageCode(live.primaryLanguage()),
                    live.memberCount(),
                    live.maxMemberCount(),
                    live.connectionType(),
                    live.transportLayer()
            );
        }

        static TargetRoute fromMap(Map<?, ?> raw) {
            return new TargetRoute(
                    stringValue(raw.get("nethernetId")),
                    stringValue(raw.get("pmsgId")),
                    stringValue(raw.get("ownerXuid")),
                    stringValue(raw.get("ownerGamertag")),
                    stringValue(raw.get("worldName")),
                    stringValue(raw.get("hostName")),
                    stringValue(raw.get("displayTitle")),
                    Math.max(0, (int) longValue(raw.get("gameType"), 0L)),
                    EggnetCommunitySupport.normalizeLanguageCode(stringValue(raw.get("primaryLanguage"))),
                    Math.max(0, (int) longValue(raw.get("memberCount"), 0L)),
                    Math.max(0, (int) longValue(raw.get("maxMemberCount"), 0L)),
                    Math.max(0, (int) longValue(raw.get("connectionType"), EggnetCommunitySupport.ADVERTISEMENT_CONNECTION_TYPE)),
                    Math.max(0, (int) longValue(raw.get("transportLayer"), EggnetCommunitySupport.ADVERTISEMENT_TRANSPORT_LAYER))
            );
        }

        String remoteTargetId() {
            return EggnetCommunitySupport.hasText(pmsgId) ? pmsgId : nethernetId;
        }

        boolean useJsonRpc() {
            return EggnetCommunitySupport.hasText(pmsgId);
        }
    }

    private record PrimedRouteState(
            String actorXuid,
            String authorization,
            long authExpiresAtMs
    ) { }

    private static final class Args {
        private String xuidOverride = "";
        private long refreshMs = DEFAULT_REFRESH_MS;
        private long authRefreshMs = DEFAULT_AUTH_REFRESH_MS;
        private boolean desktopManaged = false;

        static Args parse(String[] args) {
            Args parsed = new Args();
            for (int i = 0; i < args.length; i++) {
                String arg = Objects.requireNonNullElse(args[i], "").trim();
                if (arg.isEmpty()) {
                    continue;
                }
                switch (arg) {
                    case "--xuid" -> {
                        if (i + 1 < args.length) {
                            parsed.xuidOverride = Objects.requireNonNullElse(args[++i], "").trim();
                        }
                    }
                    case "--refresh-ms" -> {
                        if (i + 1 < args.length) {
                            parsed.refreshMs = parsePositiveLong(args[++i], DEFAULT_REFRESH_MS);
                        }
                    }
                    case "--auth-refresh-ms" -> {
                        if (i + 1 < args.length) {
                            parsed.authRefreshMs = parsePositiveLong(args[++i], DEFAULT_AUTH_REFRESH_MS);
                        }
                    }
                    case "--desktop-managed" -> parsed.desktopManaged = true;
                    default -> {
                    }
                }
            }
            return parsed;
        }

        private static long parsePositiveLong(String raw, long fallback) {
            try {
                long parsed = Long.parseLong(String.valueOf(raw).trim());
                return parsed > 0L ? parsed : fallback;
            } catch (Exception ignored) {
                return fallback;
            }
        }
    }

    private static final class RelaySession {
        private final ConnectorRuntime runtime;
        private final TargetRoute route;
        private final String authHeader;
        private final NioEventLoopGroup eventLoopGroup;
        private final PeerConnectionFactory peerFactory;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Queue<ByteBuf> localPending = new ArrayDeque<>();
        private final Queue<ByteBuf> remotePending = new ArrayDeque<>();
        private volatile Channel local;
        private volatile Channel remote;

        private RelaySession(
                ConnectorRuntime runtime,
                TargetRoute route,
                String authHeader,
                NioEventLoopGroup eventLoopGroup,
                PeerConnectionFactory peerFactory
        ) {
            this.runtime = runtime;
            this.route = route;
            this.authHeader = authHeader;
            this.eventLoopGroup = eventLoopGroup;
            this.peerFactory = peerFactory;
        }

        void onActive(Channel channel, RelaySide side) {
            if (side == RelaySide.LOCAL) {
                this.local = channel;
                info("Local LAN client connected world=%s host=%s target=%s mode=%s",
                        route.displayTitle(),
                        route.hostName(),
                        route.remoteTargetId(),
                        route.useJsonRpc() ? "jsonrpc" : "signal");
                connectRemote(channel);
                return;
            }
            this.remote = channel;
            info("Remote NetherNet channel active target=%s", route.remoteTargetId());
            flushPending(localPending, remote);
        }

        void forward(ByteBuf data, RelaySide fromSide) {
            Channel destination = fromSide == RelaySide.LOCAL ? remote : local;
            Queue<ByteBuf> backlog = fromSide == RelaySide.LOCAL ? localPending : remotePending;
            synchronized (backlog) {
                if (destination == null || !destination.isActive()) {
                    backlog.add(data.retainedDuplicate());
                    return;
                }
            }
            destination.writeAndFlush(data.retainedDuplicate());
        }

        void onInactive(RelaySide side) {
            if (closed.compareAndSet(false, true)) {
                info("%s side closed target=%s", side.name().toLowerCase(Locale.ROOT), route.remoteTargetId());
                closeBoth();
            }
        }

        void onException(Throwable cause, RelaySide side) {
            warn("%s side exception target=%s err=%s",
                    side.name().toLowerCase(Locale.ROOT),
                    route.remoteTargetId(),
                    String.valueOf(cause));
            onInactive(side);
        }

        private void connectRemote(Channel localChannel) {
            NetherNetClientSignaling signaling = route.useJsonRpc()
                    ? new NetherNetXboxRpcSignaling(authHeader)
                    : new NetherNetXboxSignaling(authHeader);

            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoopGroup)
                    .channelFactory(NetherNetChannelFactory.client(peerFactory, signaling))
                    .option(NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 7000)
                    .handler(new ChannelInitializer<NetherNetClientChannel>() {
                        @Override
                        protected void initChannel(NetherNetClientChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("connector-remote", new RemoteInboundHandler(RelaySession.this));
                        }
                    });

            ChannelFuture future = bootstrap.connect(new NetherNetAddress(route.remoteTargetId()));
            future.addListener(listener -> {
                if (!listener.isSuccess()) {
                    String message = listener.cause() == null ? "unknown" : listener.cause().getMessage();
                    warn("Remote connect failed target=%s err=%s", route.remoteTargetId(), message);
                    if (message != null) {
                        String lower = message.toLowerCase(Locale.ROOT);
                        if (lower.contains("unauthorized") || lower.contains("401")) {
                            if (!runtime.dropPrimedRoute(route)) {
                                runtime.invalidateSignalAuth();
                            }
                        }
                        if (lower.contains("not found") || lower.contains("offline")) {
                            runtime.dropRoute(route);
                        }
                    }
                    runtime.triggerRefreshAsync();
                    onInactive(RelaySide.LOCAL);
                    localChannel.close();
                    return;
                }
                this.remote = future.channel();
                flushPending(localPending, future.channel());
            });
        }

        private void closeBoth() {
            Channel localChannel = this.local;
            Channel remoteChannel = this.remote;
            releasePending(localPending);
            releasePending(remotePending);
            if (localChannel != null) {
                localChannel.close();
            }
            if (remoteChannel != null) {
                remoteChannel.close();
            }
        }

        private void flushPending(Queue<ByteBuf> queue, Channel destination) {
            if (destination == null || !destination.isActive()) {
                return;
            }
            synchronized (queue) {
                ByteBuf pending;
                while ((pending = queue.poll()) != null) {
                    destination.writeAndFlush(pending);
                }
            }
        }

        private void releasePending(Queue<ByteBuf> queue) {
            synchronized (queue) {
                ByteBuf pending;
                while ((pending = queue.poll()) != null) {
                    ReferenceCountUtil.release(pending);
                }
            }
        }
    }

    private static final class RemoteInboundHandler extends ChannelInboundHandlerAdapter {
        private final RelaySession relay;

        private RemoteInboundHandler(RelaySession relay) {
            this.relay = relay;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            relay.onActive(ctx.channel(), RelaySide.REMOTE);
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                if (msg instanceof ByteBuf buf) {
                    relay.forward(buf, RelaySide.REMOTE);
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            relay.onInactive(RelaySide.REMOTE);
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            relay.onException(cause, RelaySide.REMOTE);
            ctx.close();
        }
    }

    private static boolean isWindows() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return osName.contains("win");
    }

    private static String truncate(String input, int max) {
        String value = input == null ? "" : input.trim();
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, Math.max(0, max - 3)) + "...";
    }

    private static void info(String format, Object... args) {
        System.out.println("[connector] " + String.format(Locale.ROOT, format, args));
    }

    private static void warn(String format, Object... args) {
        System.err.println("[connector] " + String.format(Locale.ROOT, format, args));
    }
}

