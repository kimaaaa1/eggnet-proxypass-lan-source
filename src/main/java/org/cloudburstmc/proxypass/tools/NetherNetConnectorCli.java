package org.cloudburstmc.proxypass.tools;

import com.fasterxml.jackson.databind.JsonNode;
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

import java.awt.AWTException;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.RenderingHints;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.image.BufferedImage;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.http.HttpClient;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
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
import javax.imageio.ImageIO;

public final class NetherNetConnectorCli {
    private static final String COMMAND_ACK_PREFIX = "__eggnet_cmd_ack__:";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(EggnetCommunitySupport.CONNECT_TIMEOUT)
            .build();
    private static final long DEFAULT_REFRESH_MS = 5_000L;
    private static final long DEFAULT_AUTH_REFRESH_MS = 10 * 60 * 1_000L;
    private static final long AUTH_REFRESH_LEAD_MS = 120_000L;
    private static final long AUTH_MIN_ACTIVE_MS = 5_000L;
    private static final int LOCAL_DISCOVERY_PORT = 8282;
    private static final int MINECRAFT_DISCOVERY_PORT = 7551;
    private static final long LAN_ADVERTISEMENT_MS = 800L;
    private static final long ROUTE_MISS_GRACE_MS = 30_000L;
    private static final long DYNAMIC_LOCAL_ID_START = 4_000_000_000_000_000_000L;
    private static final String TRAY_ICON_RESOURCE = "/eggnet-tray-icon.png";
    private static final String LOCAL_GUEST_FOLLOWS_FILE = "local_guest_follows.json";
    private static final String LOCAL_GUEST_FOLLOWS_FILE_SYS_PROP = "eggnet.localGuestFollowsFile";
    private static final int LOCAL_GUEST_FOLLOW_LIMIT = 200;
    private static final String DESKTOP_APP_EXE_ENV = "EGGNET_DESKTOP_APP_EXE";
    private static final String OWN_WORLD_WAKE_ARG = "--eggnet-own-world-found";
    private static final long OWN_WORLD_WAKE_COOLDOWN_MS = 10 * 60 * 1000L;
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
        private final Object localGuestFollowsLock = new Object();
        private volatile Channel server;
        private volatile NetherNetDiscoverySignaling discoverySignaling;
        private volatile String currentDesktopXuid = "";
        private volatile String currentSignalAuth = "";
        private volatile long currentSignalAuthAtMs = 0L;
        private volatile long currentSignalAuthExpiresAtMs = 0L;
        private volatile String lastRouteSignature = "";
        private volatile String lastLoginState = "";
        private volatile String lastObservedOwnWorldKey = "";
        private volatile String lastOwnWorldWakeKey = "";
        private volatile long lastOwnWorldWakeAtMs = 0L;
        private volatile TrayIcon trayIcon;
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
            refreshExecutor.scheduleWithFixedDelay(
                    this::broadcastAdvertisementsSafely,
                    250L,
                    LAN_ADVERTISEMENT_MS,
                    TimeUnit.MILLISECONDS
            );
            if (args.desktopResident) {
                installTrayIcon();
            }
            if (args.desktopManaged || args.desktopResident) {
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

        private void installTrayIcon() {
            try {
                if (!SystemTray.isSupported()) {
                    warn("System tray is not supported; Eggnet LAN will keep running without tray icon");
                    return;
                }

                PopupMenu menu = new PopupMenu();
                MenuItem status = new MenuItem("Eggnet LAN running");
                status.setEnabled(false);
                MenuItem refresh = new MenuItem("Refresh LAN");
                refresh.addActionListener(event -> triggerRefreshAsync());
                MenuItem stop = new MenuItem("Stop Eggnet LAN");
                stop.addActionListener(event -> close());
                menu.add(status);
                menu.addSeparator();
                menu.add(refresh);
                menu.addSeparator();
                menu.add(stop);

                TrayIcon icon = new TrayIcon(createTrayImage(), "Eggnet LAN", menu);
                icon.setImageAutoSize(true);
                icon.addActionListener(event -> triggerRefreshAsync());
                SystemTray.getSystemTray().add(icon);
                this.trayIcon = icon;
                info("Eggnet LAN tray icon installed");
            } catch (AWTException | RuntimeException | Error e) {
                warn("Failed to install Eggnet LAN tray icon: %s", e.getMessage());
            }
        }

        private static BufferedImage createTrayImage() {
            try (InputStream input = NetherNetConnectorCli.class.getResourceAsStream(TRAY_ICON_RESOURCE)) {
                if (input != null) {
                    BufferedImage officialIcon = ImageIO.read(input);
                    if (officialIcon != null) {
                        return officialIcon;
                    }
                }
            } catch (Exception e) {
                warn("Failed to load Eggnet tray icon resource: %s", e.getMessage());
            }

            int size = 32;
            BufferedImage image = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = image.createGraphics();
            try {
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g.setColor(new Color(0x102033));
                g.fillRoundRect(1, 1, 30, 30, 8, 8);
                g.setColor(new Color(0xFFD36A));
                g.fillOval(8, 4, 16, 22);
                g.setColor(new Color(0xFFF4E0));
                g.fillOval(10, 6, 12, 18);
                g.setColor(new Color(0x22C55E));
                g.fillRoundRect(22, 17, 3, 8, 2, 2);
                g.fillRoundRect(26, 13, 3, 12, 2, 2);
                g.setColor(Color.WHITE);
                g.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 8));
                g.drawString("LAN", 6, 30);
            } finally {
                g.dispose();
            }
            return image;
        }

        void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            TrayIcon icon = this.trayIcon;
            this.trayIcon = null;
            if (icon != null) {
                try {
                    SystemTray.getSystemTray().remove(icon);
                } catch (Exception ignored) {
                }
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
                rememberLocalGuestFollow(route);
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

        private void rememberLocalGuestFollow(TargetRoute route) {
            if (route == null) {
                return;
            }
            String ownerXuid = EggnetCommunitySupport.normalizeXuid(route.ownerXuid());
            String nethernetId = route.nethernetId();
            if (!EggnetCommunitySupport.hasText(ownerXuid) && !EggnetCommunitySupport.hasText(nethernetId)) {
                return;
            }
            synchronized (localGuestFollowsLock) {
                LocalGuestFollows existing = readLocalGuestFollowsLocked();
                Set<String> ownerXuids = limitedSet(ownerXuid, existing.ownerXuids());
                Set<String> nethernetIds = limitedSet(nethernetId, existing.nethernetIds());
                writeLocalGuestFollowsLocked(ownerXuids, nethernetIds);
            }
        }

        private Set<String> limitedSet(String first, Set<String> rest) {
            Set<String> out = new LinkedHashSet<>();
            if (EggnetCommunitySupport.hasText(first)) {
                out.add(first.trim());
            }
            for (String value : rest) {
                if (!EggnetCommunitySupport.hasText(value)) {
                    continue;
                }
                out.add(value.trim());
                if (out.size() >= LOCAL_GUEST_FOLLOW_LIMIT) {
                    break;
                }
            }
            return out;
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

        private void broadcastAdvertisementsSafely() {
            if (closed.get()) {
                return;
            }
            NetherNetDiscoverySignaling signaling = this.discoverySignaling;
            if (signaling == null) {
                return;
            }
            try {
                signaling.sendDiscoveryResponsesTo(minecraftDiscoveryTargets());
            } catch (Throwable t) {
                warn("LAN advertisement broadcast failed: %s", t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
            }
        }

        private List<InetSocketAddress> minecraftDiscoveryTargets() {
            Map<String, InetSocketAddress> targets = new LinkedHashMap<>();
            try {
                addMinecraftDiscoveryTarget(targets, InetAddress.getByName("255.255.255.255"));
            } catch (Exception ignored) {
            }

            try {
                var interfaces = NetworkInterface.getNetworkInterfaces();
                if (interfaces != null) {
                    for (NetworkInterface networkInterface : Collections.list(interfaces)) {
                        if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                            continue;
                        }
                        for (InterfaceAddress address : networkInterface.getInterfaceAddresses()) {
                            InetAddress broadcast = address.getBroadcast();
                            if (broadcast != null) {
                                addMinecraftDiscoveryTarget(targets, broadcast);
                            }
                        }
                    }
                }
            } catch (Exception ignored) {
            }
            return new ArrayList<>(targets.values());
        }

        private void addMinecraftDiscoveryTarget(Map<String, InetSocketAddress> targets, InetAddress address) {
            String key = address.getHostAddress() + ":" + MINECRAFT_DISCOVERY_PORT;
            targets.putIfAbsent(key, new InetSocketAddress(address, MINECRAFT_DISCOVERY_PORT));
        }

        private void refreshOnce() throws Exception {
            long now = System.currentTimeMillis();
            int activePrimedCount = pruneExpiredPrimedStates(now);
            String selfXuid = resolveSelfXuid();
            LocalGuestFollows localGuestFollows = EggnetCommunitySupport.hasText(selfXuid)
                    ? LocalGuestFollows.empty()
                    : readLocalGuestFollows();
            if (EggnetCommunitySupport.hasText(selfXuid)) {
                activePrimedCount = clearPrimedStatesForLoggedInUser();
            }
            if (!EggnetCommunitySupport.hasText(selfXuid) && activePrimedCount == 0 && localGuestFollows.isEmpty()) {
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
            if (EggnetCommunitySupport.hasText(selfXuid) || activePrimedCount > 0 || !localGuestFollows.isEmpty()) {
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
            if (EggnetCommunitySupport.hasText(selfXuid)) {
                maybeWakeEggnetForOwnWorld(selfXuid, liveByNether);
            }
            if (!EggnetCommunitySupport.hasText(selfXuid) && activePrimedCount == 0 && localGuestFollows.isEmpty()) {
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
            } else if (!localGuestFollows.isEmpty()) {
                for (EggnetCommunitySupport.LiveServerInfo live : liveByNether.values()) {
                    if (!localGuestFollows.matches(live.ownerXuid(), live.nethernetId())) {
                        continue;
                    }
                    routes.add(TargetRoute.fromLive(live));
                }
            }
            sortRoutesForLanTab(routes, resolvePreferredLanguage(), captureStableRouteOrder());

            applyRoutes(routes);
        }

        private LocalGuestFollows readLocalGuestFollows() {
            synchronized (localGuestFollowsLock) {
                return readLocalGuestFollowsLocked();
            }
        }

        private LocalGuestFollows readLocalGuestFollowsLocked() {
            Path path = localGuestFollowsPath();
            if (path == null || !Files.isRegularFile(path)) {
                return LocalGuestFollows.empty();
            }
            try {
                JsonNode root = JSON.readTree(path.toFile());
                Set<String> ownerXuids = new LinkedHashSet<>();
                Set<String> nethernetIds = new LinkedHashSet<>();
                readLocalGuestFollowNode(root.path("ownerXuids"), ownerXuids, true);
                readLocalGuestFollowNode(root.path("nethernetIds"), nethernetIds, false);
                if (root.isArray()) {
                    for (JsonNode item : root) {
                        addLocalGuestFollowOwner(ownerXuids, item.path("ownerXuid").asText(""));
                        addLocalGuestFollowNether(nethernetIds, item.path("nethernetId").asText(""));
                    }
                } else {
                    JsonNode items = root.path("items");
                    if (items.isArray()) {
                        for (JsonNode item : items) {
                            addLocalGuestFollowOwner(ownerXuids, item.path("ownerXuid").asText(""));
                            addLocalGuestFollowNether(nethernetIds, item.path("nethernetId").asText(""));
                        }
                    }
                }
                return new LocalGuestFollows(ownerXuids, nethernetIds);
            } catch (Exception e) {
                warn("Failed to read local guest follows: %s", e.getMessage());
                return LocalGuestFollows.empty();
            }
        }

        private void readLocalGuestFollowNode(JsonNode node, Set<String> out, boolean ownerXuid) {
            if (!node.isArray()) {
                return;
            }
            for (JsonNode item : node) {
                if (ownerXuid) {
                    addLocalGuestFollowOwner(out, item.asText(""));
                } else {
                    addLocalGuestFollowNether(out, item.asText(""));
                }
            }
        }

        private void addLocalGuestFollowOwner(Set<String> out, String raw) {
            String xuid = EggnetCommunitySupport.normalizeXuid(raw);
            if (EggnetCommunitySupport.hasText(xuid)) {
                out.add(xuid);
            }
        }

        private void addLocalGuestFollowNether(Set<String> out, String raw) {
            String nethernetId = raw == null ? "" : raw.trim();
            if (EggnetCommunitySupport.hasText(nethernetId)) {
                out.add(nethernetId);
            }
        }

        private void writeLocalGuestFollowsLocked(Set<String> ownerXuids, Set<String> nethernetIds) {
            Path path = localGuestFollowsPath();
            if (path == null) {
                return;
            }
            try {
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("ownerXuids", new ArrayList<>(ownerXuids));
                payload.put("nethernetIds", new ArrayList<>(nethernetIds));
                payload.put("updatedAtMs", System.currentTimeMillis());
                JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), payload);
            } catch (Exception e) {
                warn("Failed to write local guest follows: %s", e.getMessage());
            }
        }

        private Path localGuestFollowsPath() {
            String override = System.getProperty(LOCAL_GUEST_FOLLOWS_FILE_SYS_PROP, "").trim();
            if (EggnetCommunitySupport.hasText(override)) {
                try {
                    return Paths.get(override);
                } catch (Exception ignored) {
                }
            }
            String appData = System.getenv("APPDATA");
            if (EggnetCommunitySupport.hasText(appData)) {
                return Paths.get(appData, "Eggnet Arcade", LOCAL_GUEST_FOLLOWS_FILE);
            }
            String home = System.getenv("HOME");
            if (EggnetCommunitySupport.hasText(home)) {
                return Paths.get(home, ".config", "Eggnet Arcade", LOCAL_GUEST_FOLLOWS_FILE);
            }
            return Paths.get(".").toAbsolutePath().normalize().resolve(LOCAL_GUEST_FOLLOWS_FILE);
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

        private void maybeWakeEggnetForOwnWorld(
                String selfXuid,
                Map<String, EggnetCommunitySupport.LiveServerInfo> liveByNether
        ) {
            String normalizedSelf = EggnetCommunitySupport.normalizeXuid(selfXuid);
            if (!EggnetCommunitySupport.hasText(normalizedSelf) || liveByNether == null || liveByNether.isEmpty()) {
                lastObservedOwnWorldKey = "";
                return;
            }
            for (EggnetCommunitySupport.LiveServerInfo live : liveByNether.values()) {
                if (!normalizedSelf.equals(EggnetCommunitySupport.normalizeXuid(live.ownerXuid()))) {
                    continue;
                }
                String key = EggnetCommunitySupport.firstNonBlank(
                        live.nethernetId(),
                        live.pmsgId(),
                        live.sessionName(),
                        live.displayTitle()
                );
                if (!EggnetCommunitySupport.hasText(key)) {
                    key = normalizedSelf;
                }
                if (key.equals(lastObservedOwnWorldKey)) {
                    return;
                }
                lastObservedOwnWorldKey = key;
                long now = System.currentTimeMillis();
                if (key.equals(lastOwnWorldWakeKey) && now - lastOwnWorldWakeAtMs < OWN_WORLD_WAKE_COOLDOWN_MS) {
                    return;
                }
                if (wakeEggnetDesktopForOwnWorld(live)) {
                    lastOwnWorldWakeKey = key;
                    lastOwnWorldWakeAtMs = now;
                }
                return;
            }
            lastObservedOwnWorldKey = "";
        }

        private boolean wakeEggnetDesktopForOwnWorld(EggnetCommunitySupport.LiveServerInfo live) {
            Path desktopExe = resolveEggnetDesktopExe();
            if (desktopExe == null) {
                warn("own-world wake skipped: Eggnet desktop exe not found");
                return false;
            }
            if (isEggnetDesktopAppRunning(desktopExe)) {
                info("own-world wake skipped: Eggnet desktop app already running");
                return false;
            }
            try {
                ProcessBuilder builder = new ProcessBuilder(desktopExe.toString(), OWN_WORLD_WAKE_ARG);
                Path parent = desktopExe.getParent();
                if (parent != null) {
                    builder.directory(parent.toFile());
                }
                builder.start();
                info("own-world wake launched Eggnet app world=%s host=%s",
                        live.displayTitle(),
                        live.hostName());
                return true;
            } catch (Exception e) {
                warn("own-world wake launch failed: %s", e.getMessage());
                return false;
            }
        }

        private Path resolveEggnetDesktopExe() {
            String explicit = Objects.requireNonNullElse(System.getenv(DESKTOP_APP_EXE_ENV), "").trim();
            if (EggnetCommunitySupport.hasText(explicit)) {
                Path path = safePath(explicit);
                if (path != null && Files.isRegularFile(path)) {
                    return path.toAbsolutePath().normalize();
                }
            }

            Path connectorPath = currentProcessPath();
            Path connectorDir = connectorPath == null ? null : connectorPath.getParent();
            if (connectorDir == null) {
                return null;
            }
            String[] candidates = new String[]{
                    "eggnet_fresh_web_ui.exe",
                    "Eggnet Arcade.exe"
            };
            for (String candidate : candidates) {
                Path path = connectorDir.resolve(candidate).toAbsolutePath().normalize();
                if (Files.isRegularFile(path)) {
                    return path;
                }
            }
            return null;
        }

        private Path currentProcessPath() {
            String command = ProcessHandle.current().info().command().orElse("").trim();
            Path commandPath = safePath(command);
            if (commandPath != null && Files.isRegularFile(commandPath)) {
                return commandPath.toAbsolutePath().normalize();
            }
            return null;
        }

        private boolean isEggnetDesktopAppRunning(Path desktopExe) {
            String expected = normalizePathString(desktopExe);
            if (!EggnetCommunitySupport.hasText(expected)) {
                return false;
            }
            return ProcessHandle.allProcesses().anyMatch(handle -> {
                String command = handle.info().command().orElse("").trim();
                if (!EggnetCommunitySupport.hasText(command)) {
                    return false;
                }
                Path commandPath = safePath(command);
                if (commandPath == null) {
                    return false;
                }
                return expected.equals(normalizePathString(commandPath));
            });
        }

        private Path safePath(String raw) {
            try {
                String value = Objects.requireNonNullElse(raw, "").trim();
                if (!EggnetCommunitySupport.hasText(value)) {
                    return null;
                }
                return Paths.get(value);
            } catch (Exception ignored) {
                return null;
            }
        }

        private String normalizePathString(Path path) {
            if (path == null) {
                return "";
            }
            try {
                return path.toAbsolutePath().normalize().toString().toLowerCase(Locale.ROOT);
            } catch (Exception ignored) {
                return path.toString().toLowerCase(Locale.ROOT);
            }
        }

        private void ensureServerRunning() throws Exception {
            synchronized (serverLifecycleLock) {
                Channel existing = this.server;
                if (existing != null && existing.isActive() && discoverySignaling != null) {
                    return;
                }

                closeServerLocked();

                NetherNetDiscoverySignaling nextDiscovery = new NetherNetDiscoverySignaling();
                ChannelFuture bindFuture = createServerBootstrap(nextDiscovery)
                        .bind(new InetSocketAddress("0.0.0.0", LOCAL_DISCOVERY_PORT))
                        .sync();

                this.discoverySignaling = nextDiscovery;
                this.server = bindFuture.channel();
                InetSocketAddress bound = (InetSocketAddress) bindFuture.channel().localAddress();
                info("Connector listening on %s; broadcasting LAN advertisements to UDP %s", bound, MINECRAFT_DISCOVERY_PORT);
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
            String serverName = lanDisplayName(EggnetCommunitySupport.firstNonBlank(
                    route.hostName(),
                    route.ownerGamertag(),
                    route.displayTitle(),
                    "Server"
            ), 48);
            String levelName = lanDisplayName(EggnetCommunitySupport.firstNonBlank(
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

    private record LocalGuestFollows(Set<String> ownerXuids, Set<String> nethernetIds) {
        static LocalGuestFollows empty() {
            return new LocalGuestFollows(Set.of(), Set.of());
        }

        boolean isEmpty() {
            return ownerXuids.isEmpty() && nethernetIds.isEmpty();
        }

        boolean matches(String ownerXuid, String nethernetId) {
            String safeOwner = EggnetCommunitySupport.normalizeXuid(ownerXuid);
            String safeNether = nethernetId == null ? "" : nethernetId.trim();
            return (EggnetCommunitySupport.hasText(safeOwner) && ownerXuids.contains(safeOwner))
                    || (EggnetCommunitySupport.hasText(safeNether) && nethernetIds.contains(safeNether));
        }
    }

    private static final class Args {
        private String xuidOverride = "";
        private long refreshMs = DEFAULT_REFRESH_MS;
        private long authRefreshMs = DEFAULT_AUTH_REFRESH_MS;
        private boolean desktopManaged = false;
        private boolean desktopResident = false;

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
                    case "--desktop-resident" -> parsed.desktopResident = true;
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

    private static String truncate(String input, int max) {
        String value = input == null ? "" : input.trim();
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, Math.max(0, max - 3)) + "...";
    }

    private static String lanDisplayName(String input, int max) {
        if (max <= 1) {
            return truncate(input, max);
        }
        return truncate(input, max - 1) + " ";
    }

    private static void info(String format, Object... args) {
        System.out.println("[connector] " + String.format(Locale.ROOT, format, args));
    }

    private static void warn(String format, Object... args) {
        System.err.println("[connector] " + String.format(Locale.ROOT, format, args));
    }
}

