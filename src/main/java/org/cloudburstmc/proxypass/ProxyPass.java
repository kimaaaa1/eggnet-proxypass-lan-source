package org.cloudburstmc.proxypass;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.kastle.netty.channel.nethernet.NetherNetChannelFactory;
import dev.kastle.netty.channel.nethernet.config.NetherChannelOption;
import dev.kastle.netty.channel.nethernet.config.NetherNetAddress;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetClientSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetDiscoverySignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetXboxRpcSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetXboxSignaling;
import dev.kastle.webrtc.PeerConnectionFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.ResourceLeakDetector;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import net.lenni0451.commons.httpclient.HttpClient;
import net.raphimc.minecraftauth.MinecraftAuth;
import net.raphimc.minecraftauth.bedrock.BedrockAuthManager;
import net.raphimc.minecraftauth.msa.model.MsaApplicationConfig;
import net.raphimc.minecraftauth.msa.model.MsaDeviceCode;
import net.raphimc.minecraftauth.msa.service.impl.DeviceCodeMsaAuthService;
import net.raphimc.minecraftauth.util.MinecraftAuth4To5Migrator;
import org.cloudburstmc.nbt.*;
import org.cloudburstmc.netty.channel.raknet.RakChannelFactory;
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption;
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerRateLimiter;
import org.cloudburstmc.protocol.bedrock.BedrockPeer;
import org.cloudburstmc.protocol.bedrock.BedrockPong;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodecHelper;
import org.cloudburstmc.protocol.bedrock.codec.v924.Bedrock_v924;
import org.cloudburstmc.protocol.bedrock.data.EncodingSettings;
import org.cloudburstmc.protocol.bedrock.data.definitions.BlockDefinition;
import org.cloudburstmc.protocol.bedrock.netty.initializer.BedrockChannelInitializer;
import org.cloudburstmc.protocol.common.DefinitionRegistry;
import org.cloudburstmc.proxypass.network.bedrock.jackson.ColorDeserializer;
import org.cloudburstmc.proxypass.network.bedrock.jackson.ColorSerializer;
import org.cloudburstmc.proxypass.network.bedrock.jackson.NbtDefinitionSerializer;
import org.cloudburstmc.proxypass.network.bedrock.nethernet.initializer.NetherNetBedrockChannelInitializer;
import org.cloudburstmc.proxypass.network.bedrock.session.Account;
import org.cloudburstmc.proxypass.network.bedrock.session.ProxyClientSession;
import org.cloudburstmc.proxypass.network.bedrock.session.ProxyServerSession;
import org.cloudburstmc.proxypass.network.bedrock.session.ServerAddress;
import org.cloudburstmc.proxypass.network.bedrock.session.UpstreamPacketHandler;
import org.cloudburstmc.proxypass.network.bedrock.util.NbtBlockDefinitionRegistry;
import org.cloudburstmc.proxypass.network.bedrock.util.UnknownBlockDefinitionRegistry;
import org.cloudburstmc.proxypass.network.bedrock.request.SessionGetRequest;
import org.cloudburstmc.proxypass.network.bedrock.request.model.sessiondirectory.SessionGetResult;
import org.cloudburstmc.proxypass.ui.PacketInspector;
import org.cloudburstmc.proxypass.xbox.XboxSessionManager;

import java.awt.Color;
import java.awt.Desktop;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Log4j2
@Getter
public class ProxyPass {
    public static final ObjectMapper JSON_MAPPER;
    private static final SimpleModule MODULE = new SimpleModule("ProxyPass", Version.unknownVersion())
            .addSerializer(Color.class, new ColorSerializer())
            .addDeserializer(Color.class, new ColorDeserializer())
            .addSerializer(NbtBlockDefinitionRegistry.NbtBlockDefinition.class, new NbtDefinitionSerializer());
    public static final YAMLMapper YAML_MAPPER = (YAMLMapper) new YAMLMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    public static final String MINECRAFT_VERSION;

    public static final BedrockCodecHelper HELPER = Bedrock_v924.CODEC.createHelper();
    public static final BedrockCodec CODEC = Bedrock_v924.CODEC
        .toBuilder()
        .protocolVersion(924)
        .minecraftVersion("1.26.0")
        .helper(() -> HELPER).build();
        
    public static final int PROTOCOL_VERSION = CODEC.getProtocolVersion();
    private static final BedrockPong ADVERTISEMENT = new BedrockPong()
            .edition("MCPE")
            .gameType("Survival")
            .version(ProxyPass.MINECRAFT_VERSION)
            .protocolVersion(ProxyPass.PROTOCOL_VERSION)
            .motd("ProxyPass")
            .playerCount(0)
            .maximumPlayerCount(20)
            .subMotd("https://github.com/CloudburstMC/ProxyPass")
            .nintendoLimited(false);
    private static final DefaultPrettyPrinter PRETTY_PRINTER;
    public static Map<Integer, String> legacyIdMap = new HashMap<>();
    private static final String LIST3_URL_DEFAULT = EggnetCommunitySupport.LIST3_URL_DEFAULT;
    private static final String COMMUNITY_FOLLOWERS_URL_DEFAULT = EggnetCommunitySupport.COMMUNITY_FOLLOWERS_URL_DEFAULT;
    private static final String COMMUNITY_FOLLOWING_URL_DEFAULT = EggnetCommunitySupport.COMMUNITY_FOLLOWING_URL_DEFAULT;
    private static final String COMMUNITY_RANDOM_ACTOR_URL_DEFAULT = "https://eggnet.space/api/xbl/random_actor";
    private static final Duration LIST2_CONNECT_TIMEOUT = EggnetCommunitySupport.CONNECT_TIMEOUT;
    private static final Duration LIST2_REQUEST_TIMEOUT = EggnetCommunitySupport.REQUEST_TIMEOUT;
    private static final long LIVE_DEST_REFRESH_SECONDS = 1L;
    private static final int LIVE_DEST_MAX_ROUTES = 20;
    private static final long COMMUNITY_REFRESH_MS_DEFAULT = 1_000L;
    private static final long DESKTOP_XUID_CACHE_MS = 3_000L;
    private static final int COMMUNITY_LIST_LIMIT = EggnetCommunitySupport.COMMUNITY_LIST_LIMIT;
    private static final int COMMUNITY_MAX_PAGES = EggnetCommunitySupport.COMMUNITY_MAX_PAGES;
    private static final int GUEST_LIVE_DEST_MAX_ROUTES = 5;
    private static final long DYNAMIC_LOCAL_ID_START = 4_000_000_000_000_000_000L;
    private static final String ENV_REFRESH_TOKEN = "PROXYPASS_REFRESH_TOKEN";
    private static final String ENV_DISABLE_AUTH_FILE = "PROXYPASS_DISABLE_AUTH_FILE";
    private static final String ENV_MSA_CLIENT_ID = "PROXYPASS_MSA_CLIENT_ID";
    private static final String ENV_MSA_SCOPE = "PROXYPASS_MSA_SCOPE";
    private static final String ENV_MSA_DEVICE_TYPE = "PROXYPASS_MSA_DEVICE_TYPE";
    private static final String ENV_SIGNAL_AUTH = "PROXYPASS_SIGNAL_AUTH";
    private static final String DEFAULT_MSA_CLIENT_ID = "00000000441cc96b";
    private static final String DEFAULT_MSA_SCOPE = "service::user.auth.xboxlive.com::MBI_SSL";
    private static final String DEFAULT_MSA_DEVICE_TYPE = "Nintendo";

    static {
        PRETTY_PRINTER = new DefaultPrettyPrinter() {
            @Override
            public DefaultPrettyPrinter createInstance() {
                return this;
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public void writeObjectFieldValueSeparator(JsonGenerator generator) throws IOException {
                generator.writeRaw(": ");
            }
        };

        DefaultIndenter indenter = new DefaultIndenter("    ", "\n");
        PRETTY_PRINTER.indentArraysWith(indenter);
        PRETTY_PRINTER.indentObjectsWith(indenter);

        JSON_MAPPER = new ObjectMapper().registerModule(MODULE).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).setDefaultPrettyPrinter(PRETTY_PRINTER);
        MINECRAFT_VERSION = CODEC.getMinecraftVersion();

        HELPER.setEncodingSettings(EncodingSettings.builder()
            .maxListSize(Integer.MAX_VALUE)
            .maxByteArraySize(Integer.MAX_VALUE)
            .maxNetworkNBTSize(Integer.MAX_VALUE)
            .maxItemNBTSize(Integer.MAX_VALUE)
            .maxStringLength(Integer.MAX_VALUE)
            .build());
    }

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final java.net.http.HttpClient list2HttpClient = java.net.http.HttpClient.newBuilder()
            .connectTimeout(LIST2_CONNECT_TIMEOUT)
            .build();
    private final ScheduledExecutorService liveDestinationRefreshExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "proxypass-live-destination-refresh");
        t.setDaemon(true);
        return t;
    });

    private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    
    private final Set<Channel> clients = ConcurrentHashMap.newKeySet();
    @Getter(AccessLevel.NONE)
    private final Set<Class<?>> ignoredPackets = Collections.newSetFromMap(new IdentityHashMap<>());
    private Channel server;
    private int maxClients = 0;
    private boolean onlineMode = false;
    private boolean saveAuthDetails = false;
    @Setter
    private SocketAddress targetAddress;
    private InetSocketAddress proxyAddress;
    private Configuration configuration;
    private ServerAddress serverAddress;
    private ResolvedDestination defaultDestination;
    private List<ResolvedDestination> configuredDestinations = Collections.emptyList();
    private volatile List<ResolvedDestination> resolvedDestinations = Collections.emptyList();
    private final Map<String, ResolvedDestination> destinationByLocalNetworkId = new ConcurrentHashMap<>();
    private NetherNetDiscoverySignaling discoverySignaling;
    private Configuration.Destination liveDestinationTemplate;
    private final AtomicLong dynamicLocalIdCursor = new AtomicLong(DYNAMIC_LOCAL_ID_START);
    private final Map<String, Long> dynamicLocalIdByNetherId = new ConcurrentHashMap<>();
    private String liveList3Url = LIST3_URL_DEFAULT;
    private EggnetCommunitySupport.LiveServerSnapshot liveServerSnapshot = EggnetCommunitySupport.LiveServerSnapshot.empty();
    private String communityFollowingUrl = COMMUNITY_FOLLOWING_URL_DEFAULT;
    private String communityFollowersUrl = COMMUNITY_FOLLOWERS_URL_DEFAULT;
    private String communityRandomActorUrl = COMMUNITY_RANDOM_ACTOR_URL_DEFAULT;
    private boolean communitySyncEnabled = true;
    private long communityRefreshMs = COMMUNITY_REFRESH_MS_DEFAULT;
    private volatile CommunityFollowGraph communityFollowGraph = CommunityFollowGraph.empty();
    private volatile String communityRandomActorXuid = "";
    private volatile String communityRandomActorAuthorization = "";
    private volatile long communityRandomActorAtMs = 0L;
    private volatile String desktopCommunityActorXuid = "";
    private volatile long desktopCommunityActorAtMs = 0L;
    private volatile String lastCommunityActorLogKey = "";
    private Path baseDir;
    private Path sessionsDir;
    private Path dataDir;
    private DefinitionRegistry<BlockDefinition> blockDefinitions;
    private DefinitionRegistry<BlockDefinition> blockDefinitionsHashed;
    private static Account account;
    private XboxSessionManager xboxSessionManager;
    private HttpClient authHttpClient;

    @Getter
    public static final class ResolvedDestination {
        private final Configuration.Destination destination;
        private final ServerAddress serverAddress;
        private final SocketAddress targetAddress;
        private final long localNetworkId;
        private final String localNetworkIdText;
        private final NetherNetServerSignaling.PongData advertisementData;
        private final String targetNethernetId;
        private final String targetPmsgId;
        private final String targetSessionScid;
        private final String targetSessionTemplateName;
        private final String targetSessionName;

        public ResolvedDestination(
                Configuration.Destination destination,
                ServerAddress serverAddress,
                SocketAddress targetAddress,
                long localNetworkId,
                String localNetworkIdText,
                NetherNetServerSignaling.PongData advertisementData,
                String targetNethernetId,
                String targetPmsgId,
                String targetSessionScid,
                String targetSessionTemplateName,
                String targetSessionName) {
            this.destination = destination;
            this.serverAddress = serverAddress;
            this.targetAddress = targetAddress;
            this.localNetworkId = localNetworkId;
            this.localNetworkIdText = localNetworkIdText;
            this.advertisementData = advertisementData;
            this.targetNethernetId = targetNethernetId;
            this.targetPmsgId = targetPmsgId;
            this.targetSessionScid = targetSessionScid;
            this.targetSessionTemplateName = targetSessionTemplateName;
            this.targetSessionName = targetSessionName;
        }
    }

    private static final class LiveServerInfo {
        private final String nethernetId;
        private final String pmsgId;
        private final String sessionScid;
        private final String sessionTemplateName;
        private final String sessionName;
        private final String ownerXuid;
        private final String ownerGamertag;
        private final String worldName;
        private final String hostName;
        private final String displayTitle;
        private final String primaryLanguage;
        private final int memberCount;
        private final int maxMemberCount;
        private final int connectionType;
        private final int transportLayer;

        private LiveServerInfo(
                String nethernetId,
                String pmsgId,
                String sessionScid,
                String sessionTemplateName,
                String sessionName,
                String ownerXuid,
                String ownerGamertag,
                String worldName,
                String hostName,
                String displayTitle,
                String primaryLanguage,
                int memberCount,
                int maxMemberCount,
                int connectionType,
                int transportLayer) {
            this.nethernetId = nethernetId;
            this.pmsgId = pmsgId;
            this.sessionScid = sessionScid;
            this.sessionTemplateName = sessionTemplateName;
            this.sessionName = sessionName;
            this.ownerXuid = ownerXuid;
            this.ownerGamertag = ownerGamertag;
            this.worldName = worldName;
            this.hostName = hostName;
            this.displayTitle = displayTitle;
            this.primaryLanguage = primaryLanguage;
            this.memberCount = memberCount;
            this.maxMemberCount = maxMemberCount;
            this.connectionType = connectionType;
            this.transportLayer = transportLayer;
        }
    }

    private static final class CommunityFollowGraph {
        private final String selfXuid;
        private final Set<String> following;
        private final Set<String> followers;
        private final long fetchedAtMs;

        private CommunityFollowGraph(String selfXuid, Set<String> following, Set<String> followers, long fetchedAtMs) {
            this.selfXuid = selfXuid;
            this.following = Collections.unmodifiableSet(new HashSet<>(following));
            this.followers = Collections.unmodifiableSet(new HashSet<>(followers));
            this.fetchedAtMs = fetchedAtMs;
        }

        private static CommunityFollowGraph empty() {
            return new CommunityFollowGraph("", Collections.emptySet(), Collections.emptySet(), 0L);
        }
    }

    private static final class CommunityActorIdentity {
        private final String xuid;
        private final String source;

        private CommunityActorIdentity(String xuid, String source) {
            this.xuid = xuid == null ? "" : xuid.trim();
            this.source = source == null ? "" : source.trim();
        }
    }

    private static final class RandomActorResult {
        private final String xuid;
        private final String authorization;

        private RandomActorResult(String xuid, String authorization) {
            this.xuid = xuid == null ? "" : xuid.trim();
            this.authorization = authorization == null ? "" : authorization.trim();
        }
    }

    public static void main(String[] args) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        
        // dev.kastle.webrtc.logging.Logging.addLogSink(dev.kastle.webrtc.logging.Logging.Severity.ERROR, (severity, message) -> {
        //     log.trace("[WebRTC Native] " + message.stripTrailing());
        // });

        ProxyPass proxy = new ProxyPass();
        try {
            proxy.boot();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void boot() throws IOException {
        log.info("Loading configuration...");
        Path configPath = Paths.get(".").resolve("config.yml");
        if (Files.notExists(configPath) || !Files.isRegularFile(configPath)) {
            Files.copy(ProxyPass.class.getClassLoader().getResourceAsStream("config.yml"), configPath, StandardCopyOption.REPLACE_EXISTING);
        }

        configuration = Configuration.load(configPath);

        if (configuration.isEnableUi()) {
            log.info("Starting Packet Inspector UI...");
            PacketInspector.launchUI();
            log.info("Packet Inspector UI started");
        }

        proxyAddress = configuration.getProxy().getAddress();
        maxClients = configuration.getMaxClients();
        onlineMode = configuration.isOnlineMode();
        saveAuthDetails = configuration.isSaveAuthDetails();

        configuration.getIgnoredPackets().forEach(s -> {
            try {
                ignoredPackets.add(Class.forName("org.cloudburstmc.protocol.bedrock.packet." + s));
            } catch (ClassNotFoundException e) {
                log.warn("No packet with name {}", s);
            }
        });

        baseDir = Paths.get(".").toAbsolutePath();
        sessionsDir = baseDir.resolve("sessions");
        dataDir = baseDir.resolve("data");
        Files.createDirectories(sessionsDir);
        Files.createDirectories(dataDir);

        HttpClient client = null;
        if (onlineMode) {
            log.info("Online mode is enabled. Starting auth process...");
            try {
                client = MinecraftAuth.createHttpClient();
                account = getAuthenticatedAccount(saveAuthDetails, client);
                log.info("Successfully logged in as {}", account.authManager().getMinecraftMultiplayerToken().getCached().getDisplayName());
            } catch (Exception e) {
                log.error("Setting to offline mode due to failure to get login chain:", e);
                onlineMode = false;
            }
        }
        this.authHttpClient = client;

        this.initializeDestinations(account, client);

        Object object = this.loadGzipNBT("block_palette.nbt");

        if (object instanceof NbtMap map) {
            this.blockDefinitions = new NbtBlockDefinitionRegistry(map.getList("blocks", NbtType.COMPOUND), false);
            this.blockDefinitionsHashed = new NbtBlockDefinitionRegistry(map.getList("blocks", NbtType.COMPOUND), true);
        } else {
            this.blockDefinitions = this.blockDefinitionsHashed = new UnknownBlockDefinitionRegistry();
            log.warn(
                    "Failed to load block palette. Blocks will appear as runtime IDs in packet traces and creative_content.json!");
        }

        log.info("Loading server...");

        String listenerTransport = configuration.getProxy().getTransport();
        boolean isNetherNetListener = "nethernet".equalsIgnoreCase(listenerTransport);

        ADVERTISEMENT.ipv4Port(this.proxyAddress.getPort())
                .ipv6Port(this.proxyAddress.getPort());

        if (isNetherNetListener) {
            NetherNetServerSignaling signaling = null;

            if (configuration.isBroadcastSession() && onlineMode) {
                try {
                    log.info("Starting Xbox Session Broadcast...");
                    this.xboxSessionManager = new XboxSessionManager(account.authManager(), client);
                    this.xboxSessionManager.setupConnection();

                    signaling = new NetherNetXboxSignaling(
                        this.xboxSessionManager.getNetherNetId(), 
                        account.authManager().getMinecraftSession().getUpToDate().getAuthorizationHeader()
                    );
                    log.info("Using Xbox Signaling for incoming connections");
                } catch (Exception e) {
                    log.error("Failed to start Xbox Session", e);
                    log.info("Falling back to Discovery Signaling for incoming connections");
                }
            }

            if (signaling == null) {
                this.discoverySignaling = new NetherNetDiscoverySignaling();
                if (this.resolvedDestinations.isEmpty()) {
                    this.discoverySignaling.setAdvertisementData(
                        new NetherNetServerSignaling.PongData.Builder()
                            .setServerName("NetherNet Server")
                            .setLevelName("World")
                            .setGameType(0)
                            .setPlayerCount(0)
                            .setMaxPlayerCount(10)
                            .build()
                    );
                } else {
                    for (ResolvedDestination destination : this.resolvedDestinations) {
                        this.discoverySignaling.addAdvertisementData(destination.getLocalNetworkId(), destination.getAdvertisementData());
                    }
                }
                signaling = this.discoverySignaling;
            }

            ChannelFuture future = new ServerBootstrap()
                    .group(this.eventLoopGroup)
                    .channelFactory(NetherNetChannelFactory.server(new PeerConnectionFactory(), signaling))
                    .childHandler(new NetherNetBedrockChannelInitializer<ProxyServerSession>() {
                        @Override
                        protected ProxyServerSession createSession0(BedrockPeer peer, int subClientId) {
                            return new ProxyServerSession(peer, subClientId, ProxyPass.this);
                        }

                        @Override
                        protected void initSession(ProxyServerSession session) {
                            session.setPacketHandler(new UpstreamPacketHandler(session, ProxyPass.this, account));
                        }
                    })
                    .bind(this.proxyAddress)
                    .awaitUninterruptibly();

            if (!future.isSuccess()) {
                throw new IOException("Failed to bind NetherNet server to " + this.proxyAddress, future.cause());
            }
            this.server = future.channel();

            if (this.xboxSessionManager != null) {
                try {
                    this.xboxSessionManager.startSession();
                    log.info("Xbox Session Broadcast started successfully.");
                } catch (Exception e) {
                    log.error("Failed to start Xbox Session", e);
                }
            }
            
            log.info("NetherNet server started on {}", proxyAddress);
            this.startLiveDestinationRefresh();

        } else {
            ChannelFuture future = new ServerBootstrap()
                    .group(this.eventLoopGroup)
                    .channelFactory(RakChannelFactory.server(NioDatagramChannel.class))
                    .option(RakChannelOption.RAK_ADVERTISEMENT, ADVERTISEMENT.toByteBuf())
                    .option(RakChannelOption.RAK_IP_DONT_FRAGMENT, true)
                    .childHandler(new BedrockChannelInitializer<ProxyServerSession>() {
                        @Override
                        protected ProxyServerSession createSession0(BedrockPeer peer, int subClientId) {
                            return new ProxyServerSession(peer, subClientId, ProxyPass.this);
                        }

                        @Override
                        protected void initSession(ProxyServerSession session) {
                            session.setPacketHandler(new UpstreamPacketHandler(session, ProxyPass.this, account));
                        }
                    })
                    .bind(this.proxyAddress)
                    .awaitUninterruptibly();
            
            if (!future.isSuccess()) {
                throw new IOException("Failed to bind RakNet server to " + this.proxyAddress, future.cause());
            }
            this.server = future.channel();
            
            this.server.pipeline().remove(RakServerRateLimiter.NAME);
            log.info("Bedrock server started on {}", proxyAddress);
        }

        loop();
    }

    public void newClient(SocketAddress socketAddress, Consumer<ProxyClientSession> sessionConsumer) {
        String transport = this.defaultDestination != null
                ? this.defaultDestination.getDestination().getTransport()
                : "raknet";
        String networkProtocol = this.defaultDestination != null
                ? this.defaultDestination.getServerAddress().getNetworkProtocol()
                : null;
        this.newClient(socketAddress, transport, networkProtocol, sessionConsumer);
    }

    public void newClient(ResolvedDestination destination, Consumer<ProxyClientSession> sessionConsumer) {
        if (destination == null) {
            throw new IllegalArgumentException("Resolved destination is required");
        }
        this.targetAddress = destination.getTargetAddress();
        this.serverAddress = destination.getServerAddress();

        String transport = destination.getDestination() != null ? destination.getDestination().getTransport() : null;
        String networkProtocol = destination.getServerAddress().getNetworkProtocol();
        this.newClient(destination.getTargetAddress(), transport, networkProtocol, sessionConsumer);
    }

    private void newClient(
            SocketAddress socketAddress,
            String configuredTransport,
            String networkProtocol,
            Consumer<ProxyClientSession> sessionConsumer) {
        String transport = hasText(configuredTransport) ? configuredTransport : "raknet";
        boolean isNetherNet = "nethernet".equalsIgnoreCase(transport) || socketAddress instanceof NetherNetAddress;

        ChannelFactory<? extends Channel> channelFactory;

        if (isNetherNet) {
            NetherNetClientSignaling signaling;

            if (socketAddress instanceof NetherNetAddress) {
                String authHeader = this.resolveSignalAuthorization();
                if (!hasText(authHeader)) {
                    throw new IllegalStateException("NetherNet destination requires MCToken signaling auth");
                }
                if ("NETHERNET_JSONRPC".equalsIgnoreCase(networkProtocol)) {
                    signaling = new NetherNetXboxRpcSignaling(
                        authHeader
                    );
                } else {
                    signaling = new NetherNetXboxSignaling(
                        authHeader
                    );
                }
            } else {
                signaling = new NetherNetDiscoverySignaling();
            }

            channelFactory = NetherNetChannelFactory.client(new PeerConnectionFactory(), signaling);
        } else {
            channelFactory = RakChannelFactory.client(NioDatagramChannel.class);
        }

        Bootstrap bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channelFactory(channelFactory);

        if (!isNetherNet) {
            bootstrap
                .option(RakChannelOption.RAK_PROTOCOL_VERSION, ProxyPass.CODEC.getRaknetProtocolVersion())
                .option(RakChannelOption.RAK_COMPATIBILITY_MODE, true)
                .option(RakChannelOption.RAK_IP_DONT_FRAGMENT, true)
                .option(RakChannelOption.RAK_MTU_SIZES, new Integer[]{1492, 1200, 576})
                .option(RakChannelOption.RAK_CLIENT_INTERNAL_ADDRESSES, 20)
                .option(RakChannelOption.RAK_TIME_BETWEEN_SEND_CONNECTION_ATTEMPTS_MS, 500)
                .option(RakChannelOption.RAK_GUID, ThreadLocalRandom.current().nextLong())
                .handler(new BedrockChannelInitializer<ProxyClientSession>() {
                    @Override
                    protected ProxyClientSession createSession0(BedrockPeer peer, int subClientId) {
                        return new ProxyClientSession(peer, subClientId, ProxyPass.this);
                    }

                    @Override
                    protected void initSession(ProxyClientSession session) {
                        sessionConsumer.accept(session);
                    }
                });
        } else {
            bootstrap
                .option(NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 6000)
                .handler(new NetherNetBedrockChannelInitializer<ProxyClientSession>() {
                    @Override
                    protected ProxyClientSession createSession0(BedrockPeer peer, int subClientId) {
                        return new ProxyClientSession(peer, subClientId, ProxyPass.this);
                    }

                    @Override
                    protected void initSession(ProxyClientSession session) {
                        sessionConsumer.accept(session);
                    }
                });
        }

        ChannelFuture future = bootstrap.connect(socketAddress).awaitUninterruptibly();

        if (!future.isSuccess()) {
            log.error("Failed to connect to downstream server {}: {}", socketAddress, future.cause().getMessage());
            throw new RuntimeException("Downstream connection failed", future.cause());
        }

        Channel channel = future.channel();
        this.clients.add(channel);
    }

    public ResolvedDestination resolveDestinationForConnectedViaAddress(String connectedViaAddress) {
        String key = extractLocalNetworkIdKey(connectedViaAddress);
        if (key != null) {
            ResolvedDestination route = this.destinationByLocalNetworkId.get(key);
            if (route != null) {
                return route;
            }
        }
        return this.defaultDestination;
    }

    public boolean isTargetSessionAlive(ResolvedDestination destination) {
        if (destination == null) {
            return false;
        }

        String scid = destination.getTargetSessionScid();
        String templateName = destination.getTargetSessionTemplateName();
        String sessionName = destination.getTargetSessionName();
        if (!hasText(scid) || !hasText(templateName) || !hasText(sessionName)) {
            return true;
        }

        if (!this.onlineMode || account == null || this.authHttpClient == null) {
            return true;
        }

        try {
            SessionGetResult session = this.authHttpClient.executeAndHandle(
                    new SessionGetRequest(
                            account.authManager().getXboxLiveXstsToken().getUpToDate(),
                            scid,
                            templateName,
                            sessionName
                    )
            );
            if (session == null) {
                log.info(
                        "Join gate blocked: session detail empty scid={} template={} session={} nethernetId={} pmsgId={}",
                        scid,
                        templateName,
                        sessionName,
                        destination.getTargetNethernetId(),
                        destination.getTargetPmsgId());
                return false;
            }
            return true;
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
            if (msg.contains("404") || msg.contains("not found") || msg.contains("not_found")) {
                log.info(
                        "Join gate blocked: target session not found scid={} template={} session={} nethernetId={} pmsgId={}",
                        scid,
                        templateName,
                        sessionName,
                        destination.getTargetNethernetId(),
                        destination.getTargetPmsgId());
                return false;
            }
            log.warn(
                    "Join gate XBL lookup error (pass-through): {} scid={} template={} session={}",
                    e.getMessage(),
                    scid,
                    templateName,
                    sessionName);
            return true;
        }
    }

    private String extractLocalNetworkIdKey(String connectedViaAddress) {
        if (!hasText(connectedViaAddress)) {
            return null;
        }
        String value = connectedViaAddress.trim();
        if (this.destinationByLocalNetworkId.containsKey(value)) {
            return value;
        }
        int firstColon = value.indexOf(':');
        if (firstColon > 0) {
            String hostOnly = value.substring(0, firstColon);
            if (this.destinationByLocalNetworkId.containsKey(hostOnly)) {
                return hostOnly;
            }
        }
        try {
            URI parsed = new URI(null, value, null, null).parseServerAuthority();
            String host = parsed.getHost();
            if (hasText(host) && this.destinationByLocalNetworkId.containsKey(host)) {
                return host;
            }
        } catch (URISyntaxException ignored) {
            // Not a host:port authority form.
        }
        return null;
    }

    private void initializeDestinations(Account account, HttpClient client) {
        this.destinationByLocalNetworkId.clear();

        List<Configuration.Destination> configured = new ArrayList<>();
        if (this.configuration.getDestinations() != null) {
            for (Configuration.Destination destination : this.configuration.getDestinations()) {
                if (destination != null) {
                    configured.add(destination);
                }
            }
        }
        if (configured.isEmpty() && this.configuration.getDestination() != null) {
            configured.add(this.configuration.getDestination());
        }
        if (configured.isEmpty()) {
            throw new IllegalStateException("No destination configured. Set destination or destinations in config.yml");
        }

        Set<Long> usedLocalIds = new HashSet<>();
        List<ResolvedDestination> resolved = new ArrayList<>();
        int index = 0;

        for (Configuration.Destination destination : configured) {
            ServerAddress resolvedAddress = new ServerAddress(destination, account, client);
            SocketAddress target = resolvedAddress.getAddress();
            if (target == null) {
                log.warn("Skipping destination[{}]: target address could not be resolved ({})", index, destination);
                index++;
                continue;
            }

            long localNetworkId = this.resolveLocalNetworkId(destination, usedLocalIds);
            String localNetworkIdText = Long.toUnsignedString(localNetworkId);
            NetherNetServerSignaling.PongData advertisement = this.buildAdvertisementData(destination);

            ResolvedDestination route = new ResolvedDestination(
                    destination,
                    resolvedAddress,
                    target,
                    localNetworkId,
                    localNetworkIdText,
                    advertisement,
                    destination.getNethernetId(),
                    destination.getPmsgId(),
                    "",
                    "",
                    "");

            resolved.add(route);
            this.destinationByLocalNetworkId.put(localNetworkIdText, route);
            index++;
        }

        if (resolved.isEmpty()) {
            throw new IllegalStateException("No valid destination resolved from configuration");
        }

        this.configuredDestinations = Collections.unmodifiableList(new ArrayList<>(resolved));
        this.resolvedDestinations = Collections.unmodifiableList(resolved);
        this.defaultDestination = this.resolvedDestinations.get(0);
        this.liveDestinationTemplate = this.defaultDestination.getDestination();
        this.serverAddress = this.defaultDestination.getServerAddress();
        this.targetAddress = this.defaultDestination.getTargetAddress();

        log.info(
                "Resolved destinations={}, defaultLocalNetworkId={}, defaultTarget={}",
                this.resolvedDestinations.size(),
                this.defaultDestination.getLocalNetworkIdText(),
                this.defaultDestination.getTargetAddress());
    }

    private long resolveLocalNetworkId(Configuration.Destination destination, Set<Long> used) {
        if (hasText(destination.getLocalNetworkId())) {
            try {
                long parsed = Long.parseUnsignedLong(destination.getLocalNetworkId().trim());
                if (used.add(parsed)) {
                    return parsed;
                }
                log.warn("Duplicate local-network-id detected ({}). Generating random replacement.", destination.getLocalNetworkId());
            } catch (NumberFormatException e) {
                log.warn("Invalid local-network-id '{}' (must be unsigned long). Generating random replacement.", destination.getLocalNetworkId());
            }
        }

        long generated;
        do {
            generated = ThreadLocalRandom.current().nextLong();
        } while (generated == 0L || !used.add(generated));
        return generated;
    }

    private NetherNetServerSignaling.PongData buildAdvertisementData(Configuration.Destination destination) {
        String serverName = hasText(destination.getAdvertisedServerName()) ? destination.getAdvertisedServerName() : "ProxyPass";
        String levelName = hasText(destination.getAdvertisedLevelName()) ? destination.getAdvertisedLevelName() : "World";
        int playerCount = destination.getAdvertisedPlayerCount() != null ? destination.getAdvertisedPlayerCount() : 0;
        int maxPlayerCount = destination.getAdvertisedMaxPlayerCount() != null ? destination.getAdvertisedMaxPlayerCount() : 10;
        int gameType = destination.getAdvertisedGameType() != null ? destination.getAdvertisedGameType() : 0;
        int transportLayer = destination.getAdvertisedTransportLayer() != null ? destination.getAdvertisedTransportLayer() : 2;
        int connectionType = destination.getAdvertisedConnectionType() != null ? destination.getAdvertisedConnectionType() : 4;

        return new NetherNetServerSignaling.PongData.Builder()
                .setServerName(serverName)
                .setLevelName(levelName)
                .setGameType(gameType)
                .setPlayerCount(playerCount)
                .setMaxPlayerCount(maxPlayerCount)
                .setTransportLayer(transportLayer)
                .setConnectionType(connectionType)
                .build();
    }

    private NetherNetServerSignaling.PongData buildAdvertisementData(Configuration.Destination destination, LiveServerInfo liveServerInfo) {
        String fallbackServerName = hasText(destination.getAdvertisedServerName()) ? destination.getAdvertisedServerName() : "ProxyPass";
        String fallbackLevelName = hasText(destination.getAdvertisedLevelName()) ? destination.getAdvertisedLevelName() : "World";

        String worldName = hasText(liveServerInfo.worldName)
                ? liveServerInfo.worldName
                : (hasText(liveServerInfo.displayTitle) ? liveServerInfo.displayTitle : "");
        String hostName = hasText(liveServerInfo.hostName)
                ? liveServerInfo.hostName
                : (hasText(liveServerInfo.ownerGamertag) ? liveServerInfo.ownerGamertag : "");
        String serverName = hasText(worldName) ? worldName : fallbackServerName;
        String levelName = hasText(hostName) ? hostName : (hasText(worldName) ? worldName : fallbackLevelName);
        int maxPlayerCount = liveServerInfo.maxMemberCount > 0
                ? liveServerInfo.maxMemberCount
                : (destination.getAdvertisedMaxPlayerCount() != null ? destination.getAdvertisedMaxPlayerCount() : 10);
        int gameType = destination.getAdvertisedGameType() != null ? destination.getAdvertisedGameType() : 0;
        int transportLayer = destination.getAdvertisedTransportLayer() != null
                ? destination.getAdvertisedTransportLayer()
                : EggnetCommunitySupport.ADVERTISEMENT_TRANSPORT_LAYER;
        int connectionType = destination.getAdvertisedConnectionType() != null
                ? destination.getAdvertisedConnectionType()
                : EggnetCommunitySupport.ADVERTISEMENT_CONNECTION_TYPE;

        return new NetherNetServerSignaling.PongData.Builder()
                .setServerName(serverName)
                .setLevelName(levelName)
                .setGameType(gameType)
                .setPlayerCount(Math.max(0, liveServerInfo.memberCount))
                .setMaxPlayerCount(Math.max(1, maxPlayerCount))
                .setTransportLayer(transportLayer)
                .setConnectionType(connectionType)
                .build();
    }

    private void startLiveDestinationRefresh() {
        if (this.discoverySignaling == null || this.liveDestinationTemplate == null) {
            return;
        }
        try {
            this.refreshLiveDestinations();
        } catch (Exception e) {
            log.warn("Initial live destination refresh failed: {}", e.getMessage());
        }
        this.liveDestinationRefreshExecutor.scheduleAtFixedRate(() -> {
            try {
                this.refreshLiveDestinations();
            } catch (Exception e) {
                log.warn("Live destination refresh failed: {}", e.getMessage());
            }
        }, LIVE_DEST_REFRESH_SECONDS, LIVE_DEST_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private synchronized void refreshLiveDestinations() {
        if (this.discoverySignaling == null || this.liveDestinationTemplate == null) {
            return;
        }

        CommunityFollowGraph followGraph = this.refreshCommunityFollowGraph();
        Map<String, LiveServerInfo> liveByNether = this.fetchLiveServersByNetherId();
        if (liveByNether.isEmpty()) {
            log.warn("Live destination refresh returned no active list3 sessions. Keeping previous advertisements.");
            return;
        }

        List<LiveServerInfo> liveServers = new ArrayList<>(liveByNether.values());
        if (hasText(followGraph.selfXuid)) {
            List<LiveServerInfo> followingOnly = new ArrayList<>();
            for (LiveServerInfo server : liveServers) {
                if (server == null || !hasText(server.ownerXuid)) {
                    continue;
                }
                if (followGraph.following.contains(server.ownerXuid)) {
                    followingOnly.add(server);
                }
            }
            liveServers = followingOnly;
            liveServers.sort((left, right) -> {
                if (left.memberCount != right.memberCount) {
                    return Integer.compare(right.memberCount, left.memberCount);
                }
                return String.CASE_INSENSITIVE_ORDER.compare(
                        left.ownerGamertag == null ? "" : left.ownerGamertag,
                        right.ownerGamertag == null ? "" : right.ownerGamertag
                );
            });
            if (liveServers.size() > GUEST_LIVE_DEST_MAX_ROUTES) {
                liveServers = new ArrayList<>(liveServers.subList(0, GUEST_LIVE_DEST_MAX_ROUTES));
            }
        } else {
            liveServers = this.prioritizeByDesktopLanguage(liveServers, 5);
            if (liveServers.size() > GUEST_LIVE_DEST_MAX_ROUTES) {
                liveServers = new ArrayList<>(liveServers.subList(0, GUEST_LIVE_DEST_MAX_ROUTES));
            }
        }

        List<ResolvedDestination> nextResolved = new ArrayList<>();
        Map<String, ResolvedDestination> nextByLocalNetworkId = new HashMap<>();
        Map<Long, NetherNetServerSignaling.PongData> nextAdvertisements = new LinkedHashMap<>();

        for (LiveServerInfo live : liveServers) {
            if (nextResolved.size() >= LIVE_DEST_MAX_ROUTES) {
                break;
            }
            String nethernetId = live.nethernetId;
            ServerAddress resolvedAddress = ServerAddress.fromNetherNetJsonRpc(nethernetId, live.pmsgId);
            SocketAddress target = resolvedAddress.getAddress();
            if (target == null) {
                continue;
            }

            long localNetworkId = this.resolveDynamicLocalNetworkId(nethernetId);
            String localNetworkIdText = Long.toUnsignedString(localNetworkId);
            NetherNetServerSignaling.PongData advertisement = this.buildAdvertisementData(this.liveDestinationTemplate, live);
            ResolvedDestination refreshed = new ResolvedDestination(
                    this.liveDestinationTemplate,
                    resolvedAddress,
                    target,
                    localNetworkId,
                    localNetworkIdText,
                    advertisement,
                    live.nethernetId,
                    live.pmsgId,
                    live.sessionScid,
                    live.sessionTemplateName,
                    live.sessionName);

            nextResolved.add(refreshed);
            nextByLocalNetworkId.put(refreshed.getLocalNetworkIdText(), refreshed);
            nextAdvertisements.put(refreshed.getLocalNetworkId(), refreshed.getAdvertisementData());
        }

        if (nextResolved.isEmpty()) {
            log.warn("Live destination refresh produced 0 active routes. Keeping previous advertisements.");
            return;
        }

        this.discoverySignaling.syncAdvertisementData(nextAdvertisements);
        this.destinationByLocalNetworkId.clear();
        this.destinationByLocalNetworkId.putAll(nextByLocalNetworkId);
        this.resolvedDestinations = Collections.unmodifiableList(nextResolved);
        this.defaultDestination = this.resolvedDestinations.get(0);
        this.serverAddress = this.defaultDestination.getServerAddress();
        this.targetAddress = this.defaultDestination.getTargetAddress();

        log.info(
                "Live destination refresh applied: activeRoutes={} communityFollowing={} communityFollowers={}",
                nextResolved.size(),
                followGraph.following.size(),
                followGraph.followers.size()
        );
    }

    private int compareByCommunityThenPopularity(
            LiveServerInfo left,
            LiveServerInfo right,
            CommunityFollowGraph followGraph
    ) {
        int lp = this.communityPriority(left, followGraph);
        int rp = this.communityPriority(right, followGraph);
        if (lp != rp) {
            return Integer.compare(rp, lp);
        }
        return this.compareByPopularity(left, right);
    }

    private int compareByPopularity(LiveServerInfo left, LiveServerInfo right) {
        if (left.memberCount != right.memberCount) {
            return Integer.compare(right.memberCount, left.memberCount);
        }
        return String.CASE_INSENSITIVE_ORDER.compare(
                left.ownerGamertag == null ? "" : left.ownerGamertag,
                right.ownerGamertag == null ? "" : right.ownerGamertag
        );
    }

    private List<LiveServerInfo> prioritizeByDesktopLanguage(List<LiveServerInfo> servers, int preferredQuota) {
        String preferredLanguage = normalizeLanguageCode(Locale.getDefault().getLanguage());
        List<LiveServerInfo> preferred = new ArrayList<>();
        List<LiveServerInfo> others = new ArrayList<>();

        for (LiveServerInfo server : servers) {
            if (hasText(preferredLanguage) && preferredLanguage.equals(server.primaryLanguage)) {
                preferred.add(server);
            } else {
                others.add(server);
            }
        }

        preferred.sort(this::compareByPopularity);
        others.sort(this::compareByPopularity);

        List<LiveServerInfo> ordered = new ArrayList<>(servers.size());
        int takePreferred = Math.min(Math.max(preferredQuota, 0), preferred.size());
        if (takePreferred > 0) {
            ordered.addAll(preferred.subList(0, takePreferred));
        }

        List<LiveServerInfo> remainder = new ArrayList<>(servers.size() - takePreferred);
        if (takePreferred < preferred.size()) {
            remainder.addAll(preferred.subList(takePreferred, preferred.size()));
        }
        remainder.addAll(others);
        remainder.sort(this::compareByPopularity);
        ordered.addAll(remainder);
        return ordered;
    }

    private static String normalizeLanguageCode(String raw) {
        return EggnetCommunitySupport.normalizeLanguageCode(raw);
    }

    private long resolveDynamicLocalNetworkId(String nethernetId) {
        Long existing = this.dynamicLocalIdByNetherId.get(nethernetId);
        if (existing != null) {
            return existing;
        }
        synchronized (this.dynamicLocalIdByNetherId) {
            existing = this.dynamicLocalIdByNetherId.get(nethernetId);
            if (existing != null) {
                return existing;
            }
            long allocated = this.dynamicLocalIdCursor.getAndIncrement();
            if (allocated == 0L) {
                allocated = this.dynamicLocalIdCursor.getAndIncrement();
            }
            this.dynamicLocalIdByNetherId.put(nethernetId, allocated);
            return allocated;
        }
    }

    private Map<String, LiveServerInfo> fetchLiveServersByNetherId() {
        try {
            EggnetCommunitySupport.LiveServerSnapshot snapshot =
                    EggnetCommunitySupport.fetchLiveServerSnapshot(
                            ProxyPass.JSON_MAPPER,
                            this.list2HttpClient,
                            this.liveList3Url,
                            this.liveServerSnapshot
                    );
            this.liveServerSnapshot = snapshot;
            Map<String, EggnetCommunitySupport.LiveServerInfo> fetched = snapshot.serversByNetherId();
            Map<String, LiveServerInfo> byNether = new HashMap<>(fetched.size());
            for (EggnetCommunitySupport.LiveServerInfo live : fetched.values()) {
                byNether.put(
                        live.nethernetId(),
                        new LiveServerInfo(
                                live.nethernetId(),
                                live.pmsgId(),
                                live.sessionScid(),
                                live.sessionTemplateName(),
                                live.sessionName(),
                                live.ownerXuid(),
                                live.ownerGamertag(),
                                live.worldName(),
                                live.hostName(),
                                live.displayTitle(),
                                live.primaryLanguage(),
                                live.memberCount(),
                                live.maxMemberCount(),
                                live.connectionType(),
                                live.transportLayer()
                        )
                );
            }
            return byNether;
        } catch (Exception e) {
            log.warn("Live destination refresh list3 error: {}", e.getMessage());
        }
        return new HashMap<>();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String firstNonBlank(String... candidates) {
        if (candidates == null) return "";
        for (String candidate : candidates) {
            if (candidate == null) continue;
            String trimmed = candidate.trim();
            if (!trimmed.isEmpty()) return trimmed;
        }
        return "";
    }

    private static boolean parseBool(String value) {
        if (!hasText(value)) return false;
        String v = value.trim().toLowerCase(Locale.ROOT);
        return "1".equals(v) || "true".equals(v) || "yes".equals(v) || "on".equals(v);
    }

    private int communityPriority(LiveServerInfo live, CommunityFollowGraph followGraph) {
        if (live == null || followGraph == null || !hasText(live.ownerXuid)) {
            return 0;
        }
        boolean isFollowing = followGraph.following.contains(live.ownerXuid);
        boolean isFollower = followGraph.followers.contains(live.ownerXuid);
        if (isFollowing && isFollower) return 3;
        if (isFollowing) return 2;
        if (isFollower) return 1;
        return 0;
    }

    private CommunityFollowGraph refreshCommunityFollowGraph() {
        CommunityFollowGraph current = this.communityFollowGraph;
        long now = System.currentTimeMillis();
        if (!this.communitySyncEnabled) {
            return current == null ? CommunityFollowGraph.empty() : current;
        }

        CommunityActorIdentity actor = this.resolveCommunityActorIdentity();
        String selfXuid = actor.xuid;
        if (!hasText(selfXuid)) {
            CommunityFollowGraph empty = CommunityFollowGraph.empty();
            this.communityFollowGraph = empty;
            return empty;
        }

        if (current != null &&
                current.fetchedAtMs > 0 &&
                now - current.fetchedAtMs < this.communityRefreshMs &&
                selfXuid.equals(current.selfXuid)) {
            return current;
        }
        if (!hasText(this.communityFollowingUrl) || !hasText(this.communityFollowersUrl)) {
            return current == null ? CommunityFollowGraph.empty() : current;
        }

        this.logCommunityActorIfChanged(actor);

        try {
            Set<String> following = this.fetchCommunityXuids(this.communityFollowingUrl, selfXuid);
            Set<String> followers = this.fetchCommunityXuids(this.communityFollowersUrl, selfXuid);
            CommunityFollowGraph next = new CommunityFollowGraph(selfXuid, following, followers, now);
            this.communityFollowGraph = next;
            return next;
        } catch (Exception e) {
            log.warn("Community graph refresh failed: {}", e.getMessage());
            return current == null ? CommunityFollowGraph.empty() : current;
        }
    }

    private void logCommunityActorIfChanged(CommunityActorIdentity actor) {
        String key = actor.source + ":" + actor.xuid;
        if (!key.equals(this.lastCommunityActorLogKey)) {
            this.lastCommunityActorLogKey = key;
            log.info("Community actor selected: source={} xuid={}", actor.source, actor.xuid);
        }
    }

    private CommunityActorIdentity resolveCommunityActorIdentity() {
        String desktopXuid = this.resolveDesktopCommunityActorXuid();
        if (hasText(desktopXuid)) {
            return new CommunityActorIdentity(desktopXuid, "desktop_app");
        }
        return new CommunityActorIdentity("", "none");
    }

    private String resolveDesktopCommunityActorXuid() {
        long now = System.currentTimeMillis();
        if (hasText(this.desktopCommunityActorXuid) &&
                this.desktopCommunityActorAtMs > 0 &&
                now - this.desktopCommunityActorAtMs < DESKTOP_XUID_CACHE_MS) {
            return this.desktopCommunityActorXuid;
        }
        String xuid = EggnetCommunitySupport.resolveDesktopXuid(ProxyPass.JSON_MAPPER);

        this.desktopCommunityActorXuid = xuid;
        this.desktopCommunityActorAtMs = now;
        return xuid;
    }

    private String resolveRandomCommunityActorXuid() {
        long now = System.currentTimeMillis();
        if (hasText(this.communityRandomActorXuid) &&
                this.communityRandomActorAtMs > 0 &&
                now - this.communityRandomActorAtMs < this.communityRefreshMs) {
            return this.communityRandomActorXuid;
        }

        RandomActorResult actor = this.fetchRandomActorFromTokenPool();
        if (!hasText(actor.xuid)) {
            this.communityRandomActorXuid = "";
            this.communityRandomActorAuthorization = "";
            this.communityRandomActorAtMs = now;
            return "";
        }

        this.communityRandomActorXuid = actor.xuid;
        this.communityRandomActorAuthorization = actor.authorization;
        this.communityRandomActorAtMs = now;
        return actor.xuid;
    }

    private String resolveRandomActorAuthorization() {
        long now = System.currentTimeMillis();
        if (hasText(this.communityRandomActorAuthorization) &&
                hasText(this.communityRandomActorXuid) &&
                this.communityRandomActorAtMs > 0 &&
                now - this.communityRandomActorAtMs < this.communityRefreshMs) {
            return this.communityRandomActorAuthorization;
        }
        RandomActorResult actor = this.fetchRandomActorFromTokenPool();
        if (!hasText(actor.xuid)) {
            return "";
        }
        this.communityRandomActorXuid = actor.xuid;
        this.communityRandomActorAuthorization = actor.authorization;
        this.communityRandomActorAtMs = now;
        return actor.authorization;
    }

    private String resolveSignalAuthorization() {
        String injected = firstNonBlank(
                System.getenv(ENV_SIGNAL_AUTH),
                System.getProperty("proxypass.signalAuth")
        );
        if (hasText(injected)) {
            String trimmed = injected.trim();
            if (trimmed.startsWith("MCToken ") || trimmed.startsWith("XBL3.0 x=")) {
                return trimmed;
            }
            log.warn("Injected signal auth ignored: expected MCToken/XBL3.0 prefix");
        }

        String randomAuth = this.resolveRandomActorAuthorization();
        if (hasText(randomAuth)) {
            String trimmed = randomAuth.trim();
            if (trimmed.startsWith("MCToken ") || trimmed.startsWith("XBL3.0 x=")) {
                return trimmed;
            }
            log.warn("Random actor auth ignored: expected MCToken/XBL3.0 prefix");
        }
        return "";
    }

    private RandomActorResult fetchRandomActorFromTokenPool() {
        if (!hasText(this.communityRandomActorUrl)) {
            return new RandomActorResult("", "");
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(this.communityRandomActorUrl))
                    .timeout(LIST2_REQUEST_TIMEOUT)
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> response = this.list2HttpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                return new RandomActorResult("", "");
            }

            var root = ProxyPass.JSON_MAPPER.readTree(response.body());
            if (!root.path("ok").asBoolean(false)) {
                return new RandomActorResult("", "");
            }
            String xuid = firstNonBlank(
                    root.path("xuid").asText(""),
                    root.path("actor").path("xuid").asText(""),
                    root.path("token").path("userXUID").asText(""),
                    root.path("token").path("xuid").asText("")
            );
            String auth = firstNonBlank(
                    root.path("authorization").asText(""),
                    root.path("authHeader").asText("")
            );
            return new RandomActorResult(xuid, auth);
        } catch (Exception e) {
            log.debug("Random actor fetch failed: {}", e.getMessage());
            return new RandomActorResult("", "");
        }
    }

    private Set<String> fetchCommunityXuids(String endpoint, String selfXuid) throws IOException, InterruptedException {
        return EggnetCommunitySupport.fetchCommunityXuids(
                ProxyPass.JSON_MAPPER,
                this.list2HttpClient,
                endpoint,
                selfXuid
        );
    }

    private void loop() {
        while (running.get()) {
            try {
                synchronized (this) {
                    this.wait();
                }
            } catch (InterruptedException e) {
                // ignore
            }

        }

        this.clients.forEach(Channel::disconnect);
        this.server.disconnect();
        
        this.eventLoopGroup.shutdownGracefully();
        this.liveDestinationRefreshExecutor.shutdownNow();
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            synchronized (this) {
                this.notify();
            }
        }
    }
    
    public void saveCompressedNBT(String dataName, Object dataTag) {
        Path path = dataDir.resolve(dataName + ".nbt");
        try (OutputStream outputStream = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             NBTOutputStream nbtOutputStream = NbtUtils.createGZIPWriter(outputStream)) {
            nbtOutputStream.writeTag(dataTag);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveNBT(String dataName, Object dataTag) {
        Path path = dataDir.resolve(dataName + ".dat");
        try (OutputStream outputStream = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             NBTOutputStream nbtOutputStream = NbtUtils.createNetworkWriter(outputStream)) {
            nbtOutputStream.writeTag(dataTag);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object loadNBT(String dataName) {
        Path path = dataDir.resolve(dataName + ".dat");
        try (InputStream inputStream = Files.newInputStream(path);
            NBTInputStream nbtInputStream = NbtUtils.createNetworkReader(inputStream)) {
            return nbtInputStream.readTag();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object loadGzipNBT(String dataName) {
        Path path = dataDir.resolve(dataName);
        try (InputStream inputStream = Files.newInputStream(path);
            NBTInputStream nbtInputStream = NbtUtils.createGZIPReader(inputStream)) {
            return nbtInputStream.readTag();
        } catch (IOException e) {
            return null;
        }
    }

    public void saveJson(String name, Object object) {
        Path outPath = dataDir.resolve(name);
        try (OutputStream outputStream = Files.newOutputStream(outPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
            ProxyPass.JSON_MAPPER.writer(PRETTY_PRINTER).writeValue(outputStream, object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T loadJson(String name, TypeReference<T> reference) {
        Path path = dataDir.resolve(name);
        try (InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ)) {
            return ProxyPass.JSON_MAPPER.readValue(inputStream, reference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveMojangson(String name, NbtMap nbt) {
        Path outPath = dataDir.resolve(name);
        try {
            Files.writeString(outPath, nbt.toString(), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isIgnoredPacket(Class<?> clazz) {
        return this.configuration.isInvertIgnoredList() != this.ignoredPackets.contains(clazz);
    }

    public boolean isFull() {
        return maxClients > 0 && this.clients.size() >= maxClients;
    }

    private Account getAuthenticatedAccount(boolean saveAuthDetails, HttpClient client) throws Exception {
        Path authPath = Paths.get(".").resolve("auth.json");
        BedrockAuthManager.Builder authManagerBuilder = BedrockAuthManager.create(client, CODEC.getMinecraftVersion());
        Account account;
        final boolean disableAuthFile =
                parseBool(System.getenv(ENV_DISABLE_AUTH_FILE)) ||
                parseBool(System.getProperty("proxypass.disableAuthFile"));

        final String msaClientId = firstNonBlank(
                System.getenv(ENV_MSA_CLIENT_ID),
                System.getProperty("proxypass.msaClientId"),
                DEFAULT_MSA_CLIENT_ID
        );
        final String msaScope = firstNonBlank(
                System.getenv(ENV_MSA_SCOPE),
                System.getProperty("proxypass.msaScope"),
                DEFAULT_MSA_SCOPE
        );
        final String msaDeviceType = firstNonBlank(
                System.getenv(ENV_MSA_DEVICE_TYPE),
                System.getProperty("proxypass.msaDeviceType"),
                DEFAULT_MSA_DEVICE_TYPE
        );
        authManagerBuilder.msaApplicationConfig(new MsaApplicationConfig(msaClientId, msaScope));
        authManagerBuilder.deviceType(msaDeviceType);

        String refreshToken = System.getenv(ENV_REFRESH_TOKEN);
        if (!hasText(refreshToken)) {
            refreshToken = System.getProperty("proxypass.refreshToken");
        }
        if (hasText(refreshToken)) {
            BedrockAuthManager authManager = authManagerBuilder.login(refreshToken.trim());
            account = new Account(authManager);
            account.refresh();
            log.info("Authenticated using in-memory refresh token (no auth.json read/write)");
            return account;
        }

        if (disableAuthFile) {
            throw new IllegalStateException("auth.json path is disabled; PROXYPASS_REFRESH_TOKEN is required");
        }

        if (Files.exists(authPath) && Files.isRegularFile(authPath) && saveAuthDetails) {
            String accountString = new String(Files.readAllBytes(authPath), StandardCharsets.UTF_8);
            JsonObject accountJson = JsonParser.parseString(accountString).getAsJsonObject();
            if (accountJson.has("mcChain")) {
                accountJson = MinecraftAuth4To5Migrator.migrateBedrockSave(accountJson);
            }
            account = new Account(accountJson, client, CODEC.getMinecraftVersion());
            account.refresh();
            Files.write(authPath, account.toJson().toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            return account;
        }

        BedrockAuthManager authManager = authManagerBuilder.login(DeviceCodeMsaAuthService::new, new Consumer<MsaDeviceCode>() {
            @Override
            public void accept(MsaDeviceCode msaDeviceCode) {
                log.info("Go to " + msaDeviceCode.getVerificationUri());
                log.info("Enter code " + msaDeviceCode.getUserCode());

                if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                    try {
                        Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
                        clipboard.setContents(new StringSelection(msaDeviceCode.getUserCode()), null);
                        log.info("Copied code to clipboard");
                        Desktop.getDesktop().browse(new URI(msaDeviceCode.getVerificationUri()));
                    } catch (IOException | URISyntaxException e) {
                        log.error("Failed to open browser", e);
                    }
                }
            }
        });
        account = new Account(authManager);
        account.refresh();

        if (saveAuthDetails) {
            Files.write(authPath, account.toJson().toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        return account;
    }

}
