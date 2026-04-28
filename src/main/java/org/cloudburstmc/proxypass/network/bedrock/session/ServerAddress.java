package org.cloudburstmc.proxypass.network.bedrock.session;

import dev.kastle.netty.channel.nethernet.config.NetherNetAddress;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.lenni0451.commons.httpclient.HttpClient;
import net.raphimc.minecraftauth.bedrock.model.MinecraftSession;
import net.raphimc.minecraftauth.extra.realms.model.RealmsJoinInformation;
import net.raphimc.minecraftauth.extra.realms.model.RealmsServer;
import net.raphimc.minecraftauth.extra.realms.service.impl.BedrockRealmsService;
import org.cloudburstmc.proxypass.ProxyPass;
import org.cloudburstmc.proxypass.Configuration.Destination;
import org.cloudburstmc.proxypass.network.bedrock.request.ExperienceRequest;
import org.cloudburstmc.proxypass.network.bedrock.request.FeaturedServersRequest;
import org.cloudburstmc.proxypass.network.bedrock.request.SessionGetRequest;
import org.cloudburstmc.proxypass.network.bedrock.request.SessionHandleQueryRequest;
import org.cloudburstmc.proxypass.network.bedrock.request.model.Experience;
import org.cloudburstmc.proxypass.network.bedrock.request.model.FeaturedServers;
import org.cloudburstmc.proxypass.network.bedrock.request.model.featuredservers.FeaturedServer;
import org.cloudburstmc.proxypass.network.bedrock.request.model.sessiondirectory.SessionGetResult;
import org.cloudburstmc.proxypass.network.bedrock.request.model.sessiondirectory.SessionHandleQueryData;
import org.cloudburstmc.proxypass.network.bedrock.request.model.sessiondirectory.SessionHandleQueryResult;
import org.cloudburstmc.proxypass.xbox.XboxSessionManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Log4j2
public class ServerAddress {
    private static final String LIST3_URL = "https://eggnet.space/api/servers/list3";

    @Getter
    private SocketAddress address;
    @Getter
    private String networkProtocol;
    private String host;
    private int port;

    private ServerAddress() {
    }

    public ServerAddress(Destination destination, Account account, HttpClient client) {
        this.host = destination.getHost();
        this.port = destination.getPort();
        String experienceId = destination.getExperienceId();
        String featuredServerTitle = destination.getFeaturedServerTitle();
        String realmName = destination.getRealmName();
        String friendName = destination.getFriendName();
        String nethernetId = destination.getNethernetId();
        String pmsgId = destination.getPmsgId();

        log.info("Resolving server address for destination: host={}, port={}, experienceId={}, featuredServerTitle={}, realmName={}",
                this.host, this.port, experienceId, featuredServerTitle, realmName);

        if (!hasText(pmsgId) && hasText(nethernetId)) {
            pmsgId = resolvePmsgIdFromList3(nethernetId.trim());
            if (hasText(pmsgId)) {
                log.info("Resolved pmsgId from list3 for NetherNet ID {} -> {}", nethernetId, pmsgId);
            }
        }

        if (hasText(pmsgId)) {
            this.address = new NetherNetAddress(pmsgId.trim());
            this.networkProtocol = "NETHERNET_JSONRPC";
            log.info("Using NetherNet JSON-RPC target with pmsg ID {} (source NetherNet ID: {})", pmsgId, nethernetId);
            return;
        }

        if (hasText(nethernetId)) {
            this.address = new NetherNetAddress(nethernetId.trim());
            log.info("Using NetherNet address with ID {}", nethernetId.trim());
            return;
        }

        if (friendName != null && client != null) {
            try {
                SessionHandleQueryResult result = client.executeAndHandle(
                    new SessionHandleQueryRequest(
                        account.authManager().getXboxLiveXstsToken().getUpToDate(),
                        SessionHandleQueryData.builder()
                            .scid(XboxSessionManager.SCID)
                            .monikerXuid(account.authManager().getMinecraftMultiplayerToken().getUpToDate().getXuid())
                            .build()
                    )
                );

                for (SessionHandleQueryResult.HandleResult handle : result.getResults()) {
                    if (handle.getSessionRef() == null) continue;
                    if (handle.getSessionRef().getScid() == null) continue;
                    if (handle.getSessionRef().getTemplateName() == null) continue;
                    if (handle.getSessionRef().getName() == null) continue;
                    if (handle.getCustomProperties() == null) continue;
                    if (handle.getCustomProperties().getSupportedConnections() == null) continue;
                    if (handle.getCustomProperties().getSupportedConnections().isEmpty()) continue;

                    SessionGetResult session = client.executeAndHandle(
                        new SessionGetRequest(
                            account.authManager().getXboxLiveXstsToken().getUpToDate(),
                            handle.getSessionRef().getScid(),
                            handle.getSessionRef().getTemplateName(),
                            handle.getSessionRef().getName()
                        )
                    );

                    for (SessionGetResult.SessionMember member : session.getMembers().values()) {
                        if (!friendName.equalsIgnoreCase(member.getGamertag())) continue;

                        String netherNetId = handle.getCustomProperties().getSupportedConnections().get(0).getNetherNetId();
                        if (netherNetId == null || netherNetId.isEmpty()) {
                            log.warn("Friend '{}' does not have a valid NetherNet ID in their session", friendName);
                            continue;
                        } else {
                            this.address = new NetherNetAddress(netherNetId);
                            log.info("Resolved friend's ('{}') session handle to NetherNet ID {}", friendName, netherNetId);
                            return;
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed to resolve friend's session handle", e);
            }
        }

        if (realmName != null && client != null) {
            try {
                BedrockRealmsService realmsService = new BedrockRealmsService(client, ProxyPass.CODEC.getMinecraftVersion(), account.authManager().getRealmsXstsToken());
                
                RealmsServer targetRealm = null;

                for (RealmsServer realm : realmsService.getWorlds()) {
                    if (realm.getName().equalsIgnoreCase(realmName)) {
                        targetRealm = realm;
                        log.info("Found Realm ID {} for name '{}'", realm.getId(), realmName);
                        break;
                    }
                }

                if (targetRealm != null) {
                    RealmsJoinInformation joinInfo = realmsService.joinWorld(targetRealm);
                    String fullAddress = joinInfo.getAddress();
                    String protocol = joinInfo.getNetworkProtocol();

                    log.info("Join information for Realm '{}': {}", realmName, joinInfo);

                    if (fullAddress == null || fullAddress.isEmpty()) {
                        throw new IllegalArgumentException("Realms API provided no server address!");
                    }

                    this.networkProtocol = protocol;

                    if (protocol.equalsIgnoreCase("NETHERNET")) {
                        this.address = new NetherNetAddress(fullAddress);
                        return;
                    }

                    if (protocol.equalsIgnoreCase("NETHERNET_JSONRPC")) {
                        this.address = new NetherNetAddress(fullAddress);
                        return;
                    } 

                    throw new IllegalArgumentException("Unsupported Realms network protocol: " + protocol);
                } else {
                    log.warn("Could not find Realm with name: {}", realmName);
                }
            } catch (IOException e) {
                log.error("Failed to resolve Realm address", e);
            }
        }

        if (featuredServerTitle != null && client != null) {
            try {
                MinecraftSession session = account.authManager().getMinecraftSession().getUpToDate();
                FeaturedServers servers = client.executeAndHandle(new FeaturedServersRequest(session));
                for (FeaturedServer server : servers.getData().getItems()) {
                    if (server.getTitle().getNeutral().equals(featuredServerTitle)) {
                        if (server.getDisplayProperties().getExperienceId() != null) {
                            experienceId = server.getDisplayProperties().getExperienceId();
                            log.info("Resolved featured server '{}' to properties {}", featuredServerTitle,
                                    experienceId);
                            break;
                        }
                        if (server.getDisplayProperties().getUrl() != null) {
                            this.host = server.getDisplayProperties().getUrl();
                            this.port = server.getDisplayProperties().getPort();
                            log.info("Resolved featured server '{}' to {}:{}", featuredServerTitle, this.host,
                                    this.port);
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed to fetch featured servers", e);
            }
        }

        if (experienceId != null && client != null) {
            try {
                MinecraftSession session = account.authManager().getMinecraftSession().getUpToDate();
                Experience experience = client.executeAndHandle(new ExperienceRequest(session, experienceId));
                if (!experience.getResult().getNetworkProtocol().equals("Default")) {
                    throw new IllegalArgumentException("Unsupported network protocol: " + experience.getResult().getNetworkProtocol());
                }
                this.host = experience.getResult().getIpV4Address();
                this.port = experience.getResult().getPort();
                this.networkProtocol = experience.getResult().getNetworkProtocol();
                log.info("Resolved experience ID {} to {}:{}", experienceId, this.host, this.port);
            } catch (IOException e) {
                log.error("Failed to fetch experience", e);
            }
        }

        try {
            this.address = new InetSocketAddress(this.host, this.port);
        } catch (IllegalArgumentException e) {
            log.error(
                    "Could not find a server address for the provided destination: host={}, port={}, experienceId={}, featuredServerTitle={} ",
                    this.host, this.port, experienceId, featuredServerTitle, e);
        }
    }

    public static ServerAddress fromNetherNetJsonRpc(String nethernetId, String pmsgId) {
        ServerAddress serverAddress = new ServerAddress();
        String cleanPmsgId = pmsgId == null ? "" : pmsgId.trim();
        String cleanNethernetId = nethernetId == null ? "" : nethernetId.trim();
        serverAddress.address = new NetherNetAddress(cleanPmsgId);
        serverAddress.networkProtocol = "NETHERNET_JSONRPC";
        serverAddress.host = cleanNethernetId;
        serverAddress.port = 0;
        return serverAddress;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String resolvePmsgIdFromList3(String targetNethernetId) {
        try {
            java.net.http.HttpClient http = java.net.http.HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(8))
                .build();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(LIST3_URL))
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json")
                .GET()
                .build();

            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                log.warn("Failed to resolve pmsgId from list3: status={}", response.statusCode());
                return null;
            }

            var root = ProxyPass.JSON_MAPPER.readTree(response.body());
            var servers = root.path("servers");
            if (!servers.isArray()) {
                return null;
            }

            for (var serverNode : servers) {
                var noteNode = serverNode.get("note");
                if (noteNode == null || noteNode.isNull()) continue;

                var noteObj = noteNode;
                if (noteNode.isTextual()) {
                    try {
                        noteObj = ProxyPass.JSON_MAPPER.readTree(noteNode.asText());
                    } catch (Exception ignored) {
                        continue;
                    }
                }

                var handle = noteObj.path("world").path("handle");
                if (handle.isMissingNode()) continue;

                String nether = handle.path("netherNetId").asText("");
                if (!targetNethernetId.equals(nether)) continue;

                String pmsg = handle.path("pmsgId").asText("");
                if (hasText(pmsg)) {
                    return pmsg.trim();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to resolve pmsgId from list3 for {}: {}", targetNethernetId, e.getMessage());
        }
        return null;
    }
}
