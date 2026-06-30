package org.cloudburstmc.proxypass.network.bedrock.session;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import net.raphimc.minecraftauth.bedrock.model.MinecraftMultiplayerToken;
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec;
import org.cloudburstmc.protocol.bedrock.data.PacketCompressionAlgorithm;
import org.cloudburstmc.protocol.bedrock.data.auth.*;
import org.cloudburstmc.protocol.bedrock.packet.*;
import org.cloudburstmc.protocol.bedrock.util.ChainValidationResult;
import org.cloudburstmc.protocol.bedrock.util.ChainValidationResult.IdentityData;
import org.cloudburstmc.protocol.bedrock.util.EncryptionUtils;
import org.cloudburstmc.protocol.common.PacketSignal;
import org.cloudburstmc.proxypass.ProxyPass;
import org.cloudburstmc.proxypass.network.bedrock.util.ForgeryUtils;
import org.jose4j.json.JsonUtil;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.net.SocketAddress;
import java.util.List;
import java.util.UUID;

@Log4j2
@RequiredArgsConstructor
public class UpstreamPacketHandler implements BedrockPacketHandler {
    private final ProxyServerSession session;
    private final ProxyPass proxy;
    private final Account account;
    private JSONObject skinData;
    private AuthData authData;
    private ProxyPlayerSession player;
    private String incomingDisplayName = "";
    private String incomingXuid = "";
    private boolean preferIncomingIdentityForLogin = false;

    private static ECPublicKey mojangPublicKey;
    private static AuthPayload authPayload;

    private static boolean verifyJwt(String jwt, PublicKey key) throws JoseException {
        JsonWebSignature jws = new JsonWebSignature();
        jws.setKey(key);
        jws.setCompactSerialization(jwt);
        return jws.verifySignature();
    }

    @Override
    public PacketSignal handle(RequestNetworkSettingsPacket packet) {
        int protocolVersion = packet.getProtocolVersion();

        if (protocolVersion != ProxyPass.PROTOCOL_VERSION) {
            PlayStatusPacket status = new PlayStatusPacket();
            status.setStatus(protocolVersion > ProxyPass.PROTOCOL_VERSION 
                ? PlayStatusPacket.Status.LOGIN_FAILED_SERVER_OLD 
                : PlayStatusPacket.Status.LOGIN_FAILED_CLIENT_OLD);

            session.sendPacketImmediately(status);
            return PacketSignal.HANDLED;
        }
        session.setCodec(ProxyPass.CODEC);

        NetworkSettingsPacket networkSettingsPacket = new NetworkSettingsPacket();
        networkSettingsPacket.setCompressionThreshold(0);
        networkSettingsPacket.setCompressionAlgorithm(PacketCompressionAlgorithm.ZLIB);

        session.sendPacketImmediately(networkSettingsPacket);
        session.setCompression(PacketCompressionAlgorithm.ZLIB);
        return PacketSignal.HANDLED;
    }

    @Override
    public PacketSignal handle(LoginPacket packet) {
        try {
            ChainValidationResult chain = EncryptionUtils.validatePayload(packet.getAuthPayload());
            ECPublicKey identityPublicKey;

            switch (packet.getAuthPayload()) {
                case DualPayload _ -> {
                    identityPublicKey = EncryptionUtils.parseKey(chain.identityClaims().identityPublicKey);
                }
                case TokenPayload _ -> {
                    identityPublicKey = EncryptionUtils.parseKey(chain.identityClaims().identityPublicKey);
                }
                case CertificateChainPayload _ -> {
                    JsonNode payload = ProxyPass.JSON_MAPPER.valueToTree(chain.rawIdentityClaims());
                    identityPublicKey = EncryptionUtils.parseKey(payload.get("identityPublicKey").textValue());
                }
                default -> throw new IllegalStateException("Unexpected value: " + packet.getAuthPayload());
            }

            String clientJwt = packet.getClientJwt();
            verifyJwt(clientJwt, identityPublicKey);
            JsonWebSignature jws = new JsonWebSignature();
            jws.setCompactSerialization(clientJwt);

            skinData = new JSONObject(JsonUtil.parseJson(jws.getUnverifiedPayload()));

            if (skinData.get("ServerAddress") != null) {
                String loginServerAddress = skinData.get("ServerAddress").toString();
                String existingRoute = session.getConnectedViaAddress();
                if (existingRoute == null || existingRoute.isBlank()) {
                    session.setConnectedViaAddress(loginServerAddress);
                } else {
                    log.trace("Preserving pre-resolved route localNetworkId={} (login ServerAddress={})",
                            existingRoute, loginServerAddress);
                }
            }

            IdentityData identityData = chain.identityClaims().extraData;
            this.incomingDisplayName = (identityData != null && identityData.displayName != null) ? identityData.displayName : "";
            this.incomingXuid = (identityData != null && identityData.xuid != null) ? identityData.xuid : "";
            boolean hasIncomingName = !this.incomingDisplayName.isBlank();
            boolean hasIncomingXuid = !this.incomingXuid.isBlank();

            if (account == null) {
                this.authData = new AuthData(
                        this.incomingDisplayName,
                        deriveIdentityUuid(this.incomingXuid, this.incomingDisplayName, null),
                        this.incomingXuid
                );
                this.preferIncomingIdentityForLogin = true;
            } else {
                MinecraftMultiplayerToken token = account.authManager().getMinecraftMultiplayerToken().getUpToDate();
                String effectiveDisplayName = hasIncomingName ? this.incomingDisplayName : token.getDisplayName();
                String effectiveXuid = hasIncomingXuid ? this.incomingXuid : token.getXuid();
                this.authData = new AuthData(
                        effectiveDisplayName,
                        deriveIdentityUuid(effectiveXuid, effectiveDisplayName, token.getUuid()),
                        effectiveXuid
                );
                // Incoming identity exists: prefer spoofing that identity (self-signed chain) over token-owner identity.
                this.preferIncomingIdentityForLogin = hasIncomingName || hasIncomingXuid;
            }

            log.info("Upstream login received: incomingName='{}' incomingXuid={} proxyName='{}' proxyXuid={} preferIncomingIdentity={} via={} remote={}",
                    this.incomingDisplayName,
                    this.incomingXuid,
                    this.authData.getDisplayName(),
                    this.authData.getXuid(),
                    this.preferIncomingIdentityForLogin,
                    session.getConnectedViaAddress(),
                    session.getSocketAddress());
            
            initializeProxySession();

        } catch (Exception e) {
            session.disconnect("disconnectionScreen.internalError.cantConnect");
            throw new RuntimeException("Unable to complete login", e);
        }
        return PacketSignal.HANDLED;
    }

    @Override
    public PacketSignal handle(ResourcePackClientResponsePacket packet) {
        if (!this.proxy.getConfiguration().isDownloadPacks()) {
            return PacketSignal.UNHANDLED;
        }
        if (packet.getStatus() != ResourcePackClientResponsePacket.Status.COMPLETED) {
            return PacketSignal.UNHANDLED;
        }

        player.getPackDownloader().processPacks();
        return PacketSignal.UNHANDLED;
    }

    private void initializeProxySession() {
        log.debug("Initializing proxy session");
        ProxyPass.ResolvedDestination destination = this.proxy.resolveDestinationForConnectedViaAddress(this.session.getConnectedViaAddress());
        if (destination == null) {
            this.session.disconnect("disconnectionScreen.internalError.cantConnect");
            return;
        }
        SocketAddress targetAddress = destination.getTargetAddress();
        log.info("Resolved downstream target via='{}' localNetworkId={} target={}",
                this.session.getConnectedViaAddress(),
                destination.getLocalNetworkIdText(),
                targetAddress);
        if (!this.proxy.isTargetSessionAlive(destination)) {
            log.info("Blocking join on first signal: target session is not active localNetworkId={} target={}",
                    destination.getLocalNetworkIdText(),
                    targetAddress);
            this.session.disconnect("이미 닫힌 서버입니다.");
            return;
        }

        this.proxy.newClient(destination, this.incomingXuid, downstream -> {
            BedrockCodec.Builder codecBuilder = ProxyPass.CODEC.toBuilder();
            downstream.setCodec(codecBuilder.build());

            downstream.setSendSession(this.session);
            this.session.setSendSession(downstream);

            KeyPair sessionKeyPair = (account != null) 
                ? account.authManager().getSessionKeyPair() 
                : EncryptionUtils.createKeyPair();

            ProxyPlayerSession proxySession = new ProxyPlayerSession(
                this.session,
                downstream,
                this.proxy,
                this.authData,
                sessionKeyPair
            );
            this.player = proxySession;

            downstream.setPlayer(proxySession);
            this.session.setPlayer(proxySession);

            LoginPacket login = prepareLoginPacket(proxySession, targetAddress);

            downstream.setPacketHandler(new DownstreamInitialPacketHandler(downstream, proxySession, this.proxy, login));
            downstream.setLogging(true);

            RequestNetworkSettingsPacket packet = new RequestNetworkSettingsPacket();
            packet.setProtocolVersion(ProxyPass.PROTOCOL_VERSION);
            downstream.sendPacketImmediately(packet);
            this.player.getLogger().logPacket(this.session, packet, true);
        });
    }

    private LoginPacket prepareLoginPacket(ProxyPlayerSession proxySession, SocketAddress targetAddress) {
        String jwtSkinData;
        AuthPayload payload;
        boolean useSelfSignedIdentity = (account == null) || this.preferIncomingIdentityForLogin;

        if (useSelfSignedIdentity) {
            try {
                player.getLogger().saveJson("skinData", this.skinData);
            } catch (Exception e) {
                log.error("JSON output error: " + e.getMessage(), e);
            }

            String forgedAuth = ForgeryUtils.forgeOfflineAuthData(proxySession.getProxyKeyPair(), this.authData);
            jwtSkinData = ForgeryUtils.forgeOfflineSkinData(proxySession.getProxyKeyPair(), this.skinData);
            payload = new CertificateChainPayload(List.of(forgedAuth), AuthType.SELF_SIGNED);

        } else {
            try {
                if (mojangPublicKey == null) {
                    mojangPublicKey = ForgeryUtils.forgeMojangPublicKey();
                }
                if (authPayload == null) {
                    authPayload = ForgeryUtils.forgeOnlineAuthData(account.authManager(), mojangPublicKey);
                }
            } catch (Exception e) {
                log.error("Failed to get login chain", e);
            }

            jwtSkinData = ForgeryUtils.forgeOnlineSkinData(account, this.skinData, targetAddress);

            try {
                player.getLogger().saveJson("skinData", this.skinData);
            } catch (Exception e) {
                log.error("JSON output error: " + e.getMessage(), e);
            }
            
            payload = authPayload;
        }

        LoginPacket login = new LoginPacket();
        login.setClientJwt(jwtSkinData);
        login.setAuthPayload(payload);
        login.setProtocolVersion(ProxyPass.PROTOCOL_VERSION);
        return login;
    }

    private static UUID deriveIdentityUuid(String xuid, String displayName, UUID fallback) {
        if (xuid != null && !xuid.isBlank()) {
            return UUID.nameUUIDFromBytes(xuid.getBytes(StandardCharsets.UTF_8));
        }
        if (displayName != null && !displayName.isBlank()) {
            return UUID.nameUUIDFromBytes(displayName.getBytes(StandardCharsets.UTF_8));
        }
        return fallback != null ? fallback : UUID.randomUUID();
    }

    @Override
    public void onDisconnect(CharSequence reason) {
        var sendSession = this.session.getSendSession();
        if (sendSession != null && sendSession.isConnected()) {
            sendSession.disconnect(reason);
        }
    }
}
