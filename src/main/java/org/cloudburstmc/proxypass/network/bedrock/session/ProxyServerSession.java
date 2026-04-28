package org.cloudburstmc.proxypass.network.bedrock.session;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import dev.kastle.netty.channel.nethernet.NetherNetServerChannel;
import org.cloudburstmc.protocol.bedrock.BedrockPeer;
import org.cloudburstmc.protocol.bedrock.BedrockServerSession;
import org.cloudburstmc.protocol.bedrock.BedrockSession;
import org.cloudburstmc.protocol.bedrock.netty.BedrockPacketWrapper;
import org.cloudburstmc.protocol.bedrock.packet.BedrockPacket;
import org.cloudburstmc.protocol.bedrock.packet.UnknownPacket;
import org.cloudburstmc.protocol.common.PacketSignal;
import org.cloudburstmc.proxypass.ProxyPass;
import org.cloudburstmc.proxypass.network.bedrock.util.TestUtils;

@Getter
@Log4j2
public class ProxyServerSession extends BedrockServerSession implements ProxySession {

    private final ProxyPass proxyPass;
    @Setter
    private BedrockSession sendSession;
    @Setter
    private ProxyPlayerSession player;
    @Setter
    private String connectedViaAddress;

    public ProxyServerSession(BedrockPeer peer, int subClientId, ProxyPass proxyPass) {
        super(peer, subClientId);
        this.proxyPass = proxyPass;
        String routeLocalNetworkId = peer.getChannel()
                .attr(NetherNetServerChannel.ROUTE_LOCAL_NETWORK_ID_ATTR)
                .get();
        if (routeLocalNetworkId != null && !routeLocalNetworkId.isBlank()) {
            this.connectedViaAddress = routeLocalNetworkId;
        }
    }

    @Override
    protected void onPacket(BedrockPacketWrapper wrapper) {
        BedrockPacket packet = wrapper.getPacket();
        if (player != null) {
            player.logger.logPacket(this, wrapper, true);
        }

        if (proxyPass.getConfiguration().isPacketTesting()) {
            TestUtils.testPacket(this, wrapper);
        }

        if (this.packetHandler == null) {
            log.warn("Received packet without a packet handler for {}:{}: {}", new Object[]{this.getSocketAddress(), this.subClientId, packet});
        } else if (this.packetHandler.handlePacket(packet) == PacketSignal.UNHANDLED && this.sendSession != null) {
            // this.sendSession.sendPacket(ReferenceCountUtil.retain(packet));

            ByteBuf buffer = wrapper.getPacketBuffer()
                    .retainedSlice()
                    .skipBytes(wrapper.getHeaderLength());

            UnknownPacket sendPacket = new UnknownPacket();
            sendPacket.setPayload(buffer);
            sendPacket.setPacketId(wrapper.getPacketId());
            this.sendSession.sendPacket(sendPacket);
        }
    }
}
