package dev.kastle.netty.channel.nethernet.signaling;

import dev.kastle.netty.channel.nethernet.NetherNetConstants;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetServerSignaling.PongData;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetSignaling.SignalHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class NetherNetDiscovery extends SimpleChannelInboundHandler<DatagramPacket> {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetDiscovery.class);
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final long networkId;
    private final Map<Long, SignalHandler> signalHandlers = new ConcurrentHashMap<>();
    private volatile NetherNetClientSignaling.ClientSignalFeed clientSignalFeed;
    private final Map<Long, InetSocketAddress> peerAddresses = new ConcurrentHashMap<>();
    private final Map<Long, byte[]> pongDataByNetworkId = new ConcurrentHashMap<>();
    private final Map<Long, Long> connectionRecipientByConnectionId = new ConcurrentHashMap<>();
    private Channel channel;
    private EventLoopGroup group;
    private NetherNetServerSignaling.NewConnectionHandler newConnectionHandler;
    private BiConsumer<Long, ByteBuf> discoveryCallback;
    private BiConsumer<Long, InetSocketAddress> discoveryRequestCallback;

    /**
     * Creates a NetherNetDiscovery instance with the specified Network ID.
     * 
     * @param networkId The Network ID to use for discovery.
     */
    public NetherNetDiscovery(long networkId) {
        this.networkId = networkId;
    }

    public void bind() {
        bind(NetherNetConstants.DISCOVERY_PORT);
    }

    public void bind(int port) {
        close();
        EventLoopGroup nextGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(nextGroup)
             .channel(NioDatagramChannel.class)
             .option(ChannelOption.SO_BROADCAST, true)
             .handler(this);

            this.channel = bootstrap.bind(port).sync().channel();
            this.group = nextGroup;
            log.info("NetherNet Discovery listening on port {}", port);
        } catch (InterruptedException e) {
            nextGroup.shutdownGracefully().syncUninterruptibly();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to bind NetherNet discovery", e);
        } catch (RuntimeException e) {
            nextGroup.shutdownGracefully().syncUninterruptibly();
            throw e;
        }
    }

    public void bind(InetSocketAddress address) {
        close();
        EventLoopGroup nextGroup = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(nextGroup)
             .channel(NioDatagramChannel.class)
             .option(ChannelOption.SO_BROADCAST, true)
             .handler(this);

            this.channel = bootstrap.bind(address).sync().channel();
            this.group = nextGroup;
            log.info("NetherNet Discovery listening on {}", address);
        } catch (InterruptedException e) {
            nextGroup.shutdownGracefully().syncUninterruptibly();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to bind NetherNet discovery", e);
        } catch (RuntimeException e) {
            nextGroup.shutdownGracefully().syncUninterruptibly();
            throw e;
        }
    }

    public void sendDiscoveryRequest(InetSocketAddress target, BiConsumer<Long, ByteBuf> onServerFound) {
        this.discoveryCallback = onServerFound;
        
        ByteBuf buf = Unpooled.buffer();
        buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_REQUEST);
        buf.writeLongLE(this.networkId);
        buf.writeZero(8); // Padding
        
        sendPacket(buf, target);
    }

    public void setPongData(PongData data) {
        setPongData(this.networkId, data);
    }

    public void setPongData(long localNetworkId, PongData data) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(5); // Minecraft 1.21.9x LAN ServerData version
        writeString(buf, data.serverName());
        writeString(buf, data.levelName());
        buf.writeByte(data.gameType() << 1);
        buf.writeIntLE(data.playerCount());
        buf.writeIntLE(data.maxPlayerCount());
        buf.writeBoolean(data.isEditorWorld());
        buf.writeBoolean(data.isHardcore());
        // Minecraft 1.21.9x adds two boolean flags before transport/connection.
        // Captured vanilla LAN worlds currently send both as true.
        buf.writeBoolean(true);
        buf.writeBoolean(true);
        buf.writeByte(data.transportLayer() << 1);
        buf.writeByte(data.connectionType() << 1);
        byte[] binaryData = new byte[buf.readableBytes()];
        buf.readBytes(binaryData);
        buf.release();

        String hex = encodeHex(binaryData);
        byte[] hexBytes = hex.getBytes(StandardCharsets.UTF_8);

        ByteBuf response = Unpooled.buffer();
        response.writeIntLE(hexBytes.length);
        response.writeBytes(hexBytes);

        byte[] encoded = new byte[response.readableBytes()];
        response.readBytes(encoded);
        this.pongDataByNetworkId.put(localNetworkId, encoded);
        response.release();
    }

    public void removePongData(long localNetworkId) {
        this.pongDataByNetworkId.remove(localNetworkId);
    }

    public Set<Long> getAdvertisedNetworkIds() {
        return new HashSet<>(this.pongDataByNetworkId.keySet());
    }

    public void registerSignalHandler(long connectionId, SignalHandler handler) {
        this.signalHandlers.put(connectionId, handler);
    }
    
    public void unregisterSignalHandler(long connectionId) {
        this.signalHandlers.remove(connectionId);
    }

    public void setClientSignalFeed(NetherNetClientSignaling.ClientSignalFeed feed) {
        this.clientSignalFeed = feed;
    }

    public void clearClientSignalFeed() {
        this.clientSignalFeed = null;
    }

    public Long getConnectionRecipient(long connectionId) {
        return this.connectionRecipientByConnectionId.get(connectionId);
    }

    public void setNewConnectionHandler(NetherNetServerSignaling.NewConnectionHandler handler) {
        this.newConnectionHandler = handler;
    }

    /**
     * Sends a signal immediately and schedules it to be resent periodically 
     * until the returned ScheduledFuture is cancelled.
     */
    public ScheduledFuture<?> sendSignalRetrying(InetSocketAddress recipient, long targetNetworkId, String data, long delayMs) {
        return channel.eventLoop().scheduleAtFixedRate(() -> {
            log.debug("Resending signal to {}: {}", recipient, data);
            sendSignal(recipient, targetNetworkId, data);
        }, 0, delayMs, TimeUnit.MILLISECONDS);
    }

    public void sendSignal(InetSocketAddress recipient, long targetNetworkId, String data) {
        sendSignal(recipient, this.networkId, targetNetworkId, data);
    }

    public void sendSignal(InetSocketAddress recipient, long senderNetworkId, long targetNetworkId, String data) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_MESSAGE);
        buf.writeLongLE(senderNetworkId); // Sender ID
        buf.writeZero(8); // Padding

        buf.writeLongLE(targetNetworkId); // Recipient ID
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        buf.writeIntLE(dataBytes.length);
        buf.writeBytes(dataBytes);

        sendPacket(buf, recipient);
    }

    // New sendSignal looking up Address from ID
    public void sendSignal(long targetNetworkId, String data) {
        InetSocketAddress recipient = peerAddresses.get(targetNetworkId);
        if (recipient != null) {
            long senderNetworkId = this.networkId;
            try {
                String[] parts = data.split(" ", 3);
                if (parts.length >= 2) {
                    long connectionId = Long.parseUnsignedLong(parts[1]);
                    Long selectedRecipient = connectionRecipientByConnectionId.get(connectionId);
                    if (selectedRecipient != null) {
                        senderNetworkId = selectedRecipient;
                    }
                }
            } catch (Exception ignored) {
            }

            sendSignal(recipient, senderNetworkId, targetNetworkId, data);
        } else {
            throw new IllegalArgumentException("Attempted to send signal to unknown peer: " + targetNetworkId);
        }
    }

    private void sendPacket(ByteBuf packetData, InetSocketAddress target) {
        try {
            byte[] encrypted = NetherNetConstants.encryptDiscoveryPacket(packetData);
            channel.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(encrypted), target));
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt discovery packet", e);
        } finally {
            packetData.release();
        }
    }

    private static String encodeHex(byte[] data) {
        char[] out = new char[data.length * 2];
        for (int i = 0; i < data.length; i++) {
            int value = data[i] & 0xff;
            out[i * 2] = HEX[value >>> 4];
            out[i * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(out);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        ByteBuf content = packet.content();
        ByteBuf decrypted = null;
        try {
            decrypted = NetherNetConstants.decryptDiscoveryPacket(content);
        } catch (Exception e) {
            log.debug("Failed to decrypt discovery packet from {}", packet.sender(), e);
            return;
        }
        
        if (decrypted == null) {
            log.debug("Received invalid discovery packet from {}", packet.sender());
            return;
        }

        try {
            int packetId = decrypted.readUnsignedShortLE();
            long senderId = decrypted.readLongLE();

            decrypted.skipBytes(8); // Padding

            if (senderId == this.networkId) {
                log.debug("Ignoring own discovery packet");
                return;
            }

            peerAddresses.put(senderId, packet.sender());

            switch (packetId) {
                case NetherNetConstants.ID_DISCOVERY_REQUEST -> {
                    log.trace("Handled discovery request from {}", packet.sender());
                    handleRequest(senderId, packet.sender());
                }
                case NetherNetConstants.ID_DISCOVERY_MESSAGE -> {
                    log.trace("Handled discovery message from {}", packet.sender());
                    log.trace("Message Data: {}", decrypted.toString(StandardCharsets.UTF_8));
                    handleMessage(decrypted, senderId);
                }
                case NetherNetConstants.ID_DISCOVERY_RESPONSE -> {
                    log.trace("Handled discovery response from {}", packet.sender());
                    if (discoveryCallback != null) {
                        log.trace("Response Data: {}", decrypted.toString(StandardCharsets.UTF_8));
                        // Pass the payload (decrypted buffer) to the callback
                        // We retain it because we are passing it out of the pipeline handler
                        discoveryCallback.accept(senderId, decrypted.retain());
                    }
                }
                default -> {
                    log.debug("Received unknown discovery packet ID {} from {}", packetId, packet.sender());
                }
            }
        } catch (Exception e) {
            log.debug("Error processing discovery packet from {}", packet.sender(), e);
        } finally {
            decrypted.release();
        }
    }

    private void handleRequest(long senderId, InetSocketAddress sender) {
        if (!this.pongDataByNetworkId.isEmpty()) {
            for (Map.Entry<Long, byte[]> entry : this.pongDataByNetworkId.entrySet()) {
                ByteBuf buf = Unpooled.buffer();
                buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_RESPONSE);
                buf.writeLongLE(entry.getKey());
                buf.writeZero(8);
                buf.writeBytes(entry.getValue());
                sendPacket(buf, sender);
            }
        }

        BiConsumer<Long, InetSocketAddress> callback = this.discoveryRequestCallback;
        if (callback != null) {
            callback.accept(senderId, sender);
        }
    }

    private void handleMessage(ByteBuf data, long senderId) {
        long recipientId = data.readLongLE();

        if (recipientId != 0 && !this.pongDataByNetworkId.containsKey(recipientId)) {
            log.trace("Ignoring message intended for {}, but I am {}", recipientId, this.networkId);
            return;
        }

        int len = data.readIntLE();
        if (data.readableBytes() < len) {
            log.trace("Malformed message: claimed length {} but only has {}", len, data.readableBytes());
            return;
        }

        String messageData = data.readCharSequence(len, StandardCharsets.UTF_8).toString();
        if ("Ping".equals(messageData)) {
            return;
        }

        String[] parts = messageData.split(" ", 3);
        if (parts.length < 2) return;

        try {
            String type = parts[0];
            long connectionId = Long.parseUnsignedLong(parts[1]);
            
            NetherNetClientSignaling.ClientSignalFeed activeClientFeed = this.clientSignalFeed;
            if (activeClientFeed != null) {
                activeClientFeed.onSignal(messageData);
                return;
            }

            SignalHandler handler = signalHandlers.get(connectionId);

            if (handler != null) {
                handler.onSignal(messageData);
            } else if (NetherNetConstants.RTC_NEGOTIATION_CONNECT_REQUEST.equals(type)) {
                if (newConnectionHandler != null) {
                    String payload = parts.length > 2 ? parts[2] : "";
                    long effectiveRecipient = (recipientId == 0 ? this.networkId : recipientId);
                    connectionRecipientByConnectionId.put(connectionId, effectiveRecipient);
                    log.trace("Dispatching New Connection: ID={} Sender={}", Long.toUnsignedString(connectionId), Long.toUnsignedString(senderId));
                    newConnectionHandler.onConnect(connectionId, Long.toUnsignedString(senderId), payload);
                } else {
                    log.debug("Received CONNECT_REQUEST but no NewConnectionHandler is set!");
                }
            } else {
                log.debug("Unhandled signal type: {}", type);
            }
        } catch (NumberFormatException e) {
            log.debug("Invalid connection ID format in message: {}", messageData);
        }
    }

    public void close() {
        Channel existingChannel = this.channel;
        EventLoopGroup existingGroup = this.group;
        this.channel = null;
        this.group = null;
        if (existingChannel != null) {
            existingChannel.close().syncUninterruptibly();
        }
        if (existingGroup != null) {
            existingGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    public void setDiscoveryRequestCallback(BiConsumer<Long, InetSocketAddress> callback) {
        this.discoveryRequestCallback = callback;
    }

    public void sendDiscoveryResponsesTo(InetSocketAddress sender) {
        if (!isActive() || this.pongDataByNetworkId.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, byte[]> entry : this.pongDataByNetworkId.entrySet()) {
            ByteBuf buf = Unpooled.buffer();
            buf.writeShortLE(NetherNetConstants.ID_DISCOVERY_RESPONSE);
            buf.writeLongLE(entry.getKey());
            buf.writeZero(8);
            buf.writeBytes(entry.getValue());
            sendPacket(buf, sender);
        }
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }
    
    private void writeString(ByteBuf buf, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        this.writeUnsignedVarInt(buf, b.length); 
        buf.writeBytes(b);
    }

    private void writeUnsignedVarInt(ByteBuf buf, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buf.writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.writeByte((byte) value);
    }
}
