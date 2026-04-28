package dev.kastle.netty.channel.nethernet.signaling;

import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

public class NetherNetDiscoverySignaling implements NetherNetClientSignaling, NetherNetServerSignaling {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetDiscoverySignaling.class);

    private final NetherNetDiscovery discovery;
    private final InetSocketAddress bindAddress;
    private final String localNetworkId;
    private final Map<Long, NetherNetDiscovery> extraDiscoveries = new LinkedHashMap<>();
    private NetherNetServerSignaling.NewConnectionHandler newConnectionHandler;
    private volatile boolean primaryAdvertisementAssigned = false;
    private volatile InetSocketAddress currentBindBaseAddress;
    
    // State captured after connect
    private volatile InetSocketAddress remoteAddress;
    private final AtomicReference<String> discoveredServerId = new AtomicReference<>(null);

    /**
     * Creates a NetherNetDiscoverySignaling with a random local Network ID and binds to an ephemeral port.     * 
     */
    public NetherNetDiscoverySignaling() {
        this(ThreadLocalRandom.current().nextLong(), new InetSocketAddress(0));
    }

    /**
     * Creates a NetherNetDiscoverySignaling with the specified local Network ID.
     * 
     * @param localNetworkId The local Network ID to use.
     */
    public NetherNetDiscoverySignaling(long localNetworkId) {
        this(localNetworkId, new InetSocketAddress(0));
    }

    /**
     * Creates a NetherNetDiscoverySignaling with the specified local Network ID and bind address.
     * 
     * @param localNetworkId The local Network ID to use.
     * @param bindAddress    The address to bind the discovery socket to.
     */
    public NetherNetDiscoverySignaling(long localNetworkId, InetSocketAddress bindAddress) {
        this.localNetworkId = Long.toUnsignedString(localNetworkId);
        this.discovery = new NetherNetDiscovery(localNetworkId);
        this.bindAddress = bindAddress;
    }

    @Override
    public String getLocalNetworkId() {
        return this.localNetworkId;
    }

    @Override
    public CompletableFuture<List<IceServerInfo>> connect(SocketAddress remote) {
        CompletableFuture<List<IceServerInfo>> future = new CompletableFuture<>();
        
        if (!(remote instanceof InetSocketAddress)) {
            future.completeExceptionally(new IllegalArgumentException("Discovery requires InetSocketAddress"));
            return future;
        }
        
        this.remoteAddress = (InetSocketAddress) remote;

        try {
            if (!this.discovery.isActive()) {
                log.info("Binding NetherNet Discovery to {}", bindAddress);
                this.discovery.bind(bindAddress);
            }

            log.debug("Sending Discovery Request to {}", remote);
            
            // Send request and register the callback to capture the ID
            this.discovery.sendDiscoveryRequest(this.remoteAddress, (serverNetworkId, payload) -> {
                try {
                    log.info("Discovery Response Received! Server NetworkID: {}", serverNetworkId);
                    
                    // Capture the ID so we can use it for signaling later
                    discoveredServerId.set(Long.toUnsignedString(serverNetworkId));
                    
                    future.complete(Collections.emptyList());
                } catch (Exception e) {
                    log.error("Error processing discovery response", e);
                    future.completeExceptionally(e);
                } finally {
                    ReferenceCountUtil.release(payload);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send discovery request", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void bind(SocketAddress localAddress) {
        if (!this.discovery.isActive()) {
            if (localAddress instanceof InetSocketAddress) {
                this.discovery.bind((InetSocketAddress) localAddress);
            } else {
                this.discovery.bind(bindAddress);
            }
            log.info("Primary discovery bound localNetworkId={}", this.localNetworkId);
        }

        InetSocketAddress base = localAddress instanceof InetSocketAddress
                ? (InetSocketAddress) localAddress
                : bindAddress;
        this.currentBindBaseAddress = base;
        int boundExtras = 0;
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            if (extra.isActive()) {
                continue;
            }
            InetSocketAddress extraBind = new InetSocketAddress(base.getAddress(), 0);
            extra.bind(extraBind);
            boundExtras++;
        }
        if (boundExtras > 0) {
            log.info("Extra discovery sockets bound: {}", boundExtras);
        }
    }

    @Override
    public void setNewConnectionHandler(NetherNetServerSignaling.NewConnectionHandler handler) {
        this.newConnectionHandler = handler;
        this.discovery.setNewConnectionHandler(handler);
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            extra.setNewConnectionHandler(handler);
        }
    }

    @Override
    public void setAdvertisementData(PongData pongData) {
        this.discovery.setPongData(pongData);
    }

    public synchronized void addAdvertisementData(long localNetworkId, PongData pongData) {
        // Keep every advertised ID registered on primary discovery as well,
        // so signaling packets that still land on the primary port can pass recipient checks.
        this.discovery.setPongData(localNetworkId, pongData);

        if (!this.primaryAdvertisementAssigned) {
            this.primaryAdvertisementAssigned = true;
            log.info("Registered primary advertisement localNetworkId={}", Long.toUnsignedString(localNetworkId));
            return;
        }

        NetherNetDiscovery extra = this.extraDiscoveries.computeIfAbsent(localNetworkId, id -> {
            NetherNetDiscovery d = new NetherNetDiscovery(id);
            if (this.newConnectionHandler != null) {
                d.setNewConnectionHandler(this.newConnectionHandler);
            }
            log.info("Registered extra advertisement localNetworkId={}", Long.toUnsignedString(id));
            return d;
        });
        extra.setPongData(localNetworkId, pongData);
        if (this.discovery.isActive() && !extra.isActive()) {
            InetSocketAddress base = this.currentBindBaseAddress != null ? this.currentBindBaseAddress : this.bindAddress;
            InetSocketAddress extraBind = new InetSocketAddress(base.getAddress(), 0);
            extra.bind(extraBind);
        }

        this.discovery.setDiscoveryRequestCallback((senderId, senderAddr) -> {
            for (NetherNetDiscovery d : this.extraDiscoveries.values()) {
                if (!d.isActive()) {
                    continue;
                }
                d.sendDiscoveryResponsesTo(senderAddr);
            }
        });
    }

    public synchronized void removeAdvertisementData(long localNetworkId) {
        this.discovery.removePongData(localNetworkId);
        NetherNetDiscovery extra = this.extraDiscoveries.remove(localNetworkId);
        if (extra != null) {
            extra.removePongData(localNetworkId);
            extra.close();
        }
    }

    public synchronized void syncAdvertisementData(Map<Long, PongData> advertisements) {
        Set<Long> current = new HashSet<>(this.discovery.getAdvertisedNetworkIds());
        for (Long localNetworkId : current) {
            if (!advertisements.containsKey(localNetworkId)) {
                this.removeAdvertisementData(localNetworkId);
            }
        }
        for (Map.Entry<Long, PongData> entry : advertisements.entrySet()) {
            this.addAdvertisementData(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void sendSignal(String targetNetworkId, String data) {
        String actualIdStr = targetNetworkId;

        // If '0' is passed, try to use the discovered ID (Client Mode)
        if (actualIdStr == null || actualIdStr.equals("0")) {
            actualIdStr = discoveredServerId.get();
        }

        if (actualIdStr == null) {
            log.warn("Cannot send signal: Unknown Network ID.");
            return;
        }
        
        try {
            long id = Long.parseUnsignedLong(actualIdStr);
            
            // If we have an explicit remote address (Client Mode), use it directly
            if (remoteAddress != null) {
                this.discovery.sendSignal(remoteAddress, id, data);
            } else {
                Long connectionId = null;
                try {
                    String[] parts = data.split(" ", 3);
                    if (parts.length >= 2) {
                        connectionId = Long.parseUnsignedLong(parts[1]);
                    }
                } catch (Exception ignored) {
                }

                boolean sent = false;

                // Prefer the discovery instance that accepted this connection.
                if (connectionId != null) {
                    if (this.discovery.getConnectionRecipient(connectionId) != null) {
                        try {
                            this.discovery.sendSignal(id, data);
                            sent = true;
                        } catch (IllegalArgumentException ignored) {
                        }
                    } else {
                        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
                            if (extra.getConnectionRecipient(connectionId) == null) {
                                continue;
                            }
                            try {
                                extra.sendSignal(id, data);
                                sent = true;
                                break;
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                    }
                }

                if (!sent) {
                    try {
                        this.discovery.sendSignal(id, data);
                        sent = true;
                    } catch (IllegalArgumentException ignored) {
                    }
                    if (!sent) {
                        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
                            try {
                                extra.sendSignal(id, data);
                                sent = true;
                                break;
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                    }
                }
                if (!sent) {
                    throw new IllegalArgumentException("Attempted to send signal to unknown peer: " + id);
                }
            }
        } catch (NumberFormatException e) {
            log.error("Cannot send LAN signal to non-numeric Network ID: {}", actualIdStr);
        }
    }

    @Override
    public void setSignalHandler(long connectionId, SignalHandler handler) {
        this.discovery.registerSignalHandler(connectionId, handler);
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            extra.registerSignalHandler(connectionId, handler);
        }
    }

    @Override
    public void removeSignalHandler(long connectionId) {
        this.discovery.unregisterSignalHandler(connectionId);
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            extra.unregisterSignalHandler(connectionId);
        }
    }

    @Override
    public void setClientSignalFeed(ClientSignalFeed feed) {
        this.discovery.setClientSignalFeed(feed);
    }

    @Override
    public void clearClientSignalFeed() {
        this.discovery.clearClientSignalFeed();
    }

    @Override
    public String resolveLocalNetworkIdForConnection(long connectionId) {
        Long recipient = this.discovery.getConnectionRecipient(connectionId);
        if (recipient != null) {
            return Long.toUnsignedString(recipient);
        }
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            recipient = extra.getConnectionRecipient(connectionId);
            if (recipient != null) {
                return Long.toUnsignedString(recipient);
            }
        }
        return null;
    }

    @Override
    public void setNotFoundHandler(NetherNetClientSignaling.NotFoundHandler handler) {
        // Not implemented for Discovery signaling
    }

    @Override
    public void close() {
        this.discovery.close();
        for (NetherNetDiscovery extra : this.extraDiscoveries.values()) {
            extra.close();
        }
        this.extraDiscoveries.clear();
    }
}
