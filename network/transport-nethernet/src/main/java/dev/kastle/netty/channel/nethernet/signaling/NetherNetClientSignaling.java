package dev.kastle.netty.channel.nethernet.signaling;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface NetherNetClientSignaling extends NetherNetSignaling {
    /**
     * Connects to the signaling medium (Client mode).
     * 
     * @param remoteAddress The address of the signaling server to connect to.
     */
    CompletableFuture<List<IceServerInfo>> connect(SocketAddress remoteAddress);

    /**
     * Sets the active client-side signal handler for a single handshake attempt.
     *
     * <p>The client path is single-connection oriented. Unlike the server path,
     * this should represent the one live handshake/feed for the current signaling
     * session rather than a shared per-connection dispatcher table.</p>
     *
     * @param connectionId The connection ID expected for the current handshake.
     * @param handler      The handler to process incoming signaling messages.
     */
    void setClientSignalFeed(ClientSignalFeed feed);

    /**
     * Clears the active client-side raw signal feed.
     */
    void clearClientSignalFeed();

    /**
     * Sets a handler to be called when a signaling message is received for an unknown connection ID.
     *
     * @param handler The handler to process incoming signaling messages for unknown connection IDs.
     */
    void setNotFoundHandler(NotFoundHandler handler);

    /**
     * Functional interface for handling "Not Found" signals.
     */
    @FunctionalInterface
    interface NotFoundHandler {
        /**
         * Called when the signaling service indicates the target peer was not found.
         * 
         * @param reason The reason or raw message payload regarding the failure.
         */
        void onNotFound(String reason);
    }

    /**
     * Functional interface for the single active client-side signaling feed.
     */
    @FunctionalInterface
    interface ClientSignalFeed {
        void onSignal(String signal);
    }
}
