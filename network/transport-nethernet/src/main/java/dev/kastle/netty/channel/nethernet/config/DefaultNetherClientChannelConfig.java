package dev.kastle.netty.channel.nethernet.config;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;

import java.util.Map;

public class DefaultNetherClientChannelConfig extends DefaultNetherChannelConfig  {
    private volatile int clientHandshakeTimeoutMs = 3000;
    private volatile int maxHandshakeAttempts = 3;
    private volatile boolean createUnreliableChannel = true;
    private volatile boolean closeSignalingOnClose = true;

    public DefaultNetherClientChannelConfig(Channel channel) {
        super(channel);
    }

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return this.getOptions(
                super.getOptions(), 
                NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS, 
                NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS,
                NetherChannelOption.NETHER_CLIENT_CREATE_UNRELIABLE_CHANNEL,
                NetherChannelOption.NETHER_CLIENT_CLOSE_SIGNALING_ON_CLOSE
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOption(ChannelOption<T> option) {
        if (option == NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS) {
            return (T) Integer.valueOf(this.clientHandshakeTimeoutMs);
        } else if (option == NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS) {
            return (T) Integer.valueOf(this.maxHandshakeAttempts);
        } else if (option == NetherChannelOption.NETHER_CLIENT_CREATE_UNRELIABLE_CHANNEL) {
            return (T) Boolean.valueOf(this.createUnreliableChannel);
        } else if (option == NetherChannelOption.NETHER_CLIENT_CLOSE_SIGNALING_ON_CLOSE) {
            return (T) Boolean.valueOf(this.closeSignalingOnClose);
        }

        return super.getOption(option);
    }

    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
        this.validate(option, value);

        if (option == NetherChannelOption.NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS) {
            this.setClientHandshakeTimeoutMs((Integer) value);
            return true;
        } else if (option == NetherChannelOption.NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS) {
            this.setMaxHandshakeAttempts((Integer) value);
            return true;
        } else if (option == NetherChannelOption.NETHER_CLIENT_CREATE_UNRELIABLE_CHANNEL) {
            this.setCreateUnreliableChannel((Boolean) value);
            return true;
        } else if (option == NetherChannelOption.NETHER_CLIENT_CLOSE_SIGNALING_ON_CLOSE) {
            this.setCloseSignalingOnClose((Boolean) value);
            return true;
        } else {
            return super.setOption(option, value);
        }
    }

    void setClientHandshakeTimeoutMs(int clientHandshakeTimeoutMs) {
        this.clientHandshakeTimeoutMs = clientHandshakeTimeoutMs;
    }

    void setMaxHandshakeAttempts(int maxHandshakeAttempts) {
        this.maxHandshakeAttempts = maxHandshakeAttempts;
    }

    void setCreateUnreliableChannel(boolean createUnreliableChannel) {
        this.createUnreliableChannel = createUnreliableChannel;
    }

    void setCloseSignalingOnClose(boolean closeSignalingOnClose) {
        this.closeSignalingOnClose = closeSignalingOnClose;
    }
}
