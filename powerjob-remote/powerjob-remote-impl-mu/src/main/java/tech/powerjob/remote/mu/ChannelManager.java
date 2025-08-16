package tech.powerjob.remote.mu;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.remote.framework.base.Address;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Channel manager for maintaining worker address to channel mapping
 * Supports both tell and ask modes for reverse communication
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
public class ChannelManager {

    private final ConcurrentMap<String, Channel> workerChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Register a worker channel
     * @param workerAddress worker address
     * @param channel Netty channel
     */
    public void registerWorkerChannel(Address workerAddress, Channel channel) {
        String key = workerAddress.getHost() + ":" + workerAddress.getPort();
        workerChannels.put(key, channel);
        log.info("[ChannelManager] Registered worker channel: {}", key);
        
        // Remove channel when it becomes inactive
        channel.closeFuture().addListener(future -> {
            workerChannels.remove(key);
            log.info("[ChannelManager] Removed inactive worker channel: {}", key);
        });
    }

    /**
     * Get channel for worker
     * @param workerAddress worker address
     * @return Channel or null if not found
     */
    public Channel getWorkerChannel(Address workerAddress) {
        String key = workerAddress.getHost() + ":" + workerAddress.getPort();
        return workerChannels.get(key);
    }

    /**
     * Store pending request for ask mode
     * @param requestId request ID
     * @param future future to complete when response received
     */
    public void registerPendingRequest(String requestId, CompletableFuture<Object> future) {
        pendingRequests.put(requestId, future);
    }

    /**
     * Complete pending request with response
     * @param requestId request ID
     * @param response response object
     */
    public void completePendingRequest(String requestId, Object response) {
        CompletableFuture<Object> future = pendingRequests.remove(requestId);
        if (future != null) {
            future.complete(response);
        } else {
            log.warn("[ChannelManager] No pending request found for ID: {}", requestId);
        }
    }

    /**
     * Complete pending request with exception
     * @param requestId request ID
     * @param exception exception
     */
    public void completePendingRequestExceptionally(String requestId, Throwable exception) {
        CompletableFuture<Object> future = pendingRequests.remove(requestId);
        if (future != null) {
            future.completeExceptionally(exception);
        } else {
            log.warn("[ChannelManager] No pending request found for ID: {}", requestId);
        }
    }

    /**
     * Remove pending request (timeout cleanup)
     * @param requestId request ID
     */
    public void removePendingRequest(String requestId) {
        pendingRequests.remove(requestId);
    }
}