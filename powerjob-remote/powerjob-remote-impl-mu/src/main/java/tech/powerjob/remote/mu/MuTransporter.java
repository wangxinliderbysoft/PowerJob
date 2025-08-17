package tech.powerjob.remote.mu;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.remote.framework.base.RemotingException;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.remote.framework.transporter.Protocol;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Mu protocol transporter implementation
 * Handles both client-side (worker) and reverse (server-to-worker) communication
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
public class MuTransporter implements Transporter {

    private static final Protocol PROTOCOL = new MuProtocol();
    private static final long ASK_TIMEOUT_SECONDS = 30;

    private final ChannelManager channelManager;
    private final ServerType serverType;
    private final MuConnectionManager connectionManager; // For worker-side lazy connection

    public MuTransporter(ChannelManager channelManager, ServerType serverType, MuConnectionManager connectionManager) {
        this.channelManager = channelManager;
        this.serverType = serverType;
        this.connectionManager = connectionManager;
    }

    @Override
    public Protocol getProtocol() {
        return PROTOCOL;
    }

    @Override
    public void tell(URL url, PowerSerializable request) {
        try {
            MuMessage message = new MuMessage(
                MuMessage.MessageType.TELL,
                null,
                url.getLocation().toPath(),
                null,
                request,
                null
            );

            if (serverType == ServerType.WORKER) {
                // Worker to server/worker: use connection manager for lazy connection
                connectionManager.getOrCreateConnection(url.getAddress())
                    .thenAccept(channel -> {
                        if (channel.isActive()) {
                            channel.writeAndFlush(message);
                            log.debug("[MuTransporter] Sent TELL message to {}", url);
                        } else {
                            log.error("[MuTransporter] Channel is not active for {}", url);
                        }
                    })
                    .exceptionally(throwable -> {
                        log.error("[MuTransporter] Failed to get connection for TELL to {}", url, throwable);
                        return null;
                    });
            } else {
                // Server side: distinguish between worker and server targets
                if (url.getServerType() == ServerType.WORKER) {
                    // Server to worker: use stored channel from worker registration
                    Channel channel = channelManager.getWorkerChannel(url.getAddress());
                    if (channel != null && channel.isActive()) {
                        channel.writeAndFlush(message);
                        log.debug("[MuTransporter] Sent TELL message to worker {}", url);
                    } else {
                        log.error("[MuTransporter] No active channel available for worker {}", url);
                        throw new RemotingException("No active channel available for " + url);
                    }
                } else {
                    // Server to server: use connection manager for direct connection
                    connectionManager.getOrCreateConnection(url.getAddress())
                        .thenAccept(channel -> {
                            if (channel.isActive()) {
                                channel.writeAndFlush(message);
                                log.debug("[MuTransporter] Sent TELL message to server {}", url);
                            } else {
                                log.error("[MuTransporter] Channel is not active for server {}", url);
                            }
                        })
                        .exceptionally(throwable -> {
                            log.error("[MuTransporter] Failed to get connection for TELL to server {}", url, throwable);
                            return null;
                        });
                }
            }
        } catch (Exception e) {
            log.error("[MuTransporter] Failed to send TELL message to {}", url, e);
            throw new RemotingException("Failed to send TELL message", e);
        }
    }

    @Override
    public <T> CompletionStage<T> ask(URL url, PowerSerializable request, Class<T> clz) throws RemotingException {
        try {
            String requestId = UUID.randomUUID().toString();
            CompletableFuture<T> future = new CompletableFuture<>();

            // Register the future for response handling
            channelManager.registerPendingRequest(requestId, (CompletableFuture<Object>) future, clz);

            // Set timeout for the request (JDK8 compatible)
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    channelManager.removePendingRequest(requestId);
                }
            });

            // Schedule timeout manually for JDK8 compatibility
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                if (!future.isDone()) {
                    channelManager.removePendingRequest(requestId);
                    future.completeExceptionally(new java.util.concurrent.TimeoutException("Request timeout after " + ASK_TIMEOUT_SECONDS + " seconds"));
                }
            }, ASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            MuMessage message = new MuMessage(
                MuMessage.MessageType.ASK,
                requestId,
                url.getLocation().toPath(),
                null,
                request,
                null
            );

            if (serverType == ServerType.WORKER) {
                // Worker to server/worker: use connection manager for lazy connection
                connectionManager.getOrCreateConnection(url.getAddress())
                    .thenAccept(channel -> {
                        if (channel.isActive()) {
                            channel.writeAndFlush(message);
                            log.debug("[MuTransporter] Sent ASK message to {} with requestId {}", url, requestId);
                        } else {
                            channelManager.removePendingRequest(requestId);
                            future.completeExceptionally(new RemotingException("Channel is not active for " + url));
                        }
                    })
                    .exceptionally(throwable -> {
                        channelManager.removePendingRequest(requestId);
                        future.completeExceptionally(new RemotingException("Failed to get connection for ASK to " + url, throwable));
                        return null;
                    });
            } else {
                // Server side: distinguish between worker and server targets
                if (url.getServerType() == ServerType.WORKER) {
                    // Server to worker: use stored channel from worker registration
                    Channel channel = channelManager.getWorkerChannel(url.getAddress());
                    if (channel != null && channel.isActive()) {
                        channel.writeAndFlush(message);
                        log.debug("[MuTransporter] Sent ASK message to worker {} with requestId {}", url, requestId);
                    } else {
                        channelManager.removePendingRequest(requestId);
                        future.completeExceptionally(new RemotingException("No active channel available for " + url));
                    }
                } else {
                    // Server to server: use connection manager for direct connection
                    connectionManager.getOrCreateConnection(url.getAddress())
                        .thenAccept(channel -> {
                            if (channel.isActive()) {
                                channel.writeAndFlush(message);
                                log.debug("[MuTransporter] Sent ASK message to server {} with requestId {}", url, requestId);
                            } else {
                                channelManager.removePendingRequest(requestId);
                                future.completeExceptionally(new RemotingException("Channel is not active for server " + url));
                            }
                        })
                        .exceptionally(throwable -> {
                            channelManager.removePendingRequest(requestId);
                            future.completeExceptionally(new RemotingException("Failed to get connection for ASK to server " + url, throwable));
                            return null;
                        });
                }
            }

            return future;
        } catch (Exception e) {
            log.error("[MuTransporter] Failed to send ASK message to {}", url, e);
            throw new RemotingException("Failed to send ASK message", e);
        }
    }
}