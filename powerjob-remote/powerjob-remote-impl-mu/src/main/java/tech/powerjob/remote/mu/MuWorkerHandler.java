package tech.powerjob.remote.mu;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.remote.framework.actor.ActorInfo;
import tech.powerjob.remote.framework.actor.HandlerInfo;
import tech.powerjob.remote.framework.utils.RemoteUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-side message handler for Mu protocol
 * Handles incoming messages from server and processes local handlers
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
@ChannelHandler.Sharable
public class MuWorkerHandler extends SimpleChannelInboundHandler<MuMessage> {

    private final Map<String, ActorInfo> handlerMap = new ConcurrentHashMap<>();
    private final ChannelManager channelManager;

    public MuWorkerHandler(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    public void bindHandlers(List<ActorInfo> actorInfos) {
        for (ActorInfo actorInfo : actorInfos) {
            if (actorInfo.getHandlerInfos() != null) {
                for (HandlerInfo handlerInfo : actorInfo.getHandlerInfos()) {
                    String path = handlerInfo.getLocation().toPath();
                    handlerMap.put(path, actorInfo);
                    log.info("[MuWorkerHandler] Bound handler: {}", path);
                }
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MuMessage msg) throws Exception {
        try {
            switch (msg.getMessageType()) {
                case TELL:
                    handleTell(ctx, msg);
                    break;
                case ASK:
                    handleAsk(ctx, msg);
                    break;
                case RESPONSE:
                    handleResponse(ctx, msg);
                    break;
                case ERROR:
                    handleError(ctx, msg);
                    break;
                case HEARTBEAT:
                    handleHeartbeat(ctx, msg);
                    break;
                default:
                    log.warn("[MuWorkerHandler] Unknown message type: {}", msg.getMessageType());
            }
        } catch (Exception e) {
            log.error("[MuWorkerHandler] Error processing message", e);
            if (msg.getMessageType() == MuMessage.MessageType.ASK) {
                // Send error response for ASK messages
                MuMessage errorResponse = new MuMessage(
                    MuMessage.MessageType.ERROR,
                    msg.getRequestId(),
                    null,
                    null,
                    null,
                    "Internal worker error: " + e.getMessage()
                );
                ctx.writeAndFlush(errorResponse);
            }
        }
    }

    private void handleTell(ChannelHandlerContext ctx, MuMessage msg) {
        invokeHandler(msg, false, ctx);
    }

    private void handleAsk(ChannelHandlerContext ctx, MuMessage msg) {
        Object response = invokeHandler(msg, true, ctx);
        
        MuMessage.MessageType responseType = response != null ? 
            MuMessage.MessageType.RESPONSE : MuMessage.MessageType.ERROR;
        String errorMessage = response == null ? "Handler returned null" : null;
        
        MuMessage responseMsg = new MuMessage(
            responseType,
            msg.getRequestId(),
            null,
            null,
            response,
            errorMessage
        );
        
        ctx.writeAndFlush(responseMsg);
    }

    private void handleResponse(ChannelHandlerContext ctx, MuMessage msg) {
        channelManager.completePendingRequest(msg.getRequestId(), msg.getPayload());
    }

    private void handleError(ChannelHandlerContext ctx, MuMessage msg) {
        Exception exception = new RuntimeException(msg.getErrorMessage());
        channelManager.completePendingRequestExceptionally(msg.getRequestId(), exception);
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, MuMessage msg) {
        // Worker接收到心跳消息时，通常不需要特殊处理
        // 但记录一下调试信息，表明收到了心跳
        if (msg.getSenderAddress() != null) {
            log.debug("[MuWorkerHandler] Received heartbeat from: {}", msg.getSenderAddress());
        } else {
            log.debug("[MuWorkerHandler] Received heartbeat");
        }
    }

    private Object invokeHandler(MuMessage msg, boolean needResponse, ChannelHandlerContext ctx) {
        try {
            String path = msg.getPath();
            ActorInfo actorInfo = handlerMap.get(path);
            
            if (actorInfo == null) {
                log.warn("[MuWorkerHandler] No handler found for path: {}", path);
                return null;
            }

            HandlerInfo handlerInfo = actorInfo.getHandlerInfos().stream()
                .filter(h -> h.getLocation().toPath().equals(path))
                .findFirst()
                .orElse(null);

            if (handlerInfo == null) {
                log.warn("[MuWorkerHandler] Handler info not found for path: {}", path);
                return null;
            }

            Method method = handlerInfo.getMethod();
            Optional<Class<?>> powerSerializeClz = RemoteUtils.findPowerSerialize(method.getParameterTypes());

            if (!powerSerializeClz.isPresent()) {
                log.error("[MuWorkerHandler] No PowerSerializable parameter found for handler: {}", path);
                return null;
            }

            Object convertedPayload = convertPayload(msg.getPayload(), powerSerializeClz.get());
            Object response = method.invoke(actorInfo.getActor(), convertedPayload);
            
            if (needResponse) {
                return response;
            }
            
            return null;
        } catch (Exception e) {
            log.error("[MuWorkerHandler] Failed to invoke handler for path: {}", msg.getPath(), e);
            return null;
        }
    }

    @SneakyThrows
    private Object convertPayload(Object payload, Class<?> targetClass) {
        if (payload == null) {
            return null;
        }
        
        if (targetClass.isInstance(payload)) {
            return payload;
        }

        return JsonUtils.toJavaObject(payload, targetClass);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[MuWorkerHandler] Channel exception", cause);
        ctx.close();
    }
}