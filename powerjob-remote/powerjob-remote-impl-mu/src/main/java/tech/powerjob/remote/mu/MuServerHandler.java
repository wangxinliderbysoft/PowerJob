package tech.powerjob.remote.mu;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
 * Server-side message handler for Mu protocol
 * Handles incoming messages from workers and manages channel registration
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
@ChannelHandler.Sharable
public class MuServerHandler extends SimpleChannelInboundHandler<MuMessage> {

    private final ChannelManager channelManager;
    private final Map<String, ActorInfo> handlerMap = new ConcurrentHashMap<>();

    public MuServerHandler(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    public void bindHandlers(List<ActorInfo> actorInfos) {
        for (ActorInfo actorInfo : actorInfos) {
            if (actorInfo.getHandlerInfos() != null) {
                for (HandlerInfo handlerInfo : actorInfo.getHandlerInfos()) {
                    String path = handlerInfo.getLocation().toPath();
                    handlerMap.put(path, actorInfo);
                    log.info("[MuServerHandler] Bound handler: {}", path);
                }
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MuMessage msg) throws Exception {
        try {
            switch (msg.getMessageType()) {
                case HEARTBEAT:
                    handleHeartbeat(ctx, msg);
                    break;
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
                default:
                    log.warn("[MuServerHandler] Unknown message type: {}", msg.getMessageType());
            }
        } catch (Exception e) {
            log.error("[MuServerHandler] Error processing message", e);
            if (msg.getMessageType() == MuMessage.MessageType.ASK) {
                // Send error response for ASK messages
                MuMessage errorResponse = new MuMessage(
                    MuMessage.MessageType.ERROR,
                    msg.getRequestId(),
                    null,
                    null,
                    null,
                    "Internal server error: " + e.getMessage()
                );
                ctx.writeAndFlush(errorResponse);
            }
        }
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, MuMessage msg) {
        if (msg.getSenderAddress() != null) {
            channelManager.registerWorkerChannel(msg.getSenderAddress(), ctx.channel());
            log.debug("[MuServerHandler] Registered worker: {}", msg.getSenderAddress());
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

    private Object invokeHandler(MuMessage msg, boolean needResponse, ChannelHandlerContext ctx) {
        try {
            String path = msg.getPath();
            ActorInfo actorInfo = handlerMap.get(path);
            
            if (actorInfo == null) {
                log.warn("[MuServerHandler] No handler found for path: {}", path);
                return null;
            }

            HandlerInfo handlerInfo = actorInfo.getHandlerInfos().stream()
                .filter(h -> h.getLocation().toPath().equals(path))
                .findFirst()
                .orElse(null);

            if (handlerInfo == null) {
                log.warn("[MuServerHandler] Handler info not found for path: {}", path);
                return null;
            }

            Method method = handlerInfo.getMethod();
            Optional<Class<?>> powerSerializeClz = RemoteUtils.findPowerSerialize(method.getParameterTypes());

            if (!powerSerializeClz.isPresent()) {
                log.error("[MuServerHandler] No PowerSerializable parameter found for handler: {}", path);
                return null;
            }

            Object convertedPayload = convertPayload(msg.getPayload(), powerSerializeClz.get());
            Object response = method.invoke(actorInfo.getActor(), convertedPayload);
            
            if (needResponse) {
                return response;
            }
            
            return null;
        } catch (Exception e) {
            log.error("[MuServerHandler] Failed to invoke handler for path: {}", msg.getPath(), e);
            return null;
        }
    }

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
        log.error("[MuServerHandler] Channel exception", cause);
        ctx.close();
    }
}