package tech.powerjob.remote.mu;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.remote.framework.base.Address;

/**
 * Worker heartbeat handler for maintaining connection and registration
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
public class MuWorkerHeartbeatHandler extends ChannelInboundHandlerAdapter {

    private final Address workerAddress;

    public MuWorkerHeartbeatHandler(Address workerAddress) {
        this.workerAddress = workerAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        sendHeartbeat(ctx);
        log.info("[MuWorkerHeartbeatHandler] Worker connected and sent initial heartbeat");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.WRITER_IDLE) {
                sendHeartbeat(ctx);
                log.debug("[MuWorkerHeartbeatHandler] Sent heartbeat");
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    private void sendHeartbeat(ChannelHandlerContext ctx) {
        MuMessage heartbeat = new MuMessage(
            MuMessage.MessageType.HEARTBEAT,
            null,
            null,
            workerAddress,
            null,
            null
        );
        ctx.writeAndFlush(heartbeat);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[MuWorkerHeartbeatHandler] Exception in heartbeat handler", cause);
        ctx.close();
    }
}