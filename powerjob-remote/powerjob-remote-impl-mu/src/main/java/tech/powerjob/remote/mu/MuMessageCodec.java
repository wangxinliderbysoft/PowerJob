package tech.powerjob.remote.mu;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Mu message codec for encoding/decoding messages over Netty
 *
 * @author claude
 * @since 2025/1/1
 */
@Slf4j
public class MuMessageCodec extends ByteToMessageCodec<MuMessage> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024; // 64MB

    @Override
    protected void encode(ChannelHandlerContext ctx, MuMessage msg, ByteBuf out) throws Exception {
        try {
            byte[] data = OBJECT_MAPPER.writeValueAsBytes(msg);
            if (data.length > MAX_MESSAGE_SIZE) {
                throw new IllegalArgumentException("Message too large: " + data.length + " bytes");
            }
            
            // Write message length followed by message data
            out.writeInt(data.length);
            out.writeBytes(data);
        } catch (Exception e) {
            log.error("[MuMessageCodec] Failed to encode message", e);
            throw e;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            // Need at least 4 bytes for length
            if (in.readableBytes() < 4) {
                return;
            }

            // Mark reader index to reset if not enough data
            in.markReaderIndex();
            
            // Read message length
            int length = in.readInt();
            
            if (length <= 0 || length > MAX_MESSAGE_SIZE) {
                throw new IllegalArgumentException("Invalid message length: " + length);
            }

            // Check if we have enough bytes for the full message
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            // Read and decode message
            byte[] data = new byte[length];
            in.readBytes(data);
            
            MuMessage message = OBJECT_MAPPER.readValue(data, MuMessage.class);
            out.add(message);
        } catch (Exception e) {
            log.error("[MuMessageCodec] Failed to decode message", e);
            throw e;
        }
    }
}