package tech.powerjob.remote.mu;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.remote.framework.base.Address;

/**
 * Mu protocol message format
 *
 * @author claude
 * @since 2025/1/1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MuMessage implements PowerSerializable {

    /**
     * Message types
     */
    public enum MessageType {
        TELL,           // Fire-and-forget
        ASK,            // Request-response
        RESPONSE,       // Response to ASK
        HEARTBEAT,      // Worker heartbeat/registration
        ERROR           // Error response
    }

    private MessageType messageType;
    private String requestId;           // Unique ID for ask/response correlation
    private String path;                // Handler path
    private Address senderAddress;      // Sender address for registration
    private Object payload;             // Actual message payload
    private String errorMessage;        // Error message for ERROR type
}