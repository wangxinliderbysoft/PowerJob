package tech.powerjob.remote.mu;

import tech.powerjob.remote.framework.transporter.Protocol;

/**
 * Mu Protocol implementation using Netty for bidirectional communication
 * with support for worker-only outbound connectivity
 *
 * @author claude
 * @since 2025/1/1
 */
public class MuProtocol implements Protocol {

    @Override
    public String name() {
        return tech.powerjob.common.enums.Protocol.MU.name();
    }
}