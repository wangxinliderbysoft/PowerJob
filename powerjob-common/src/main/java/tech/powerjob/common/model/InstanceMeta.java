package tech.powerjob.common.model;

import lombok.Data;
import tech.powerjob.common.PowerSerializable;

/**
 * 调度元信息
 *
 * @author tjq
 * @since 2025/8/17
 */
@Data
public class InstanceMeta implements PowerSerializable {

    /**
     * expectTriggerTime
     * 原始的期望调度时间（无论重试N次都保留最开始的期望调度时间）
     */
    private Long ett;

    public InstanceMeta() {
    }
}
