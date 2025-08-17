package tech.powerjob.worker.background.heartbeat;

import tech.powerjob.common.model.SystemMetrics;
import tech.powerjob.worker.common.utils.SystemInfoUtils;
import tech.powerjob.worker.extension.SystemMetricsCollector;

/**
 * 默认的系统指标采集器
 *
 * @author tjq
 * @since 2025/8/17
 */
public class DefaultSystemMetricsCollector implements SystemMetricsCollector {
    @Override
    public SystemMetrics collect() {
        return SystemInfoUtils.getSystemMetrics();
    }
}
