package tech.powerjob.common.utils;

import org.apache.commons.lang3.StringUtils;
import tech.powerjob.common.PowerJobDKey;

/**
 * 系统工具
 *
 * @author tjq
 * @since 2025/8/17
 */
public class SysUtils {

    /**
     * 可用处理器核数
     * @return 可用处理器核数
     */
    public static int availableProcessors() {
        String property = System.getProperty(PowerJobDKey.SYS_AVAILABLE_PROCESSORS);
        if (StringUtils.isEmpty(property)) {
            return Runtime.getRuntime().availableProcessors();
        }
        return Integer.parseInt(property);
    }

    /**
     * 判断是否为测试环境
     * @return 测试环境
     */
    public static boolean isTestEnv() {
        String property = System.getProperty(PowerJobDKey.MARK_TEST_ENV);
        if (StringUtils.isEmpty(property)) {
            return false;
        }
        return Boolean.TRUE.toString().equalsIgnoreCase(property);
    }
}
