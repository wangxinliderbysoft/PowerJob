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
