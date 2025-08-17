package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 特殊环境
 *
 * @author tjq
 * @since 2025/8/17
 */
@Getter
@AllArgsConstructor
public enum SpEnv {

    /**
     * 测试环境
     */
    TEST("test"),
    /**
     * 试用环境
     */
    TRIAL("trial")
    ;

    private final String code;
}
