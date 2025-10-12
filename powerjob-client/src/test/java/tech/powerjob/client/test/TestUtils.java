package tech.powerjob.client.test;

import tech.powerjob.common.serialize.JsonUtils;

/**
 * TestUtils
 *
 * @author tjq
 * @since 2024/11/21
 */
public class TestUtils {

    public static void output(Object v) {
        String str = JsonUtils.toJSONString(v);
        System.out.println(str);
    }
}
