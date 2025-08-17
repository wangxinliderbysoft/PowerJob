package tech.powerjob.remote.framework.actor;

import lombok.extern.slf4j.Slf4j;

/**
 * 内置一个用来通用测试的 TestActor
 *
 * @author tjq
 * @since 2022/12/31
 */
@Slf4j
@Actor(path = "test")
public class TestActor {

    public static void simpleStaticMethod() {
    }

    public void simpleMethod() {
    }

    @Handler(path = "method1")
    public String handlerMethod1() {
        log.info("[TestActor] handlerMethod1");
        return "1";
    }

}
