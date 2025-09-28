package tech.powerjob.official.processors.impl;

import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.TestUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * HttpProcessorTest
 *
 * @author tjq
 * @since 2021/1/31
 */
class HttpProcessorTest {
    
    @Test
    void testDefaultMethod() throws Exception {
        String url = "https://www.baidu.com";
        Map<String,String> params = new HashMap<>();
        params.put("url", url);
        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }

    @Test
    void testGet() throws Exception {
        String url = "https://www.baidu.com";
        Map<String,String> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "GET");

        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }

    @Test
    void testPost() throws Exception {
        String url = "https://mock.uutool.cn/4f5qfgcdahj0?test=true";
        Map<String,String> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "POST");
        params.put("mediaType", "application/json");
        params.put("body", JsonUtils.toJSONString(params));

        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }
    
    @Test
    void testPostDefaultJson() throws Exception {
        String url = "https://mock.uutool.cn/4f5qfgcdahj0?test=true";
        Map<String,String> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "POST");
        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }
    
    @Test
    void testPostDefaultWithMediaType() throws Exception {
        String url = "https://mock.uutool.cn/4f5qfgcdahj0?test=true";
        Map<String,String> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "POST");
        params.put("mediaType", "application/json");
        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }

    @Test
    void testTimeout() throws Exception {
        String url = "http://localhost:7700/tmp/sleep";
        Map<String,Object> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "GET");
        params.put("timeout", 20);
        System.out.println(new HttpProcessor().process(TestUtils.genTaskContext(JsonUtils.toJSONString(params))));
    }
}