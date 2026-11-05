package tech.powerjob.official.processors.impl;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.TestUtils;
import tech.powerjob.worker.core.processor.ProcessResult;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HttpProcessorJsonTest {

    @Test
    void preservesJsonInferenceExplicitPlainTextAndEmptyBodyDefaults() throws Exception {
        LinkedBlockingQueue<List<String>> requests = new LinkedBlockingQueue<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            List<String> request = new ArrayList<>();
            request.add(exchange.getRequestHeaders().getFirst("Content-Type"));
            request.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            requests.add(request);
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
            for (String body : List.of("{\"nested\":[null,\"中文\"]}", "[1,true]", "\"quoted\"", "true", "null", "12")) {
                assertRequest(url, body, null, "application/json; charset=utf-8", body, requests);
            }
            assertRequest(url, null, null, "application/json; charset=utf-8", "{}", requests);
            assertRequest(url, "", null, "application/json; charset=utf-8", "{}", requests);
            assertRequest(url, "plain text", "text/plain", "text/plain; charset=utf-8", "plain text", requests);
            assertRequest(url, "{} trailing", "text/plain", "text/plain; charset=utf-8", "{} trailing", requests);
            assertRequest(url, "{\"json\":true}", "text/plain", "text/plain; charset=utf-8", "{\"json\":true}", requests);
        } finally {
            server.stop(0);
        }
    }

    private void assertRequest(String url, String body, String mediaType, String expectedType, String expectedBody,
                               LinkedBlockingQueue<List<String>> requests) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("url", url);
        params.put("method", "POST");
        params.put("timeout", 5);
        params.put("body", body);
        params.put("mediaType", mediaType);
        ProcessResult result = new HttpProcessor().process0(TestUtils.genTaskContext(JsonUtils.toJSONStringUnsafe(params)));
        assertTrue(result.isSuccess(), result.getMsg());
        List<String> request = requests.poll(5, TimeUnit.SECONDS);
        assertEquals(List.of(expectedType, expectedBody), request);
    }
}
