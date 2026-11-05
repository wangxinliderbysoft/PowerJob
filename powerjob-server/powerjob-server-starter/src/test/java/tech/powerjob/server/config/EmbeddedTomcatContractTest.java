package tech.powerjob.server.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.servlet.autoconfigure.MultipartAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import tech.powerjob.common.response.InstanceInfoDTO;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.server.auth.interceptor.PowerJobAuthInterceptor;
import tech.powerjob.server.core.container.ContainerService;
import tech.powerjob.server.openapi.OpenApiInterceptor;
import tech.powerjob.server.web.websocket.ContainerDeployServerEndpoint;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies real servlet conversion, binding, filters, uploads and WebSocket upgrade on Tomcat.
 */
class EmbeddedTomcatContractTest {

    @Test
    void preservesHttpAndWebSocketBoundaries() throws Exception {
        PowerJobAuthInterceptor auth = mock(PowerJobAuthInterceptor.class);
        when(auth.preHandle(any(), any(), any())).thenReturn(true);
        SpringApplication application = new SpringApplication(TestApplication.class);
        application.addInitializers(context -> {
            context.getBeanFactory().registerSingleton("powerJobAuthInterceptor", auth);
            context.getBeanFactory().registerSingleton("openApiInterceptor", mock(OpenApiInterceptor.class));
            context.getBeanFactory().registerSingleton("containerService", mock(ContainerService.class));
        });
        try (ConfigurableApplicationContext context = application.run(
                "--server.port=0", "--spring.profiles.active=embedded-contract");
             HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            URI base = URI.create("http://127.0.0.1:" + port);
            assertTrue(context.getBean(RequestMappingHandlerAdapter.class).getMessageConverters().stream()
                    .anyMatch(JacksonJsonHttpMessageConverter.class::isInstance));

            HttpResponse<String> dag = client.send(HttpRequest.newBuilder(base.resolve("/contract/dag")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, dag.statusCode());
            assertTrue(dag.body().contains("\"instanceId\":\"9007199254740993\""));

            HttpResponse<String> enumResponse = client.send(HttpRequest.newBuilder(base.resolve("/contract/enum"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("\"API\"")).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, enumResponse.statusCode());
            assertEquals("\"API\"", enumResponse.body());

            HttpResponse<String> form = client.send(HttpRequest.newBuilder(base.resolve("/contract/form"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Origin", "https://example.invalid")
                    .POST(HttpRequest.BodyPublishers.ofString("instanceId=9007199254740993")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, form.statusCode());
            assertTrue(form.body().contains("\"instanceId\":9007199254740993"));
            assertTrue(form.body().contains("\"success\":true"));
            assertEquals("*", form.headers().firstValue("Access-Control-Allow-Origin").orElseThrow());

            String multipart = "--fixture\r\nContent-Disposition: form-data; name=\"file\"; filename=\"job.jar\"\r\n"
                    + "Content-Type: application/octet-stream\r\n\r\nprocessor-bytes\r\n--fixture--\r\n";
            HttpResponse<String> upload = client.send(HttpRequest.newBuilder(base.resolve("/contract/upload"))
                    .header("Content-Type", "multipart/form-data; boundary=fixture")
                    .POST(HttpRequest.BodyPublishers.ofString(multipart)).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, upload.statusCode());
            assertEquals("processor-bytes", upload.body());

            HttpResponse<byte[]> download = client.send(HttpRequest.newBuilder(base.resolve("/contract/download"))
                    .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, download.statusCode());
            assertEquals("application/octet-stream", download.headers().firstValue("Content-Type").orElseThrow());
            assertEquals("processor-bytes", new String(download.body(), java.nio.charset.StandardCharsets.UTF_8));
            verify(context.getBean(PowerJobAuthInterceptor.class), org.mockito.Mockito.atLeastOnce())
                    .preHandle(any(), any(), any());

            CompletableFuture<String> message = new CompletableFuture<>();
            CompletableFuture<Integer> closed = new CompletableFuture<>();
            WebSocket socket = client.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
                    .buildAsync(URI.create("ws://127.0.0.1:" + port + "/container/deploy/123"), new WebSocket.Listener() {
                        @Override
                        public java.util.concurrent.CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                            message.complete(data.toString());
                            webSocket.request(1);
                            return null;
                        }

                        @Override
                        public java.util.concurrent.CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                            closed.complete(statusCode);
                            return null;
                        }
                    }).get(5, TimeUnit.SECONDS);
            try {
                assertTrue(message.get(5, TimeUnit.SECONDS).contains("connected successfully"));
                assertEquals(WebSocket.NORMAL_CLOSURE, closed.get(5, TimeUnit.SECONDS));
                verify(context.getBean(ContainerService.class)).deploy(org.mockito.ArgumentMatchers.eq(123L), any());
            } finally {
                if (!socket.isInputClosed()) {
                    socket.abort();
                }
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({TomcatServletWebServerAutoConfiguration.class, DispatcherServletAutoConfiguration.class,
            WebMvcAutoConfiguration.class, MultipartAutoConfiguration.class, JacksonAutoConfiguration.class,
            HttpMessageConvertersAutoConfiguration.class})
    @Import({WebConfig.class, CachingRequestBodyFilter.class, OmsEndpointConfigure.class,
            ContainerDeployServerEndpoint.class, ContractController.class})
    static class TestApplication {
    }

    @RestController
    static class ContractController {
        @PostMapping("/contract/enum")
        tech.powerjob.common.enums.TimeExpressionType enumeration(
                @org.springframework.web.bind.annotation.RequestBody tech.powerjob.common.enums.TimeExpressionType value) {
            return value;
        }

        @GetMapping("/contract/dag")
        PEWorkflowDAG.Node dag() {
            PEWorkflowDAG.Node node = new PEWorkflowDAG.Node(1L);
            node.setInstanceId(9007199254740993L);
            return node;
        }

        @PostMapping("/contract/form")
        ResultDTO<InstanceInfoDTO> form(Long instanceId) {
            InstanceInfoDTO instance = new InstanceInfoDTO();
            instance.setInstanceId(instanceId);
            return ResultDTO.success(instance);
        }

        @PostMapping("/contract/upload")
        byte[] upload(@RequestParam MultipartFile file) throws java.io.IOException {
            return file.getBytes();
        }

        @GetMapping("/contract/download")
        ResponseEntity<byte[]> download() {
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body("processor-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}
