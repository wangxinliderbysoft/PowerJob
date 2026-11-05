package tech.powerjob.server.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.server.common.module.ServerInfo;
import tech.powerjob.server.core.instance.InstanceService;
import tech.powerjob.server.core.service.AppInfoService;
import tech.powerjob.server.core.service.CacheService;
import tech.powerjob.server.core.service.JobService;
import tech.powerjob.server.core.workflow.WorkflowInstanceService;
import tech.powerjob.server.core.workflow.WorkflowService;
import tech.powerjob.server.openapi.OpenAPIController;
import tech.powerjob.server.openapi.security.OpenApiSecurityService;
import tech.powerjob.server.remote.server.self.ServerInfoService;
import tech.powerjob.server.web.controller.OpenApiController;
import tech.powerjob.common.response.ResultDTO;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises dynamic documentation for actual PowerJob endpoints without connecting services. */
class OpenApiDocumentationTest {

    @Test
    void coversAllRealControllersWithUniqueOperationIds() throws Exception {
        try (var context = new org.springframework.web.context.support.StaticWebApplicationContext()) {
            MappingIntrospector mappings = new MappingIntrospector();
            mappings.setApplicationContext(context);
            mappings.afterPropertiesSet();
            var scanner = new org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider(false);
            scanner.addIncludeFilter(new org.springframework.core.type.filter.AnnotationTypeFilter(RestController.class));
            var controllers = new java.util.HashSet<>(scanner.findCandidateComponents("tech.powerjob.server.web.controller"));
            controllers.addAll(scanner.findCandidateComponents("tech.powerjob.server.openapi"));
            for (var bean : controllers) {
                Class<?> controller = Class.forName(bean.getBeanClassName());
                Object instance = mock(controller);
                for (var method : controller.getDeclaredMethods()) {
                    var mapping = mappings.mapping(method, controller);
                    if (mapping != null) {
                        mappings.registerMapping(mapping, instance, method);
                    }
                }
            }
            var document = new OpenApiDocumentService(mappings, new io.swagger.v3.oas.models.OpenAPI()).document("");
            assertTrue(document.getPaths().size() > 70);
            assertTrue(document.getPaths().containsKey("/openApi/saveJob"));
            var ids = new java.util.HashSet<String>();
            document.getPaths().values().forEach(path -> path.readOperations().forEach(operation ->
                    assertTrue(ids.add(operation.getOperationId()), "Duplicate operation ID: " + operation.getOperationId())));
            assertFalse(io.swagger.v3.core.util.Json.mapper().writeValueAsString(document).contains("x-powerjob-unbounded-schema"));
        }
    }

    @Test
    void preservesActualControllerDocumentsAndUiUnderContextPath() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplication(TestApplication.class).run(
                "--spring.config.location=optional:classpath:/documentation-test.properties",
                "--server.port=0", "--server.servlet.context-path=/fixture",
                "--oms.swagger.enable=true");
             HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            String base = "http://127.0.0.1:" + port + "/fixture";
            String document = get(client, base + "/v3/api-docs/PowerJob-ALL", 200);
            Map<String, Object> json = JsonUtils.parseMap(document);
            assertEquals("PowerJob", ((Map<?, ?>) json.get("info")).get("title"));
            Map<?, ?> paths = (Map<?, ?>) json.get("paths");
            assertTrue(paths.containsKey("/openApi/fetchAllJob"));
            assertTrue(paths.containsKey("/openApi/saveJob"));
            Map<?, ?> save = (Map<?, ?>) ((Map<?, ?>) paths.get("/openApi/saveJob")).get("post");
            assertEquals(true, ((Map<?, ?>) save.get("requestBody")).get("required"));
            Map<?, ?> schemas = (Map<?, ?>) ((Map<?, ?>) json.get("components")).get("schemas");
            assertTrue(schemas.containsKey("SaveJobInfoRequest"));
            Map<?, ?> authProperties = (Map<?, ?>) ((Map<?, ?>) schemas.get("AppAuthRequest")).get("properties");
            assertEquals(Map.of(), ((Map<?, ?>) authProperties.get("extra")).get("additionalProperties"));
            Map<?, ?> voidProperties = (Map<?, ?>) ((Map<?, ?>) schemas.get("ResultDTOVoid")).get("properties");
            assertEquals(Map.of(), voidProperties.get("data"));
            Map<?, ?> workflowProperties = (Map<?, ?>) ((Map<?, ?>) schemas.get("WorkflowInfoVO")).get("properties");
            assertTrue(workflowProperties.containsKey("peworkflowDAG"));
            assertFalse(workflowProperties.containsKey("PEWorkflowDAG"));
            assertFalse(workflowProperties.containsKey("pEWorkflowDAG"));
            Map<?, ?> dagSchema = (Map<?, ?>) schemas.get("PEWorkflowDAG");
            assertNull(dagSchema.get("required"));
            Map<?, ?> dagProperties = (Map<?, ?>) dagSchema.get("properties");
            assertNull(((Map<?, ?>) dagProperties.get("edges")).get("nullable"));
            assertTrue(schemas.toString().contains("jobName"));
            assertTrue(schemas.toString().contains("instanceId"));
            assertTrue(get(client, base + "/v3/api-docs", 200).contains("PowerJob"));
            assertTrue(get(client, base + "/v3/api-docs.yaml/PowerJob-ALL", 200).contains("openapi:"));
            assertTrue(get(client, base + "/v3/api-docs/swagger-config", 200).contains("PowerJob-ALL"));
            assertTrue(get(client, base + "/swagger-ui/index.html", 200).contains("swagger-ui"));
            assertTrue(get(client, base + "/swagger-ui/swagger-initializer.js", 200).contains("/fixture/v3/api-docs/swagger-config"));
            assertTrue(get(client, base + "/swagger-ui/swagger-ui-bundle.js", 200).contains("SwaggerUIBundle"));
            Map<?, ?> upload = (Map<?, ?>) ((Map<?, ?>) paths.get("/documentation-fixture/upload")).get("post");
            Map<?, ?> requestContent = (Map<?, ?>) ((Map<?, ?>) upload.get("requestBody")).get("content");
            Map<?, ?> uploadSchema = (Map<?, ?>) ((Map<?, ?>) requestContent.get("multipart/form-data")).get("schema");
            Map<?, ?> properties = (Map<?, ?>) uploadSchema.get("properties");
            assertEquals("binary", ((Map<?, ?>) properties.get("file")).get("format"));
            assertTrue(properties.containsKey("metadata"));
            Map<?, ?> query = (Map<?, ?>) ((Map<?, ?>) paths.get("/documentation-fixture/{id}")).get("get");
            java.util.List<?> parameters = (java.util.List<?>) query.get("parameters");
            Map<?, ?> limit = parameters.stream().map(value -> (Map<?, ?>) value)
                    .filter(value -> value.get("name").equals("limit")).findFirst().orElseThrow();
            assertEquals(7, ((Map<?, ?>) limit.get("schema")).get("default"));
            assertEquals(false, limit.get("required"));
            assertThrows(ClassNotFoundException.class, () -> Class.forName("com.fasterxml.jackson.databind.ObjectMapper"));
            String capture = System.getProperty("powerjob.openapi.capture");
            if (capture != null) {
                Files.writeString(Path.of(capture), document);
            }
        }
    }

    @Test
    void disabledDocumentationDoesNotExposeDocumentsOrUi() throws Exception {
        try (ConfigurableApplicationContext context = new SpringApplication(TestApplication.class).run(
                "--spring.config.location=optional:classpath:/documentation-test.properties",
                "--server.port=0", "--oms.swagger.enable=false");
             HttpClient client = HttpClient.newHttpClient()) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            String base = "http://127.0.0.1:" + port;
            get(client, base + "/v3/api-docs", 404);
            get(client, base + "/swagger-ui/index.html", 404);
        }
    }

    private static String get(HttpClient client, String url, int expectedStatus) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(expectedStatus, response.statusCode(), url + ": " + response.body());
        return response.body();
    }

    static class MappingIntrospector extends org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping {
        org.springframework.web.servlet.mvc.method.RequestMappingInfo mapping(java.lang.reflect.Method method, Class<?> type) {
            return getMappingForMethod(method, type);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({TomcatServletWebServerAutoConfiguration.class, DispatcherServletAutoConfiguration.class,
            WebMvcAutoConfiguration.class, JacksonAutoConfiguration.class,
            HttpMessageConvertersAutoConfiguration.class})
    @Import({SwaggerConfig.class, OpenApiDocumentService.class, OpenApiController.class, TestApplication.DocumentationFixtureController.class})
    static class TestApplication {
        @Bean
        org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping controllerEndpointHandlerMapping() {
            return new org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping();
        }

        @Bean
        ServerInfoService serverInfoService() {
            ServerInfoService service = mock(ServerInfoService.class);
            ServerInfo info = new ServerInfo();
            info.setVersion("fixture");
            when(service.fetchCurrentServerInfo()).thenReturn(info);
            return service;
        }

        @Bean
        OpenAPIController openAPIController() {
            return new OpenAPIController(mock(AppInfoService.class), mock(JobService.class),
                    mock(InstanceService.class), mock(WorkflowService.class), mock(WorkflowInstanceService.class),
                    mock(OpenApiSecurityService.class), mock(CacheService.class));
        }

        @RestController
        static class DocumentationFixtureController {
            @PostMapping(value = "/documentation-fixture/upload", consumes = "multipart/form-data")
            ResultDTO<Long> upload(@RequestPart MultipartFile file, @RequestPart Payload metadata) {
                return ResultDTO.success(1L);
            }

            @GetMapping("/documentation-fixture/{id}")
            ResultDTO<Long> query(@PathVariable Long id, @RequestParam(defaultValue = "7") Integer limit) {
                return ResultDTO.success(id);
            }
        }

        record Payload(String value) {
        }
    }
}
