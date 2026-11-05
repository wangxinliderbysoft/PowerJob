package tech.powerjob.server.web.controller;

import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.powerjob.server.common.PowerJobServerConfigKey;
import tech.powerjob.server.config.OpenApiDocumentService;

import java.net.URI;
import java.util.List;
import java.util.Map;

/** Preserves documentation and UI URLs while serializing models with Swagger's Jackson 3 mapper. */
@RestController
@ConditionalOnProperty(name = PowerJobServerConfigKey.SWAGGER_UI_ENABLE, havingValue = "true")
@RequiredArgsConstructor
public class OpenApiController {
    private final OpenApiDocumentService documents;

    @GetMapping(value = {"/v3/api-docs", "/v3/api-docs/PowerJob-ALL"}, produces = "application/json")
    public String json(HttpServletRequest request) {
        return Json.mapper().writeValueAsString(documents.document(request.getContextPath()));
    }

    @GetMapping(value = {"/v3/api-docs.yaml", "/v3/api-docs.yaml/PowerJob-ALL"}, produces = "application/vnd.oai.openapi")
    public String yaml(HttpServletRequest request) {
        return Yaml.mapper().writeValueAsString(documents.document(request.getContextPath()));
    }

    @GetMapping(value = "/v3/api-docs/swagger-config", produces = "application/json")
    public Map<String, Object> configuration(HttpServletRequest request) {
        String context = request.getContextPath();
        return Map.of("configUrl", context + "/v3/api-docs/swagger-config",
                "oauth2RedirectUrl", context + "/swagger-ui/oauth2-redirect.html",
                "urls", List.of(Map.of("name", "PowerJob-ALL", "url", context + "/v3/api-docs/PowerJob-ALL")),
                "validatorUrl", "");
    }

    @GetMapping(value = "/swagger-ui/swagger-initializer.js", produces = "application/javascript")
    public String initializer(HttpServletRequest request) {
        String configUrl = Json.mapper().writeValueAsString(request.getContextPath() + "/v3/api-docs/swagger-config");
        return "window.onload = function() { window.ui = SwaggerUIBundle({configUrl: " + configUrl
                + ", dom_id: '#swagger-ui', deepLinking: true, presets: [SwaggerUIBundle.presets.apis, "
                + "SwaggerUIStandalonePreset], layout: 'StandaloneLayout'}); };";
    }

    @GetMapping("/swagger-ui.html")
    public ResponseEntity<Void> redirect(HttpServletRequest request) {
        return ResponseEntity.status(302).location(URI.create(request.getContextPath() + "/swagger-ui/index.html")).build();
    }
}
