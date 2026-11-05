package tech.powerjob.server.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import tech.powerjob.server.common.PowerJobServerConfigKey;
import tech.powerjob.server.remote.server.self.ServerInfoService;

/**
 * Configuration class for Swagger UI.
 * Uses Swagger Core's Jackson 3 schema engine and the existing Swagger UI WebJar.
 * <a href="http://localhost:7700/swagger-ui/index.html#/">api address</a>
 *
 * @author tjq
 * @author Jiang Jining
 * @since 2020/3/29
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = PowerJobServerConfigKey.SWAGGER_UI_ENABLE, havingValue = "true")
@RequiredArgsConstructor
public class SwaggerConfig implements WebMvcConfigurer {
    
    private final ServerInfoService serverInfoService;

    @Bean
    public OpenAPI springShopOpenAPI() {
        final Contact contact = new Contact();
        contact.setName("Team PowerJob");
        contact.setUrl("http://www.powerjob.tech");
        contact.setEmail("tengjiqi@gmail.com");

        // apiInfo()用来创建该Api的基本信息（这些基本信息会展现在文档页面中
        return new OpenAPI()
                .info(new Info().title("PowerJob")
                        .description("Distributed scheduling and computing framework.")
                        .version(serverInfoService.fetchCurrentServerInfo().getVersion())
                        .contact(contact)
                        .license(new License().name("Apache License 2.0").url("https://github.com/PowerJob/PowerJob/blob/master/LICENSE")));
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/swagger-ui/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui/5.32.2/");
    }
    
}
