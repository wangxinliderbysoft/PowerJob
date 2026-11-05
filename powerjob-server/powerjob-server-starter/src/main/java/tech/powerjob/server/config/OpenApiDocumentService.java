package tech.powerjob.server.config;

import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.core.converter.ResolvedSchema;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.servers.Server;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Service;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ValueConstants;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import tech.powerjob.server.common.PowerJobServerConfigKey;
import tech.powerjob.server.web.controller.OpenApiController;

import java.lang.reflect.Type;
import java.security.Principal;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Adapts actual Spring MVC mappings to Swagger Core 3 without a Jackson 2 runtime. */
@Service
@ConditionalOnProperty(name = PowerJobServerConfigKey.SWAGGER_UI_ENABLE, havingValue = "true")
public class OpenApiDocumentService {
    private static final String UNBOUNDED_SCHEMA = "x-powerjob-unbounded-schema";
    private final RequestMappingHandlerMapping mappings;
    private final OpenAPI metadata;

    public OpenApiDocumentService(@Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping mappings,
                                  OpenAPI metadata) {
        this.mappings = mappings;
        this.metadata = metadata;
    }

    public OpenAPI document(String contextPath) {
        OpenAPI document = new OpenAPI().openapi("3.0.1").info(metadata.getInfo())
                .servers(List.of(new Server().url(contextPath.isEmpty() ? "/" : contextPath)))
                .paths(new Paths()).components(new Components());
        ModelConverters converters = new ModelConverters();
        converters.addConverter((type, context, chain) -> {
            var javaType = Json.mapper().constructType(type.getType());
            if (javaType.hasRawClass(Object.class) || javaType.hasRawClass(Void.class)) {
                Schema<?> schema = new Schema<>();
                schema.addExtension(UNBOUNDED_SCHEMA, true);
                return schema;
            }
            return chain.hasNext() ? chain.next().resolve(type, context, chain) : null;
        });
        mappings.getHandlerMethods().forEach((mapping, handler) -> {
            Class<?> controller = handler.getBeanType();
            if (controller == OpenApiController.class
                    || !(AnnotatedElementUtils.hasAnnotation(controller, RestController.class)
                    || handler.hasMethodAnnotation(ResponseBody.class))) {
                return;
            }
            Set<RequestMethod> methods = mapping.getMethodsCondition().getMethods();
            if (methods.isEmpty()) {
                methods = EnumSet.allOf(RequestMethod.class);
            }
            for (String path : mapping.getPatternValues()) {
                PathItem item = document.getPaths().computeIfAbsent(path, ignored -> new PathItem());
                for (RequestMethod method : methods) {
                    item.operation(PathItem.HttpMethod.valueOf(method.name()),
                            operation(mapping, handler, document.getComponents(), converters));
                }
            }
        });
        Set<String> operationIds = new HashSet<>();
        document.getPaths().keySet().stream().sorted().forEach(path ->
                document.getPaths().get(path).readOperations().forEach(operation -> {
                    String base = operation.getOperationId();
                    String id = base;
                    int suffix = 1;
                    while (!operationIds.add(id)) {
                        id = base + "_" + suffix++;
                    }
                    operation.setOperationId(id);
                }));
        document.getComponents().getSchemas().values().forEach(this::restoreUnboundedSchemas);
        return document;
    }

    private void restoreUnboundedSchemas(Schema<?> schema) {
        // Swagger copies child schemas and infers object types after conversion; Object allows any JSON value.
        if (schema.getExtensions() != null && schema.getExtensions().remove(UNBOUNDED_SCHEMA) != null) {
            schema.setType(null);
        }
        if (schema.getProperties() != null) {
            schema.getProperties().values().forEach(this::restoreUnboundedSchemas);
        }
        if (schema.getAdditionalProperties() instanceof Schema<?> additional) {
            restoreUnboundedSchemas(additional);
        }
        if (schema.getItems() != null) {
            restoreUnboundedSchemas(schema.getItems());
        }
        if (schema.getAllOf() != null) {
            schema.getAllOf().forEach(this::restoreUnboundedSchemas);
        }
        if (schema.getAnyOf() != null) {
            schema.getAnyOf().forEach(this::restoreUnboundedSchemas);
        }
        if (schema.getOneOf() != null) {
            schema.getOneOf().forEach(this::restoreUnboundedSchemas);
        }
    }

    private Operation operation(RequestMappingInfo mapping, HandlerMethod handler, Components components,
                                ModelConverters converters) {
        Operation operation = new Operation().operationId(handler.getMethod().getName())
                .tags(List.of(handler.getBeanType().getSimpleName().replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase(java.util.Locale.ROOT)));
        for (MethodParameter parameter : handler.getMethodParameters()) {
            Class<?> type = parameter.getParameterType();
            if (ServletRequest.class.isAssignableFrom(type) || ServletResponse.class.isAssignableFrom(type)
                    || Principal.class.isAssignableFrom(type) || Errors.class.isAssignableFrom(type)) {
                continue;
            }
            org.springframework.web.bind.annotation.RequestBody body =
                    parameter.getParameterAnnotation(org.springframework.web.bind.annotation.RequestBody.class);
            Schema schema = schema(GenericTypeResolver.resolveType(parameter.getGenericParameterType(), handler.getBeanType()),
                    parameter.getParameterAnnotations(), components, converters);
            if (body != null) {
                operation.requestBody(new RequestBody().required(body.required())
                        .content(content(mapping.getConsumesCondition().getConsumableMediaTypes(), "application/json", schema)));
                continue;
            }
            RequestPart part = parameter.getParameterAnnotation(RequestPart.class);
            RequestParam query = parameter.getParameterAnnotation(RequestParam.class);
            PathVariable path = parameter.getParameterAnnotation(PathVariable.class);
            RequestHeader header = parameter.getParameterAnnotation(RequestHeader.class);
            CookieValue cookie = parameter.getParameterAnnotation(CookieValue.class);
            String name = parameter.getParameterName();
            boolean required = true;
            String in = "query";
            String defaultValue = ValueConstants.DEFAULT_NONE;
            if (query != null) {
                name = name(query.name(), query.value(), name);
                required = query.required();
                defaultValue = query.defaultValue();
            } else if (path != null) {
                name = name(path.name(), path.value(), name);
                in = "path";
            } else if (header != null) {
                name = name(header.name(), header.value(), name);
                in = "header";
                required = header.required();
                defaultValue = header.defaultValue();
            } else if (cookie != null) {
                name = name(cookie.name(), cookie.value(), name);
                in = "cookie";
                required = cookie.required();
                defaultValue = cookie.defaultValue();
            }
            if (!ValueConstants.DEFAULT_NONE.equals(defaultValue)) {
                schema.setDefault(defaultValue);
                required = false;
            }
            boolean file = MultipartFile.class.isAssignableFrom(type)
                    || type.isArray() && MultipartFile.class.isAssignableFrom(type.getComponentType())
                    || MultipartFile.class.isAssignableFrom(ResolvableType.forMethodParameter(parameter).getGeneric(0).resolve(Object.class));
            if (file || part != null) {
                if (part != null) {
                    name = name(part.name(), part.value(), name);
                    required = part.required();
                }
                if (file) {
                    schema = type == MultipartFile.class ? new StringSchema().format("binary")
                            : new ArraySchema().items(new StringSchema().format("binary"));
                }
                if (operation.getRequestBody() == null) {
                    operation.requestBody(new RequestBody().content(new Content()
                            .addMediaType("multipart/form-data", new MediaType().schema(new ObjectSchema()))));
                }
                Schema form = operation.getRequestBody().getContent().get("multipart/form-data").getSchema();
                form.addProperty(name, schema);
                if (required) {
                    form.addRequiredItem(name);
                    operation.getRequestBody().required(true);
                }
            } else if (query == null && path == null && header == null && cookie == null && !BeanUtils.isSimpleProperty(type)) {
                Schema<?> resolved = schema.get$ref() == null ? schema
                        : components.getSchemas().get(schema.get$ref().substring(schema.get$ref().lastIndexOf('/') + 1));
                if (resolved.getProperties() != null) {
                    resolved.getProperties().forEach((property, value) -> operation.addParametersItem(
                            new Parameter().name(property).in("query").required(false).schema(value)));
                }
            } else {
                operation.addParametersItem(new Parameter().name(name).in(in).required(required && !parameter.isOptional()).schema(schema));
            }
        }
        ResolvableType returnType = ResolvableType.forMethodReturnType(handler.getMethod(), handler.getBeanType());
        if (HttpEntity.class.isAssignableFrom(returnType.resolve(Object.class))) {
            returnType = returnType.getGeneric(0);
        }
        ApiResponse response = new ApiResponse().description("OK");
        if (returnType.resolve() != void.class && returnType.resolve() != Void.class) {
            Type type = GenericTypeResolver.resolveType(returnType.getType(), handler.getBeanType());
            Schema schema = returnType.resolve() == byte[].class || Resource.class.isAssignableFrom(returnType.resolve(Object.class))
                    ? new StringSchema().format("binary") : schema(type, handler.getMethod().getAnnotations(), components, converters);
            response.content(content(mapping.getProducesCondition().getProducibleMediaTypes(), "*/*", schema));
        }
        ResponseStatus status = handler.getMethodAnnotation(ResponseStatus.class);
        operation.responses(new ApiResponses().addApiResponse(status == null ? "200" : Integer.toString(status.code().value()), response));
        return operation;
    }

    private Schema schema(Type type, java.lang.annotation.Annotation[] annotations, Components components,
                          ModelConverters converters) {
        ResolvedSchema resolved = converters.resolveAsResolvedSchema(new AnnotatedType(type)
                .ctxAnnotations(annotations).resolveAsRef(true));
        if (resolved.referencedSchemas != null) {
            resolved.referencedSchemas.forEach(components::addSchemas);
        }
        if (resolved.schema == null) {
            throw new IllegalStateException("Unable to describe OpenAPI type " + type.getTypeName());
        }
        restoreUnboundedSchemas(resolved.schema);
        return resolved.schema;
    }

    private static Content content(Set<org.springframework.http.MediaType> mediaTypes, String fallback, Schema schema) {
        Content content = new Content();
        if (mediaTypes.isEmpty()) {
            content.addMediaType(fallback, new MediaType().schema(schema));
        } else {
            mediaTypes.forEach(type -> content.addMediaType(type.toString(), new MediaType().schema(schema)));
        }
        return content;
    }

    private static String name(String name, String value, String fallback) {
        if (!name.isEmpty()) {
            return name;
        }
        if (!value.isEmpty()) {
            return value;
        }
        if (fallback == null) {
            throw new IllegalStateException("OpenAPI requires compiler parameter metadata");
        }
        return fallback;
    }
}
