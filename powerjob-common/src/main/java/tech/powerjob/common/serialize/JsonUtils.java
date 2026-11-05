package tech.powerjob.common.serialize;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.SneakyThrows;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import tech.powerjob.common.exception.ImpossibleException;
import tech.powerjob.common.exception.PowerJobException;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON工具类
 *
 * @author tjq
 * @since 2020/4/16
 */
@Slf4j
public class JsonUtils {

    private static final JsonMapper JSON_MAPPER = JsonMapper.builder()
            .configureForJackson2()
            .configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true)
            .configure(JsonReadFeature.ALLOW_SINGLE_QUOTES, true)
            .configure(StreamReadFeature.IGNORE_UNDEFINED, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
            .configure(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS, true)
            .changeDefaultPropertyInclusion(inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
            .build();

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };
    private static final JsonFactory JSON_VALIDATOR_FACTORY = JsonFactory.builder().build();

    private JsonUtils() {

    }

    public static String toJSONString(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return (String) obj;
        }
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("[PowerJob] toJSONString failed", e);
        }
        return null;
    }

    public static String toJSONStringUnsafe(Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        }
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        throw new ImpossibleException();
    }

    public static byte[] toBytes(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            log.error("[PowerJob] serialize failed", e);
        }
        return null;
    }

    public static <T> T parseObject(String json, Class<T> clz) throws Exception {
        return JSON_MAPPER.readValue(json, clz);
    }

    public static Map<String, Object> parseMap(String json) {
        if (StringUtils.isEmpty(json)) {
            return new HashMap<>();
        }
        try {
            return JSON_MAPPER.readValue(json, MAP_TYPE_REFERENCE);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        throw new ImpossibleException();
    }

    public static <T> T parseObject(byte[] b, Class<T> clz) throws IOException {
        return JSON_MAPPER.readValue(b, clz);
    }

    public static <T> T parseObject(byte[] b, TypeReference<T> typeReference) throws IOException {
        return JSON_MAPPER.readValue(b, typeReference);
    }

    @SneakyThrows
    public static <T> T parseObject(String json, TypeReference<T> typeReference) throws IOException {
        return JSON_MAPPER.readValue(json, typeReference);
    }

    public static <T> T parseObjectIgnoreException(String json, Class<T> clz) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        try {
            return JSON_MAPPER.readValue(json, clz);
        } catch (Exception e) {
            log.error("unable to parse json string to object,current string:{}", json, e);
            return null;
        }

    }

    public static <T> T parseObjectUnsafe(String json, Class<T> clz) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return JSON_MAPPER.readValue(json, clz);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
        throw new PowerJobException("impossible");
    }

    public static <T> T parseObjectUnsafe(String json, TypeReference<T> typeReference) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        return JSON_MAPPER.readerFor(typeReference)
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .readValue(json);
    }

    public static Map<String, String> parseStringMap(String json) {
        Map<String, Object> values = parseObjectUnsafe(json, MAP_TYPE_REFERENCE);
        if (values == null) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>();
        values.forEach((key, value) -> result.put(key, value == null ? null : toJSONStringUnsafe(value)));
        return result;
    }

    public static boolean isValidJson(String json) {
        if (StringUtils.isBlank(json)) {
            return false;
        }
        try (var parser = JSON_VALIDATOR_FACTORY.createParser(json)) {
            if (parser.nextToken() == null) {
                return false;
            }
            parser.skipChildren();
            return parser.nextToken() == null;
        } catch (JacksonException e) {
            return false;
        }
    }

    public static <T> T toJavaObject(Object o, Class<T> clz) {
        if (o == null) {
            return null;
        }
        try {
            return JSON_MAPPER.convertValue(o, clz);
        } catch (JacksonException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }


    /**
     * Serializes an object to its JSON string representation.
     */
    @SneakyThrows
    public static String toJson(Object object) {
        return JSON_MAPPER.writeValueAsString(object);
    }

    /**
     * Deserializes a JSON string to an object of the specified class.
     */
    @SneakyThrows
    public static <T> T fromJson(String json, Class<T> clazz) {
        return JSON_MAPPER.readValue(json, clazz);
    }

    public static <T> T copy(T source) {
        String json = toJson(source);
        return (T) fromJson(json, source.getClass());
    }

    @SneakyThrows
    public static <T> List<T> fromJson2List(String json, Class<T> elementClass) {
        JavaType javaType = JSON_MAPPER.getTypeFactory().constructParametricType(List.class, elementClass);
        return JSON_MAPPER.readValue(json, javaType);
    }
}
