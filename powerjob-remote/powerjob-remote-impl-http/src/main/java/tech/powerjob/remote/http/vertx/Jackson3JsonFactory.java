package tech.powerjob.remote.http.vertx;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.JsonFactory;
import io.vertx.core.spi.json.JsonCodec;
import tools.jackson.core.JacksonException;
import tools.jackson.core.Base64Variants;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Supplies the HTTP transport's Jackson 3 codec through Vert.x's supported discovery SPI. */
public final class Jackson3JsonFactory implements JsonFactory, JsonCodec {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<Object>> LIST_TYPE = new TypeReference<>() {};
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .configureForJackson2()
            .defaultBase64Variant(Base64Variants.MODIFIED_FOR_URL)
            .configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true)
            .configure(JsonReadFeature.ALLOW_SINGLE_QUOTES, true)
            .configure(StreamReadFeature.IGNORE_UNDEFINED, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
            .changeDefaultPropertyInclusion(inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
            .addModule(new SimpleModule("PowerJobVertx")
                    .addSerializer(JsonObject.class, unwrapping(JsonObject::getMap))
                    .addSerializer(JsonArray.class, unwrapping(JsonArray::getList))
                    .addSerializer(Buffer.class, unwrapping(buffer -> encodeBinary(buffer.getBytes())))
                    .addSerializer(byte[].class, unwrapping(Jackson3JsonFactory::encodeBinary))
                    .addSerializer(Instant.class, unwrapping(Instant::toString)))
            .build();

    @Override
    public int order() {
        return 0;
    }

    @Override
    public JsonCodec codec() {
        return this;
    }

    @Override
    public <T> T fromString(String json, Class<T> type) {
        try {
            if (type == JsonObject.class) {
                return type.cast(new JsonObject(MAPPER.readValue(json, MAP_TYPE)));
            }
            if (type == JsonArray.class) {
                return type.cast(new JsonArray(MAPPER.readValue(json, LIST_TYPE)));
            }
            return MAPPER.readValue(json, type);
        } catch (JacksonException | IllegalArgumentException failure) {
            throw new DecodeException("Unable to decode HTTP transport JSON", failure);
        }
    }

    @Override
    public <T> T fromBuffer(Buffer json, Class<T> type) {
        return fromString(json.toString(), type);
    }

    @Override
    public <T> T fromValue(Object value, Class<T> type) {
        Object unwrapped = value instanceof JsonObject object ? object.getMap()
                : value instanceof JsonArray array ? array.getList() : value;
        try {
            if (type == JsonObject.class) {
                return type.cast(new JsonObject(MAPPER.convertValue(unwrapped, MAP_TYPE)));
            }
            if (type == JsonArray.class) {
                return type.cast(new JsonArray(MAPPER.convertValue(unwrapped, LIST_TYPE)));
            }
            return MAPPER.convertValue(unwrapped, type);
        } catch (JacksonException | IllegalArgumentException failure) {
            throw new DecodeException("Unable to map HTTP transport JSON", failure);
        }
    }

    @Override
    public String toString(Object value, boolean pretty) {
        try {
            return (pretty ? MAPPER.writerWithDefaultPrettyPrinter() : MAPPER.writer()).writeValueAsString(value);
        } catch (JacksonException | IllegalArgumentException failure) {
            throw new EncodeException("Unable to encode HTTP transport JSON", failure);
        }
    }

    @Override
    public Buffer toBuffer(Object value, boolean pretty) {
        return Buffer.buffer(toString(value, pretty));
    }

    private static String encodeBinary(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static <T> ValueSerializer<T> unwrapping(Function<T, Object> unwrap) {
        return new ValueSerializer<>() {
            @Override
            public void serialize(T value, JsonGenerator generator, SerializationContext context) {
                context.writeValue(generator, unwrap.apply(value));
            }
        };
    }
}
