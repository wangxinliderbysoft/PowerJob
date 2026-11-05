package tech.powerjob.remote.http;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/** Pins the HTTP transport JSON boundary, including nested Vert.x values. */
class Jackson3JsonFactoryTest {
    @Test
    void roundTripsUrlSafeBinaryPojoFields() {
        Payload input = new Payload();
        input.bytes = new byte[]{(byte) 255, (byte) 255};
        String json = Json.encode(input);
        assertTrue(json.contains("\"bytes\":\"__8\""));
        assertArrayEquals(input.bytes, Json.decodeValue(json, Payload.class).bytes);
        assertArrayEquals(input.bytes, Json.decodeValue("\"__8\"", byte[].class));
    }

    @Test
    void discoversJackson3BeforeFirstVertxJsonUse() {
        assertTrue(Json.CODEC.getClass().getName().contains("Jackson3JsonFactory"), Json.CODEC.getClass().getName());
        JsonObject input = new JsonObject().put("nested", new JsonArray().add(new JsonObject().put("id", 9007199254740993L)))
                .put("bytes", Buffer.buffer(new byte[]{(byte) 255, (byte) 255}))
                .put("instant", Instant.parse("2024-01-01T00:00:00Z"));
        JsonObject output = input.toBuffer().toJsonObject();
        assertEquals(9007199254740993L, output.getJsonArray("nested").getJsonObject(0).getLong("id"));
        assertArrayEquals(new byte[]{(byte) 255, (byte) 255}, output.getBinary("bytes"));
        assertEquals("2024-01-01T00:00:00Z", output.getString("instant"));
        assertEquals("__8", output.getString("bytes"));
    }

    @Test
    void mapsPojoAndIgnoresNewPeerFields() {
        Payload input = new Payload();
        input.id = 9007199254740993L;
        JsonObject json = JsonObject.mapFrom(input);
        assertFalse(json.containsKey("optional"));
        assertFalse(json.containsKey("hidden"));
        json.put("futureField", true);
        assertEquals(input.id, json.mapTo(Payload.class).id);
        assertEquals(18L, Json.decodeValue("{'id':18}", Payload.class).id);
        DecodeException exception = assertThrows(DecodeException.class, () -> Json.decodeValue("{", Payload.class));
        assertTrue(exception.getCause().getClass().getName().startsWith("tools.jackson."));
    }

    public static class Payload {
        public long id;
        public String optional;
        public byte[] bytes;
        private transient String hidden = "do-not-send";

        public String getHidden() {
            return hidden;
        }
    }
}
