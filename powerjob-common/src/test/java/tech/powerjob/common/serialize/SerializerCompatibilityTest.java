package tech.powerjob.common.serialize;

import org.junit.jupiter.api.Test;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.InstanceInfoDTO;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Fixtures were generated from commit d92ad662 with Java 8, Jackson 2.14.3 and Kryo 5.3.0.
 */
class SerializerCompatibilityTest {

    @Test
    void retainsLegacyInstanceBinaryAndJson() throws Exception {
        byte[] binary = binary("instance");
        InstanceInfoDTO instance = (InstanceInfoDTO) SerializerUtils.deSerialized(binary);
        assertEquals(9007199254740993L, instance.getInstanceId());
        assertEquals(new Date(1704067200000L), instance.getGmtCreate());
        assertEquals("legacy-java8", instance.getResult());
        assertArrayEquals(binary, SerializerUtils.serialize(instance));
        assertEquals(JsonUtils.parseMap(resource("instance.json")), JsonUtils.parseMap(JsonUtils.toJSONString(instance)));
    }

    @Test
    void retainsDagLargeIdAnnotationAndSharedReferences() throws Exception {
        byte[] binary = binary("dag");
        PEWorkflowDAG dag = (PEWorkflowDAG) SerializerUtils.deSerialized(binary);
        assertSame(dag.getNodes().getFirst(), dag.getNodes().getLast());
        assertEquals(9007199254740993L, dag.getNodes().getFirst().getInstanceId());
        assertArrayEquals(binary, SerializerUtils.serialize(dag));
        assertEquals(JsonUtils.parseMap(resource("dag.json")), JsonUtils.parseMap(JsonUtils.toJSONString(dag)));
    }

    @Test
    void retainsNullAndUnknownFieldHandling() throws Exception {
        String oldJson = resource("instance.json");
        String extended = oldJson.substring(0, oldJson.length() - 1) + ",\"futureField\":true,\"jobId\":null}";
        InstanceInfoDTO instance = JsonUtils.parseObject(extended, InstanceInfoDTO.class);
        assertEquals(JsonUtils.parseMap(oldJson), JsonUtils.parseMap(JsonUtils.toJSONString(instance)));
    }

    @Test
    void preservesCyclicReferences() {
        List<Object> cycle = new ArrayList<>();
        cycle.add(cycle);
        List<?> restored = (List<?>) SerializerUtils.deSerialized(SerializerUtils.serialize(cycle));
        assertSame(restored, restored.getFirst());
    }

    private static byte[] binary(String name) throws IOException {
        return Base64.getDecoder().decode(resource(name + ".base64").trim());
    }

    private static String resource(String name) throws IOException {
        try (InputStream input = Objects.requireNonNull(SerializerCompatibilityTest.class
                .getResourceAsStream("/serialization/java8/" + name))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
