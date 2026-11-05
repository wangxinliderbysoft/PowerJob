package tech.powerjob.common.serialize;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import tools.jackson.core.type.TypeReference;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import static org.junit.jupiter.api.Assertions.*;

/**
 * test json utils
 *
 * @author tjq
 * @since 2024/3/16
 */
@Slf4j
class JsonUtilsTest {

    @Test
    void parsesGenericResponsesWithoutLosingLargeIdentifiers() {
        var result = JsonUtils.parseObjectUnsafe(
                "{'success':true,'data':[{'id':9007199254740993,'nodeName':'legacy','future':true}]}",
                new TypeReference<ResultDTO<List<WorkflowNodeInfoDTO>>>() {});
        assertTrue(result.isSuccess());
        assertEquals(9007199254740993L, result.getData().getFirst().getId());
        assertEquals("legacy", result.getData().getFirst().getNodeAlias());
        assertNull(JsonUtils.parseObjectUnsafe(null, new TypeReference<List<String>>() {}));
        assertNull(JsonUtils.parseObjectUnsafe("  ", new TypeReference<List<String>>() {}));
        assertNull(JsonUtils.parseObjectUnsafe("  ", Person.class));
        assertThrows(Exception.class, () -> JsonUtils.parseObjectUnsafe("{", new TypeReference<List<String>>() {}));
    }

    @Test
    void validatesExactlyOneJsonValue() {
        for (String json : List.of("{}", "[]", "{\"nested\":[null,true,1,\"value\"]}", "\"text\"", "42", "true", "null")) {
            assertTrue(JsonUtils.isValidJson(json), json);
        }
        for (String json : List.of("", " ", "{", "{\"x\":}", "{} {}", "null trailing", "{'x':1}")) {
            assertFalse(JsonUtils.isValidJson(json), json);
        }
        assertFalse(JsonUtils.isValidJson(null));
    }

    @Test
    void readsHistoricalWorkflowDagWithNumericAndStringInstanceIds() {
        for (String id : List.of("9007199254740993", "\"9007199254740993\"")) {
            PEWorkflowDAG dag = JsonUtils.parseObjectUnsafe(
                    "{\"nodes\":[{\"nodeId\":7,\"instanceId\":" + id
                            + ",\"enable\":false,\"future\":true}],\"edges\":[]}", PEWorkflowDAG.class);
            assertEquals(9007199254740993L, dag.getNodes().getFirst().getInstanceId());
            assertFalse(dag.getNodes().getFirst().getEnable());
            PEWorkflowDAG restored = JsonUtils.parseObjectUnsafe(JsonUtils.toJSONStringUnsafe(dag), PEWorkflowDAG.class);
            assertEquals(dag, restored);
        }
    }

    @Test
    void preservesStructuredAndScalarWorkflowContextStrings() {
        Map<String, String> context = JsonUtils.parseStringMap(
                "{\"message\":\"quoted \\\"value\\\"\",\"flag\":true,\"id\":9007199254740993,"
                        + "\"nested\":{\"amount\":0.123456789012345678901234},\"values\":[1,null,2],\"absent\":null}");
        assertEquals("quoted \"value\"", context.get("message"));
        assertEquals("true", context.get("flag"));
        assertEquals("9007199254740993", context.get("id"));
        assertEquals("{\"amount\":0.123456789012345678901234}", context.get("nested"));
        assertEquals("[1,null,2]", context.get("values"));
        assertTrue(context.containsKey("absent"));
        assertNull(context.get("absent"));
        assertNull(JsonUtils.parseStringMap(null));
        assertNull(JsonUtils.parseStringMap("null"));
        assertThrows(Exception.class, () -> JsonUtils.parseStringMap("[]"));
    }

    @Test
    void preservesEnumNamesInsteadOfCustomToStringValues() throws Exception {
        assertEquals("\"API\"", JsonUtils.toJSONStringUnsafe(tech.powerjob.common.enums.TimeExpressionType.API));
        assertEquals(tech.powerjob.common.enums.TimeExpressionType.API,
                JsonUtils.parseObject("\"API\"", tech.powerjob.common.enums.TimeExpressionType.class));
    }

    @Test
    void reportsMalformedJsonFromJackson3InsteadOfCompatibilityBridge() {
        Exception failure = assertThrows(Exception.class, () -> JsonUtils.parseObject("{", Person.class));
        assertTrue(failure.getClass().getName().startsWith("tools.jackson."), failure.getClass().getName());
    }

    @Test
    void preservesLegacyWorkflowPropertyName() throws Exception {
        WorkflowInfoDTO workflow = new WorkflowInfoDTO();
        workflow.setPEWorkflowDAG(new PEWorkflowDAG());
        Map<String, Object> json = JsonUtils.parseMap(JsonUtils.toJSONStringUnsafe(workflow));
        assertTrue(json.containsKey("peworkflowDAG"));
        assertFalse(json.containsKey("pEWorkflowDAG"));
        assertNotNull(JsonUtils.parseObject("{\"peworkflowDAG\":{}}", WorkflowInfoDTO.class).getPEWorkflowDAG());
    }

    @Test
    void preservesTransientAndFixedTimeContracts() throws Exception {
        TransientPayload payload = new TransientPayload();
        assertEquals(Map.of("value", "visible"), JsonUtils.parseMap(JsonUtils.toJSONStringUnsafe(payload)));
        PersonPlus person = new PersonPlus().setBirthDay(LocalDateTime.of(2024, 1, 2, 3, 4, 5));
        assertEquals(List.of(2024, 1, 2, 3, 4, 5), JsonUtils.parseMap(JsonUtils.toJSONStringUnsafe(person)).get("birthDay"));
        assertEquals(person, JsonUtils.parseObject(JsonUtils.toJSONStringUnsafe(person), PersonPlus.class));
    }

    @Test
    void preservesGenericSingleQuotedAndUnknownFieldInput() throws Exception {
        List<Person> people = JsonUtils.parseObject("[{'name':'fixture','age':18,'future':true}]",
                new TypeReference<List<Person>>() {});
        assertEquals(new Person().setName("fixture").setAge(18), people.getFirst());
    }

    static class TransientPayload {
        public String value = "visible";
        private transient String ignored = "hidden";

        public String getIgnored() {
            return ignored;
        }
    }

    @Test
    @SneakyThrows
    void simpleTest() {
        Person person = new Person().setName("mubao").setAge(18);
        String jsonString = JsonUtils.toJSONString(person);
        log.info("[JsonUtilsTest] person: {}, jsonString: {}", person, jsonString);
        assert jsonString != null;
        Person person2 = JsonUtils.parseObject(jsonString, Person.class);
        assert person2.equals(person);
    }

    @Test
    @SneakyThrows
    void testAdvanceApi() {
        PersonPlus personPlus = new PersonPlus();
        personPlus.setName("gongbao").setAge(3);
        personPlus.setBirthDay(LocalDateTime.now());

        String jsonString = JsonUtils.toJSONString(personPlus);
        PersonPlus personPlus2 = JsonUtils.parseObject(jsonString, PersonPlus.class);
        assert personPlus2.equals(personPlus);
    }

    @Test
    @SneakyThrows
    void testMoreOrLessFields() {
        PersonPlus personPlus = new PersonPlus().setBirthDay(LocalDateTime.now());
        personPlus.setName("gongbao").setAge(3);

        String originJsonStr = JsonUtils.toJSONString(personPlus);

        Map<String, Object> personPlusMapMore = JsonUtils.parseMap(originJsonStr);
        personPlusMapMore.put("extraKey", System.currentTimeMillis());

        PersonPlus personPlusByMoreFieldsJsonStr = JsonUtils.parseObject(JsonUtils.toJSONString(personPlusMapMore), PersonPlus.class);
        assert personPlusByMoreFieldsJsonStr.equals(personPlus);

        Map<String, Object> personPlusMapLess = JsonUtils.parseMap(originJsonStr);
        personPlusMapLess.remove("birthDay");

        PersonPlus personPlusByLessFieldsJsonStr = JsonUtils.parseObject(JsonUtils.toJSONString(personPlusMapLess), PersonPlus.class);
        assert personPlusByLessFieldsJsonStr.getName().equals(personPlus.getName());
        assert personPlusByLessFieldsJsonStr.getAge().equals(personPlus.getAge());
    }

    @Data
    @Accessors(chain = true)
    static class Person implements Serializable {
        private String name;
        private Integer age;
    }

    @Data
    @Accessors(chain = true)
    @EqualsAndHashCode(callSuper = true)
    static class PersonPlus extends Person {
        private LocalDateTime birthDay;
    }
}