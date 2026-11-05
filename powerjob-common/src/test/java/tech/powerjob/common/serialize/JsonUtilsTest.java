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