package tech.powerjob.official.processors.impl.context;

import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.TestUtils;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.WorkflowContext;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InjectWorkflowContextProcessorTest {

    @Test
    void preservesNestedJsonNullsAndUnquotedStringContextValues() {
        TaskContext context = TestUtils.genTaskContext("""
                {"nested":{"items":[1,null,{"name":"中文","empty":null}]},"plain":"hello","encoded":"{\\"a\\":1}","nil":null}
                """);
        context.setWorkflowContext(new WorkflowContext(1L, null));

        assertTrue(new InjectWorkflowContextProcessor().process0(context).isSuccess());

        Map<String, String> appended = context.getWorkflowContext().getAppendedContextData();
        assertEquals("hello", appended.get("plain"));
        assertEquals("{\"a\":1}", appended.get("encoded"));
        assertEquals("null", appended.get("nil"));
        Map<String, Object> nested = JsonUtils.parseMap(appended.get("nested"));
        assertEquals(Arrays.asList(1, null, JsonUtils.parseMap("{\"name\":\"中文\",\"empty\":null}")), nested.get("items"));
    }

    @Test
    void malformedParametersReturnFailureWithoutInjectingData() {
        TaskContext context = TestUtils.genTaskContext("{invalid");
        context.setWorkflowContext(new WorkflowContext(1L, null));
        assertFalse(new InjectWorkflowContextProcessor().process0(context).isSuccess());
        assertTrue(context.getWorkflowContext().getAppendedContextData().isEmpty());
    }

    @Test
    void preservesHighPrecisionNestedDecimalContextValues() {
        TaskContext context = TestUtils.genTaskContext("{\"nested\":{\"amount\":0.123456789012345678901234}}");
        context.setWorkflowContext(new WorkflowContext(1L, null));
        assertTrue(new InjectWorkflowContextProcessor().process0(context).isSuccess());
        assertEquals("{\"amount\":0.123456789012345678901234}",
                context.getWorkflowContext().getAppendedContextData().get("nested"));
    }
}
