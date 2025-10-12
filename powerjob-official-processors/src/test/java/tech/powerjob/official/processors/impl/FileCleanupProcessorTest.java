package tech.powerjob.official.processors.impl;

import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.TestUtils;
import tech.powerjob.worker.core.processor.TaskContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * test FileCleanupProcessor
 *
 * @author tjq
 * @since 2021/2/1
 */
class FileCleanupProcessorTest {

    @Test
    void testPatternCompile() throws Exception {
        String fileName = "abc.log";
        System.out.println(fileName.matches("[\\s\\S]*log"));
        System.out.println(Pattern.matches("[a-z.0-9]*log", fileName));
    }

    @Test
    void testScriptCompile() throws Exception {
        Pattern compile = Pattern.compile("(shell|python)_[0-9]*\\.(sh|py)");
        String fileNameA = "shell_158671537124147264.sh";
        String fileNameB = "python_158671537124147264.py";
        assertTrue(compile.matcher(fileNameA).matches());
        assertTrue(compile.matcher(fileNameB).matches());
    }

    @Test
    void testProcess() throws Exception {
        Map<String,Object> params = new HashMap<>() ;
        params.put("dirPath", "/Users/tjq/logs");
        params.put("filePattern", "[\\s\\S]*log");
        params.put("retentionTime", 0);
        String paramsStr = JsonUtils.toJSONString(List.of(params));

        System.out.println(paramsStr);

        TaskContext taskContext = TestUtils.genTaskContext(paramsStr);
        System.out.println(new FileCleanupProcessor().process(taskContext));
    }

    @Test
    void testCleanWorkerScript() throws Exception {
        Map<String,Object> params = new HashMap<>() ;
        params.put("dirPath", "/");
        params.put("filePattern", "(shell|python)_[0-9]*\\.(sh|py)");
        params.put("retentionTime", 24);
        List<Map<String, Object>> params1 = List.of(params);


        TaskContext taskContext = TestUtils.genTaskContext(JsonUtils.toJSONString(params1));
        System.out.println(new FileCleanupProcessor().process(taskContext));
    }
}