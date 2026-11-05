package tech.powerjob.official.processors.impl;

import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.TestUtils;
import tech.powerjob.worker.core.processor.TaskContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ConfigProcessorJsonTest {

    @Test
    void persistsNestedConfigurationWithoutNullMapEntriesButKeepsNullArrayElements() throws Exception {
        Path directory = Path.of("target", "config-json-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Path file = directory.resolve("config.json");
        Map<String, Object> config = JsonUtils.parseMap("""
                {"empty":null,"nested":{"nil":null,"value":"中文"},"items":[null,{"nil":null,"value":7}]}
                """);
        try {
            TaskContext context = TestUtils.genTaskContext(JsonUtils.toJSONStringUnsafe(
                    Map.of("config", config, "persistentFileName", file.toString())));
            assertTrue(new ConfigProcessor().process(context).isSuccess());

            Map<String, Object> persisted = JsonUtils.parseMap(Files.readString(file));
            assertFalse(persisted.containsKey("empty"));
            assertEquals(Map.of("value", "中文"), persisted.get("nested"));
            assertEquals(Arrays.asList(null, Map.of("value", 7)), persisted.get("items"));
            assertTrue(ConfigProcessor.fetchConfig().containsKey("empty"), "In-memory configuration must not be changed");
        } finally {
            Files.deleteIfExists(file);
            Files.delete(directory);
        }
    }
}
