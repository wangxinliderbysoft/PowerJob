package tech.powerjob.server.core.container;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import tech.powerjob.server.common.utils.OmsFileUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.zip.ZipFile;
import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Verifies the downloaded processor project defaults to Java 21 while preserving explicit targets.
 */
class ContainerTemplateGeneratorTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {8, 21})
    void generatesBuildableCompilerSettings(Integer javaVersion, @TempDir Path directory) throws Exception {
        try (MockedStatic<OmsFileUtils> paths = mockStatic(OmsFileUtils.class, CALLS_REAL_METHODS)) {
            paths.when(OmsFileUtils::genTemporaryWorkPath).thenReturn(directory + File.separator);
            File template = ContainerTemplateGenerator.generate("upgrade.fixture", "processor", "Processor",
                    "upgrade.fixture", javaVersion);
            try (ZipFile zip = new ZipFile(template);
                 var pom = zip.getInputStream(zip.getEntry("oms-template-origin/pom.xml"))) {
                var document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(pom);
                assertEquals(String.valueOf(javaVersion == null ? 21 : javaVersion),
                        document.getElementsByTagName("maven.compiler.release").item(0).getTextContent());
                assertEquals("upgrade.fixture", document.getElementsByTagName("groupId").item(0).getTextContent());
            }
        }
    }
}
