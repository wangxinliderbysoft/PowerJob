package tech.powerjob.worker.autoconfigure;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.worker.PowerJobSpringWorker;
import tech.powerjob.worker.PowerJobWorker;
import tech.powerjob.worker.common.PowerJobWorkerConfig;
import tech.powerjob.worker.common.constants.StoreStrategy;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

/**
 * Exercises Boot's imports discovery and worker lifecycle without remote connections.
 */
class PowerJobAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(DiscoveryApplication.class);

    @Test
    void discoversConfigurationBindsPropertiesAndClosesWorker() throws Exception {
        AtomicReference<PowerJobWorkerConfig> config = new AtomicReference<>();
        try (MockedConstruction<PowerJobWorker> workers = mockConstruction(PowerJobWorker.class,
                (worker, context) -> config.set((PowerJobWorkerConfig) context.arguments().getFirst()))) {
            runner.withPropertyValues("powerjob.worker.app-name=boot4-test",
                    "powerjob.worker.server-address=127.0.0.1:1,127.0.0.1:2",
                    "powerjob.worker.port=0", "powerjob.worker.protocol=HTTP",
                    "powerjob.worker.store-strategy=MEMORY", "powerjob.worker.allow-lazy-connect-server=true")
                    .run(context -> {
                        assertThat(context).hasNotFailed().hasSingleBean(PowerJobSpringWorker.class);
                        assertThat(workers.constructed()).hasSize(1);
                        assertThat(config.get().getAppName()).isEqualTo("boot4-test");
                        assertThat(config.get().getServerAddress()).containsExactly("127.0.0.1:1", "127.0.0.1:2");
                        assertThat(config.get().getProtocol()).isEqualTo(Protocol.HTTP);
                        assertThat(config.get().getPort()).isZero();
                        assertThat(config.get().getStoreStrategy()).isEqualTo(StoreStrategy.MEMORY);
                        assertThat(config.get().isAllowLazyConnectServer()).isTrue();
                        verify(workers.constructed().getFirst()).init();
                    });
            verify(workers.constructed().getFirst()).destroy();
        }
    }

    @Test
    void disabledWorkerNeedsNoServerConfiguration() {
        runner.withPropertyValues("powerjob.worker.enabled=false")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(PowerJobSpringWorker.class));
    }

    @Test
    void backsOffForCustomWorkerWithoutRequiringServerProperties() {
        runner.withUserConfiguration(CustomWorker.class)
                .run(context -> assertThat(context).hasNotFailed().hasSingleBean(PowerJobSpringWorker.class));
    }

    @Test
    void missingServerAddressFailsExplicitly() {
        runner.run(context -> assertThat(context).hasFailed()
                .getFailure().hasRootCauseMessage("serverAddress can't be empty! "
                        + "if you don't want to enable powerjob, please config program arguments: powerjob.worker.enabled=false"));
    }

    @Test
    void retainsDeprecatedPortBinding() {
        AtomicReference<PowerJobWorkerConfig> config = new AtomicReference<>();
        try (MockedConstruction<PowerJobWorker> ignored = mockConstruction(PowerJobWorker.class,
                (worker, context) -> config.set((PowerJobWorkerConfig) context.arguments().getFirst()))) {
            runner.withPropertyValues("powerjob.worker.app-name=legacy-test",
                    "powerjob.worker.server-address=127.0.0.1:1", "powerjob.worker.akka-port=28888")
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(config.get().getPort()).isEqualTo(28888);
                    });
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class DiscoveryApplication {
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomWorker {
        @Bean
        PowerJobSpringWorker customWorker() {
            return mock(PowerJobSpringWorker.class);
        }
    }
}
