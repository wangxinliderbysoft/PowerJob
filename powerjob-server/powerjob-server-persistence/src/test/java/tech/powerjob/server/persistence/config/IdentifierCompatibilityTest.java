package tech.powerjob.server.persistence.config;

import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.jupiter.api.Test;
import tech.powerjob.server.persistence.config.dialect.PowerJobPGDialect;
import tech.powerjob.server.persistence.remote.model.AppInfoDO;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks identifier mappings without creating or updating any database schema.
 */
class IdentifierCompatibilityTest {

    @Test
    void mysqlKeepsIdentityColumns() {
        try (StandardServiceRegistry registry = registry(MySQLDialect.class.getName())) {
            Metadata metadata = new MetadataSources(registry).addAnnotatedClass(AppInfoDO.class).buildMetadata();
            try (SessionFactory factory = metadata.buildSessionFactory()) {
                assertTrue(((SessionFactoryImplementor) factory).getMappingMetamodel()
                        .getEntityDescriptor(AppInfoDO.class).getGenerator().generatedOnExecution());
                assertTrue(metadata.getEntityBinding(AppInfoDO.class.getName()).getIdentifier().getColumns().getFirst().isIdentity());
            }
        }
    }

    @Test
    void postgresKeepsSharedSequenceAndIncrement() {
        try (StandardServiceRegistry registry = registry(PowerJobPGDialect.class.getName())) {
            Metadata metadata = new MetadataSources(registry).addAnnotatedClass(AppInfoDO.class).buildMetadata();
            try (SessionFactory ignored = metadata.buildSessionFactory()) {
                List<String> sequences = new ArrayList<>();
                metadata.getDatabase().getNamespaces().forEach(namespace -> namespace.getSequences().forEach(sequence -> {
                    sequences.add(sequence.getName().getSequenceName().getText());
                    assertEquals(1, sequence.getIncrementSize());
                }));
                assertEquals(List.of("native"), sequences);
            }
        }
    }

    private static StandardServiceRegistry registry(String dialect) {
        return new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", dialect)
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", false)
                .applySetting("hibernate.hbm2ddl.auto", "none")
                .build();
    }
}
