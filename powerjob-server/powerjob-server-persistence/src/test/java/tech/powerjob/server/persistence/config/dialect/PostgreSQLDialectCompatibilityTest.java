package tech.powerjob.server.persistence.config.dialect;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards the existing PostgreSQL text and bytea mappings without connecting to a database.
 */
class PostgreSQLDialectCompatibilityTest {

    @Test
    void powerJobClobsUseTextInsteadOfOid() {
        try (StandardServiceRegistry registry = registry(PowerJobPGDialect.class)) {
            TypeConfiguration types = types(registry);
            assertEquals("text", types.getDdlTypeRegistry().getTypeName(Types.CLOB, 255L, null, null));
            assertEquals(Types.LONGVARCHAR, types.getJdbcTypeRegistry().getDescriptor(Types.CLOB).getJdbcTypeCode());
        }
    }

    @Test
    void adaptedDialectUsesByteaInsteadOfLargeObjectApi() {
        try (StandardServiceRegistry registry = registry(AdpPostgreSQLDialect.class)) {
            TypeConfiguration types = types(registry);
            assertEquals("text", types.getDdlTypeRegistry().getTypeName(Types.CLOB, 255L, null, null));
            assertEquals("bytea", types.getDdlTypeRegistry().getTypeName(Types.BLOB, 255L, null, null));
            assertEquals(Types.LONGVARBINARY, types.getJdbcTypeRegistry().getDescriptor(Types.BLOB).getJdbcTypeCode());
        }
    }

    private static StandardServiceRegistry registry(Class<? extends PowerJobPGDialect> dialect) {
        return new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", dialect.getName())
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", false)
                .build();
    }

    private static TypeConfiguration types(StandardServiceRegistry registry) {
        return ((MetadataImplementor) new MetadataSources(registry).buildMetadata()).getTypeConfiguration();
    }
}
