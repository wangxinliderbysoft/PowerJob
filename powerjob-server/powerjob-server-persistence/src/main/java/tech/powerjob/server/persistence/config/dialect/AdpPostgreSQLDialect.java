package tech.powerjob.server.persistence.config.dialect;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.descriptor.jdbc.LongVarbinaryJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;

import java.sql.Types;

/**
 * <a href="https://github.com/PowerJob/PowerJob/issues/750">PG数据库方言</a>
 * 使用方自行通过配置文件激活：spring.datasource.remote.hibernate.properties.hibernate.dialect=tech.powerjob.server.persistence.config.dialect.AdpPostgreSQLDialect
 *
 * @author litong0531
 * @since 2024/8/11
 */
public class AdpPostgreSQLDialect extends PowerJobPGDialect {

    @Override
    protected String columnType(int sqlTypeCode) {
        return sqlTypeCode == Types.BLOB ? "bytea" : super.columnType(sqlTypeCode);
    }

    @Override
    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        JdbcTypeRegistry jdbcTypes = typeContributions.getTypeConfiguration().getJdbcTypeRegistry();
        jdbcTypes.addDescriptor(Types.BLOB, LongVarbinaryJdbcType.INSTANCE);
        jdbcTypes.addDescriptor(Types.NCLOB, LongVarbinaryJdbcType.INSTANCE);
    }
}
