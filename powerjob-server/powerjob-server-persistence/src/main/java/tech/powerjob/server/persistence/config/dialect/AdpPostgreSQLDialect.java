package tech.powerjob.server.persistence.config.dialect;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.PostgresPlusDialect;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.sql.Types;

import static org.hibernate.type.SqlTypes.GEOMETRY;

/**
 * <a href="https://github.com/PowerJob/PowerJob/issues/750">PG数据库方言</a>
 * 使用方自行通过配置文件激活：spring.datasource.remote.hibernate.properties.hibernate.dialect=tech.powerjob.server.persistence.config.dialect.AdpPostgreSQLDialect
 *
 * @author litong0531
 * @since 2024/8/11
 */
public class AdpPostgreSQLDialect extends PostgresPlusDialect {

    public AdpPostgreSQLDialect() {
        super();
    }

//    @Override
//    public SqlTypeDescriptor remapSqlTypeDescriptor(SqlTypeDescriptor sqlTypeDescriptor) {
//        switch (sqlTypeDescriptor.getSqlType()) {
//            case Types.CLOB:
//                return LongVarcharTypeDescriptor.INSTANCE;
//            case Types.BLOB:
//                return LongVarbinaryTypeDescriptor.INSTANCE;
//            case Types.NCLOB:
//                return LongVarbinaryTypeDescriptor.INSTANCE;
//        }
//        return super.remapSqlTypeDescriptor(sqlTypeDescriptor);
//    }

    @Override
    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);
        final DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.BLOB, "bytea", this ) );
        ddlTypeRegistry.addDescriptor( new DdlTypeImpl( Types.CLOB, "text", this ) );

    }
}
