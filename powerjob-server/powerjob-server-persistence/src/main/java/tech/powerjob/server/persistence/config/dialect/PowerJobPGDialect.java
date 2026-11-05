package tech.powerjob.server.persistence.config.dialect;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.community.dialect.PostgreSQLLegacyDialect;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.descriptor.jdbc.LongVarcharJdbcType;

import java.sql.Types;

/**
 * PostgreSQL 数据库支持，需要在 application.properties 中添加以下配置项进行激活
 * spring.datasource.remote.hibernate.properties.hibernate.dialect=tech.powerjob.server.persistence.config.dialect.PowerJobPGDialect
 *
 * @author Kung Yao
 * @author Echo009
 * 2021/3/24 下午 04:23
 * 1074_King
 */
public class PowerJobPGDialect extends PostgreSQLLegacyDialect {

    public PowerJobPGDialect() {
        super(DatabaseVersion.make(10));
    }

    @Override
    protected String columnType(int sqlTypeCode) {
        return sqlTypeCode == Types.CLOB ? "text" : super.columnType(sqlTypeCode);
    }

    /**
     * 使用 {@link Types#LONGVARCHAR} 覆盖 {@link Types#CLOB} 类型
     *
     * 注意，如果在 PG 库创建表时使用的列类型为 oid ，那么这样会导致没法正确读取数据
     * 在 PowerJob 中能这样用是因为 PowerJob 的所有实体类中被 @Lob 注解标记的列对应数据库中的字段类型都是 text
     * 另外还需要注意数据库版本，如果是 10.x 以前的，需自行提供一个合适的 Dialect 类（选择合适的版本继承）
     *
     * 更多内容请关注该 issues：https://github.com/PowerJob/PowerJob/issues/153
     */
    @Override
    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        typeContributions.getTypeConfiguration().getJdbcTypeRegistry()
                .addDescriptor(Types.CLOB, LongVarcharJdbcType.INSTANCE);
    }
}
