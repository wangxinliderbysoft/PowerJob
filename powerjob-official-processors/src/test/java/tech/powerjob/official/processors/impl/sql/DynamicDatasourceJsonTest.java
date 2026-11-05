package tech.powerjob.official.processors.impl.sql;

import org.junit.jupiter.api.Test;
import tech.powerjob.official.processors.TestUtils;
import tech.powerjob.worker.core.processor.TaskContext;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class DynamicDatasourceJsonTest {

    @Test
    void preservesScalarStructuredAndStringEncodedJdbcProperties() throws Exception {
        AtomicReference<Properties> captured = new AtomicReference<>();
        Driver driver = new Driver() {
            @Override
            public Connection connect(String url, Properties info) throws SQLException {
                if (!acceptsURL(url)) {
                    return null;
                }
                captured.set(info);
                return DriverManager.getConnection("jdbc:h2:mem:dynamic_json_properties");
            }
            @Override public boolean acceptsURL(String url) { return "jdbc:json-properties:test".equals(url); }
            @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
            @Override public int getMajorVersion() { return 1; }
            @Override public int getMinorVersion() { return 0; }
            @Override public boolean jdbcCompliant() { return false; }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
        };
        DriverManager.registerDriver(driver);
        try {
            TaskContext context = TestUtils.genTaskContext("""
                    {"jdbcUrl":"jdbc:json-properties:test","sql":"select 1","user":"test","timeout":"7","showResult":true,
                     "sqlParams":"{\\"id\\":1}","options":{"active":true,"empty":null},"ids":[1,null,{"empty":null}],"nil":null,
                     "amount":0.123456789012345678901234}
                    """);
            DynamicDatasourceSqlProcessor processor = new DynamicDatasourceSqlProcessor();
            AbstractSqlProcessor.SqlParams params = processor.extractParams(context);
            assertEquals(7, params.getTimeout());
            try (Connection connection = processor.getConnection(params, context)) {
                assertFalse(connection.isClosed());
                Properties properties = captured.get();
                assertEquals("test", properties.getProperty("user"));
                assertEquals("true", properties.getProperty("showResult"));
                assertEquals("7", properties.getProperty("timeout"));
                assertEquals("{\"id\":1}", properties.getProperty("sqlParams"));
                assertEquals("{\"active\":true}", properties.getProperty("options"));
                assertEquals("[1,null,{}]", properties.getProperty("ids"));
                assertEquals("null", properties.getProperty("nil"));
                assertEquals("0.123456789012345678901234", properties.getProperty("amount"));
            }
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }
}
