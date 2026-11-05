package tech.powerjob.server.persistence.config;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.dialect.MySQLDialect;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tech.powerjob.server.common.utils.PropertyUtils;
import tech.powerjob.server.persistence.config.dialect.AdpPostgreSQLDialect;
import tech.powerjob.server.persistence.local.LocalInstanceLogDO;
import tech.powerjob.server.persistence.local.LocalInstanceLogRepository;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.JobInfoRepository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates against frozen Java 8/Hibernate 5 schemas on explicitly supplied disposable databases.
 * Run this class explicitly; both URLs must identify a loopback database named powerjob_upgrade_test.
 */
class PersistenceMigrationIT {

    @Test
    void mysqlPreservesSchemaIdsLobsAndTransactionRouting() throws Exception {
        verify("mysql", MySQLDialect.class.getName());
    }

    @Test
    void postgresPreservesSchemaIdsLobsAndTransactionRouting() throws Exception {
        verify("postgresql", AdpPostgreSQLDialect.class.getName());
    }

    private void verify(String database, String dialect) throws Exception {
        String prefix = "powerjob.test." + database;
        String url = Objects.requireNonNull(System.getProperty(prefix + ".url"), prefix + ".url is required");
        assertTrue(url.matches("jdbc:" + database + "://127\\.0\\.0\\.1:[0-9]+/powerjob_upgrade_test(\\?.*)?"),
                "Only a dedicated loopback powerjob_upgrade_test database is allowed");
        String user = System.getProperty(prefix + ".username", database.equals("mysql") ? "root" : "powerjob");
        String password = System.getProperty(prefix + ".password", "");
        try (Connection connection = DriverManager.getConnection(url, user, password);
             ResultSet tables = connection.getMetaData().getTables(connection.getCatalog(), null,
                     "compat_app_info", new String[]{"TABLE"})) {
            if (!tables.next()) {
                ScriptUtils.executeSqlScript(connection, new ClassPathResource("schema/java8/" + database + ".sql"));
            }
        }

        String previousPrefix = (String) PropertyUtils.getProperties().setProperty("oms.table-prefix", "compat_");
        SpringApplication application = new SpringApplication(PersistenceConfiguration.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        try (ConfigurableApplicationContext context = application.run(
                "--spring.config.location=optional:classpath:/migration-test.properties",
                "--spring.datasource.core.jdbc-url=" + url, "--spring.datasource.core.username=" + user,
                "--spring.datasource.core.password=" + password,
                "--spring.datasource.remote.hibernate.properties.hibernate.hbm2ddl.auto=validate",
                "--spring.datasource.remote.hibernate.properties.hibernate.dialect=" + dialect)) {
            JobInfoRepository jobs = context.getBean(JobInfoRepository.class);
            LocalInstanceLogRepository logs = context.getBean(LocalInstanceLogRepository.class);
            TransactionTemplate remote = new TransactionTemplate(context.getBean("remoteTransactionManager", PlatformTransactionManager.class));
            TransactionTemplate local = context.getBean("localTransactionTemplate", TransactionTemplate.class);
            String content = "legacy-compatible-large-value-".repeat(4096);
            Long jobId = remote.execute(status -> {
                JobInfoDO job = new JobInfoDO();
                job.setJobName("java21-migration");
                job.setAppId(9007199254740993L);
                job.setJobParams(content);
                JobInfoDO saved = jobs.saveAndFlush(job);
                assertNotNull(saved.getId());
                Objects.requireNonNull(EntityManagerFactoryUtils.getTransactionalEntityManager(
                        context.getBean("remoteEntityManagerFactory", EntityManagerFactory.class))).clear();
                assertEquals(content, jobs.findById(saved.getId()).orElseThrow().getJobParams());
                status.setRollbackOnly();
                return saved.getId();
            });
            assertNotNull(jobId);
            assertTrue(jobs.findById(jobId).isEmpty());
            local.executeWithoutResult(status -> {
                LocalInstanceLogDO log = new LocalInstanceLogDO();
                log.setInstanceId(jobId);
                log.setLogTime(1704067200000L);
                log.setLogContent(content);
                LocalInstanceLogDO saved = logs.saveAndFlush(log);
                assertNotNull(saved.getId());
                assertEquals(1, logs.countByInstanceId(jobId));
                status.setRollbackOnly();
            });
            assertEquals(0, logs.countByInstanceId(jobId));
        } finally {
            if (previousPrefix == null) {
                PropertyUtils.getProperties().remove("oms.table-prefix");
            } else {
                PropertyUtils.getProperties().setProperty("oms.table-prefix", previousPrefix);
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties
    @ImportAutoConfiguration(HibernateJpaAutoConfiguration.class)
    @Import({MultiDatasourceConfig.class, LocalJpaConfig.class, RemoteJpaConfig.class})
    static class PersistenceConfiguration {
        @Bean
        MultiDatasourceProperties multiDatasourceProperties() {
            return new MultiDatasourceProperties();
        }
    }
}
