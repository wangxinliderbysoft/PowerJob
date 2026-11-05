package tech.powerjob.server.persistence.storage.impl;

import org.jclouds.ContextBuilder;
import org.jclouds.Constants;
import org.jclouds.io.Payload;
import org.jclouds.s3.domain.S3Object;
import org.jclouds.s3.reference.S3Constants;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.env.Environment;
import tech.powerjob.server.common.spring.condition.PropertyAndOneBeanCondition;
import tech.powerjob.server.extension.dfs.*;
import tech.powerjob.server.persistence.storage.AbstractDFsService;
import tech.powerjob.common.exception.PowerJobException;

import jakarta.annotation.Priority;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * MINIO support
 * <a href="https://min.io/">High Performance Object Storage</a>
 * 配置项：
 * oms.storage.dfs.minio.endpoint
 * oms.storage.dfs.minio.bucketName
 * oms.storage.dfs.minio.accessKey
 * oms.storage.dfs.minio.secretKey
 *
 * @author xinyi
 * @since 2023/8/21
 */
@Slf4j
@Priority(value = Integer.MAX_VALUE - 3)
@Conditional(MinioOssService.MinioOssCondition.class)
public class MinioOssService extends AbstractDFsService {

    private static final String TYPE_MINIO = "minio";
    private static final String KEY_ENDPOINT = "endpoint";
    private static final String KEY_BUCKET_NAME = "bucketName";
    private static final String ACCESS_KEY = "accessKey";
    private static final String SECRET_KEY = "secretKey";
    private MinioS3Client s3;
    private String bucket;

    @Override
    public void store(StoreRequest storeRequest) {
        S3Object object = s3.newS3Object();
        object.getMetadata().setKey(parseFileName(storeRequest.getFileLocation()));
        object.setPayload(storeRequest.getLocalFile());
        s3.putObject(bucket, object);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void download(DownloadRequest downloadRequest) {
        FileUtils.forceMkdirParent(downloadRequest.getTarget());
        S3Object object = s3.getObject(bucket, parseFileName(downloadRequest.getFileLocation()));
        if (object == null) {
            throw new PowerJobException("MinIO object does not exist");
        }
        try (Payload payload = object.getPayload(); InputStream input = payload.openStream()) {
            Files.copy(input, downloadRequest.getTarget().toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * 获取文件元
     *
     * @param fileLocation 文件位置
     */
    @Override
    public Optional<FileMeta> fetchFileMeta(FileLocation fileLocation) {
        return Optional.ofNullable(s3.headFileMeta(bucket, parseFileName(fileLocation)));
    }

    private static String parseFileName(FileLocation fileLocation) {
        return String.format("%s/%s", fileLocation.getBucket(), fileLocation.getName());
    }

    /**
     * 清理过期文件
     *
     * @param bucket 桶名
     * @param days   日期
     */
    @Override
    public void cleanExpiredFiles(String bucket, int days) {
        /*
        使用Minio的管理界面或Minio客户端命令行工具设置对象的生命周期规则。在生命周期规则中定义文件的过期时间。Minio将自动根据设置的规则删除过期的文件。
         */
    }

    /**
     * 释放连接
     */
    @Override
    @SneakyThrows(IOException.class)
    public void destroy() {
        if (s3 != null) {
            s3.close();
        }
    }

    /**
     * 初始化minio
     *
     * @param applicationContext /
     */
    @Override
    protected void init(ApplicationContext applicationContext) {
        Environment environment = applicationContext.getEnvironment();

        String endpoint = fetchProperty(environment, TYPE_MINIO, KEY_ENDPOINT);
        String bucketName = fetchProperty(environment, TYPE_MINIO, KEY_BUCKET_NAME);
        String accessKey = fetchProperty(environment, TYPE_MINIO, ACCESS_KEY);
        String secretKey = fetchProperty(environment, TYPE_MINIO, SECRET_KEY);

        try {
            initOssClient(endpoint, bucketName, accessKey, secretKey);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * 创建minio连接并且创建桶
     *
     * @param endpoint   端口
     * @param bucketName 桶名
     * @param accessKey  访问密钥
     * @param secretKey  秘密密钥
     */
    public void initOssClient(String endpoint, String bucketName, String accessKey, String secretKey) {
        log.info("[Minio] initializing OSS storage");
        if (StringUtils.isEmpty(bucketName)) {
            throw new IllegalArgumentException("'oms.storage.dfs.minio.bucketName' can't be empty, please creat a bucket in minio oss console then config it to powerjob");
        }

        Properties properties = new Properties();
        properties.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        properties.setProperty(S3Constants.PROPERTY_SIGNER_VERSION, "4");
        properties.setProperty("jclouds.region.us-east-1.endpoint", endpoint);
        properties.setProperty(Constants.PROPERTY_CONNECTION_TIMEOUT, "10000");
        properties.setProperty(Constants.PROPERTY_SO_TIMEOUT, "50000");
        this.s3 = ContextBuilder.newBuilder(new MinioS3Client.Metadata()).endpoint(endpoint)
                .credentials(accessKey, secretKey).overrides(properties).buildApi(MinioS3Client.class);
        this.bucket = bucketName;
        createBucket(bucketName);
        log.info("[Minio] initialize OSS successfully!");
    }

    /**
     * 创建 bucket
     *
     * @param bucketName 桶名
     */
    @SneakyThrows(Exception.class)
    public void createBucket(String bucketName) {

        // 建议自行创建 bucket，设置好相关的策略
        if (bucketExists(bucketName)) {
           return;
        }

        if (!s3.putBucketInRegion("us-east-1", bucketName)) {
            throw new PowerJobException("Unable to create MinIO bucket");
        }
        log.info("[Minio] createBucket successfully, bucketName: {}", bucketName);

        String policy = "{\n" +
                "    \"Version\": \"2012-10-17\",\n" +
                "    \"Statement\": [\n" +
                "        {\n" +
                "            \"Action\": [\n" +
                "                \"s3:GetObject\"\n" +
                "            ],\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Principal\": {\n" +
                "                \"AWS\": [\n" +
                "                    \"*\"\n" +
                "                ]\n" +
                "            },\n" +
                "            \"Resource\": [\n" +
                "                \"arn:aws:s3:::" + bucketName + "/*\"\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        try {
            s3.putBucketPolicy(bucketName, policy);
        } catch (Exception e) {
            log.warn("[Minio] setBucketPolicy failed, maybe you need to setBucketPolicy by yourself!", e);
        }
    }

    /**
     * 判断 bucket是否存在
     *
     * @param bucketName: 桶名
     * @return boolean
     */
    @SneakyThrows(Exception.class)
    public boolean bucketExists(String bucketName) {

        return s3.bucketExists(bucketName);
    }

    public static class MinioOssCondition extends PropertyAndOneBeanCondition {

        @Override
        protected List<String> anyConfigKey() {
            return Lists.newArrayList("oms.storage.dfs.minio.endpoint");
        }

        @Override
        protected Class<?> beanType() {
            return DFsService.class;
        }
    }
}
