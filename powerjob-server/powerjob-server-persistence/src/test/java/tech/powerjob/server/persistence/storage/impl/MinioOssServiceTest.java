package tech.powerjob.server.persistence.storage.impl;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.server.extension.dfs.DFsService;

import java.util.Optional;

/**
 * MinioOssServiceTest
 * 测试需要先本地部署 minio，因此捕获异常，失败也不阻断测试
 *
 * @author tjq
 * @since 2024/2/26
 */
@Slf4j
class MinioOssServiceTest extends AbstractDfsServiceTest {

    @Override
    protected Optional<DFsService> fetchService() {
        try {
            MinioOssService aliOssService = new MinioOssService();
            aliOssService.initOssClient("pj2");
            return Optional.of(aliOssService);
        } catch (Exception e) {
            // 仅异常提醒
            log.error("[MinioOssServiceTest] test exception!", e);
        }
        return Optional.empty();
    }
}