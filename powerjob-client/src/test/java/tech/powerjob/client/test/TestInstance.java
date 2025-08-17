package tech.powerjob.client.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.request.http.RunJobRequest;
import tech.powerjob.common.request.query.InstancePageQuery;
import tech.powerjob.common.response.InstanceInfoDTO;
import tech.powerjob.common.response.PageResult;
import tech.powerjob.common.response.PowerResultDTO;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.serialize.JsonUtils;

/**
 * 测试任务实例
 *
 * @author tjq
 * @since 2025/8/17
 */
@Slf4j
public class TestInstance extends ClientInitializer {

    private static final Long jobId = 1L;

    @Test
    void testOuterKeyAndExtendValue() {
        String outerKey = "ok_" + System.currentTimeMillis();
        RunJobRequest runJobRequest = new RunJobRequest()
                .setJobId(jobId)
                .setOuterKey(outerKey).setExtendValue("TEST_EXT_VALUE")
                .setInstanceParams("OpenAPI-Params")
                .setDelay(1000L);

        PowerResultDTO<Long> runJobResult = powerJobClient.runJob(runJobRequest);
        log.info("[TestInstance] runJobResult: {}", runJobResult);
        Long instanceId = runJobResult.getData();

        InstancePageQuery instancePageQuery = new InstancePageQuery();
        instancePageQuery.setOuterKeyEq(outerKey);
        ResultDTO<PageResult<InstanceInfoDTO>> pageResultResultDTO = powerJobClient.queryInstanceInfo(instancePageQuery);
        log.info("[TestInstance] queryInstanceInfo: {}", JsonUtils.toJSONString(pageResultResultDTO));

        assert pageResultResultDTO.getData().getData().get(0).getInstanceId().equals(instanceId);
    }
}
