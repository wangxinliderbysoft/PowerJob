package tech.powerjob.samples.processors.test;

import org.springframework.stereotype.Component;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.official.processors.util.CommonUtils;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * LogTestProcessor
 *
 * @author tjq
 * @since 2022/9/18
 */
@Component
public class LogTestProcessor implements BasicProcessor {

    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        final OmsLogger omsLogger = context.getOmsLogger();
        final String parseParams = CommonUtils.parseParams(context);
        final Map<String,Object> config = Optional.ofNullable(JsonUtils.parseMap(parseParams)).orElse(new HashMap<>());

        final long loopTimes = Optional.ofNullable((Long)config.get("loopTimes")).orElse(1000L);

        for (int i = 0; i < loopTimes; i++) {
            omsLogger.debug("[DEBUG] one DEBUG log in {}", new Date());
            omsLogger.info("[INFO] one INFO log in {}", new Date());
            omsLogger.warn("[WARN] one WARN log in {}", new Date());
            omsLogger.error("[ERROR] one ERROR log in {}", new Date());
        }

        return new ProcessResult(true);
    }
}
