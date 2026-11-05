package tech.powerjob.official.processors.util;

import tech.powerjob.worker.core.processor.TaskContext;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CommonUtils
 *
 * @author tjq
 * @since 2021/2/1
 */
public class CommonUtils {

    private CommonUtils() {

    }

    public static String parseParams(TaskContext context) {
        // 工作流中的总是优先使用 jobParams
        if (context.getWorkflowContext().getWfInstanceId() != null) {
            return context.getJobParams();
        }

        if (StringUtils.isNotEmpty(context.getInstanceParams())) {
            return context.getInstanceParams();
        }
        return context.getJobParams();
    }

    /**
     * Preserves null-map-entry omission in persisted configuration and JDBC JSON properties.
     * Null array elements are retained.
     */
    public static Object omitNullMapValues(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            map.forEach((key, entry) -> {
                if (entry != null) {
                    result.put(key, omitNullMapValues(entry));
                }
            });
            return result;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(CommonUtils::omitNullMapValues).toList();
        }
        return value;
    }
}
