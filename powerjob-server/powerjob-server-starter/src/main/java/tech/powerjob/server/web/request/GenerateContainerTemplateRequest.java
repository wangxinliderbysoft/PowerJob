package tech.powerjob.server.web.request;

import lombok.Data;

/**
 * 创建容器模版请求
 *
 * @author tjq
 * @since 2020/5/15
 */
@Data
public class GenerateContainerTemplateRequest {

    /**
     * Maven Group
     */
    private String group;
    /**
     * Maven artifact
     */
    private String artifact;
    /**
     * Maven name
     */
    private String name;
    /**
     * 包名（com.xx.xx.xx）
     */
    private String packageName;
    /**
     * Target Java version; defaults to 21 when omitted.
     */
    private Integer javaVersion;

}
