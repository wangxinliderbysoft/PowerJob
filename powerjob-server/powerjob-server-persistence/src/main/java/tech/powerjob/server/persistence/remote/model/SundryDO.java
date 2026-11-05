package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.NativeGenerator;

import jakarta.persistence.*;
import java.util.Date;

/**
 * 杂项
 * KKV 表存一些配置数据
 *
 * @author tjq
 * @since 2024/2/15
 */
@Data
@Entity
@NoArgsConstructor
@Table(uniqueConstraints = {@UniqueConstraint(name = "uidx01_sundry", columnNames = {"pkey", "skey"})})
public class SundryDO {

    @Id
    @GeneratedValue
    @NativeGenerator(sequenceForm = @SequenceGenerator(name = "native", sequenceName = "native", allocationSize = 1))
    private Long id;

    /**
     * PKEY
     */
    private String pkey;
    /**
     * SKEY
     */
    private String skey;
    /**
     * 内容
     */
    private String content;

    /**
     * 其他参数
     */
    private String extra;

    private Date gmtCreate;

    private Date gmtModified;
}
