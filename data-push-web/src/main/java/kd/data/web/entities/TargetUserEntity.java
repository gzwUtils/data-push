package kd.data.web.entities;

import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.annotation.ConsumerTarget;
import kd.data.core.customer.annotation.EsIndex;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Data;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * 写入mysql
 * @author gaozw
 * @date 2025/7/28 9:41
 */

@Data
@ConsumerTarget(TargetEnums.ELASTICSEARCH)
@EsIndex("target_user")
@Document(indexName = "target_user",createIndex = false)
public class TargetUserEntity {

    @ConsumerField(value = "id",role = ConsumerField.FieldRole.ID)
    @Field(type = FieldType.Long,name = "id")
    private Long id;

    @Field(type = FieldType.Keyword,name="full_name")
    private String name;

    @Field(type = FieldType.Keyword,name="email_address")
    private String email;
}
