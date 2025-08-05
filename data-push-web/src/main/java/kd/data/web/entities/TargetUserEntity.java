package kd.data.web.entities;

import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.annotation.ConsumerTarget;
import kd.data.core.customer.annotation.CustomerTableName;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Data;

/**
 * 写入mysql
 * @author gaozw
 * @date 2025/7/28 9:41
 */

@Data
@ConsumerTarget(TargetEnums.ELASTICSEARCH)
@CustomerTableName("users")
public class TargetUserEntity {

    @ConsumerField(value = "id",role = ConsumerField.FieldRole.ID)
    private String id;

    @ConsumerField("username")
    private String name;

    @ConsumerField("email")
    private String email;
}
