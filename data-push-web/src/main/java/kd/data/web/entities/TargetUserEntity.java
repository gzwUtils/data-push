package kd.data.web.entities;

import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.annotation.ConsumerTarget;
import kd.data.core.customer.annotation.CustomerTableName;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Data;

/**
 * @author gaozw
 * @date 2025/7/28 9:41
 */

@Data
@ConsumerTarget(TargetEnums.JDBC)
@CustomerTableName("target_user")
public class TargetUserEntity {

    @ConsumerField(value = "user_id",role = ConsumerField.FieldRole.ID)
    private String id;

    @ConsumerField("full_name")
    private String name;

    @ConsumerField("email_address")
    private String email;
}
