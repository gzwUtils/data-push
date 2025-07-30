package kd.data.web.entities;

import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.annotation.ConsumerTarget;
import kd.data.core.customer.annotation.CustomerTableName;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Data;

import java.math.BigInteger;
import java.time.LocalDateTime;

/**
 * @author gaozw
 * @date 2025/7/29 9:51
 */
@ConsumerTarget(TargetEnums.JDBC)
@CustomerTableName("tar_message")
@Data
public class TargetMessage {




        @ConsumerField(value = "id", role = ConsumerField.FieldRole.ID)
        private BigInteger id;

        @ConsumerField("room_id")
        private BigInteger roomId;

        @ConsumerField("from_uid")
        private BigInteger fromUid;

        @ConsumerField("content")
        private String content;

        @ConsumerField("reply_msg_id")
        private BigInteger replyMsgId;

        @ConsumerField("status")
        private Integer status;

        @ConsumerField("gap_count")
        private Integer gapCount;

        @ConsumerField("type")
        private Integer type;

        @ConsumerField("extra")
        private String extra;

        @ConsumerField("create_time")
        private LocalDateTime createTime;

        @ConsumerField( "update_time")
        private LocalDateTime updateTime;
}
