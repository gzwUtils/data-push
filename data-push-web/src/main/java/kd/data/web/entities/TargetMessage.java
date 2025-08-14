package kd.data.web.entities;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.annotation.ConsumerTarget;
import kd.data.core.customer.annotation.EsIndex;
import kd.data.core.customer.target.targetenums.TargetEnums;
import lombok.Data;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import java.math.BigInteger;
import java.time.LocalDateTime;

/**
 * 写入es
 * @author gaozw
 * @date 2025/7/29 9:51
 */
@EsIndex("tar_message")
@ConsumerTarget(TargetEnums.ELASTICSEARCH)
@Data
@Document(indexName = "tar_message",createIndex = false)
public class TargetMessage {

        @Field(name="id")
        @ConsumerField(value = "id", role = ConsumerField.FieldRole.ID)
        private BigInteger id;

        @Field(type = FieldType.Long,name = "roomId")
        private BigInteger roomId;

        @Field(type = FieldType.Long,name = "fromUid")
        private BigInteger fromUid;

        @Field(type = FieldType.Text,name = "content",index = false)
        private String content;

        @Field(type = FieldType.Long,name = "replyMsgId")
        private BigInteger replyMsgId;

        @Field(type = FieldType.Integer,name = "status")
        private Integer status;

        @Field(type = FieldType.Integer,name = "gapCount")
        private Integer gapCount;

        @Field(type = FieldType.Integer,name = "type")
        private Integer type;

        @Field(type = FieldType.Text,name = "extra",index = false)
        private String extra;


        @JsonDeserialize(using = LocalDateTimeDeserializer.class)
        @JsonSerialize(using = LocalDateTimeSerializer.class)
        @Field(type = FieldType.Date,name = "createTime",format = DateFormat.date_hour_minute_second_millis)
        private LocalDateTime createTime;

        @JsonDeserialize(using = LocalDateTimeDeserializer.class)
        @JsonSerialize(using = LocalDateTimeSerializer.class)
        @Field(type = FieldType.Date,name = "updateTime",format = DateFormat.date_hour_minute_second_millis)
        private LocalDateTime updateTime;
}
