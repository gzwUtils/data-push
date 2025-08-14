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

import java.time.LocalDateTime;

/**
 * 写入mysql
 * @author gaozw
 * @date 2025/7/28 9:41
 */
@Document(indexName = "order_index",createIndex = false)
@Data
@ConsumerTarget(TargetEnums.ELASTICSEARCH)
@EsIndex("order_index")
public class TargetOrderEntity {

    @Field(name="order_id")
    @ConsumerField(value = "order_id",role = ConsumerField.FieldRole.ID)
    private Long orderId;

    @Field(type = FieldType.Keyword,name = "order_no")
    private String orderNo;

    @Field(type = FieldType.Long,name = "user_id")
    private Long userId;

    @Field(type = FieldType.Text,name = "user_name")
    private String userName;

    @Field(type = FieldType.Keyword,name = "user_phone")
    private String userPhone;

    @Field(type = FieldType.Text,analyzer = "address_analyzer",name = "user_address")
    private String userAddress;

    @Field(type = FieldType.Scaled_Float,scalingFactor = 100,name = "order_amount")
    private Double orderAmount;

    @Field(type = FieldType.Byte,name = "order_status")
    private Byte orderStatus;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Field(type = FieldType.Date,name = "order_time",format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime orderTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Field(type = FieldType.Date,name = "payment_time",format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime paymentTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Field(type = FieldType.Date,name = "delivery_time",format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime deliveryTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Field(type = FieldType.Date,name = "complete_time",format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime completeTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Field(type = FieldType.Date,name = "cancel_time",format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime cancelTime;

    @Field(type = FieldType.Long,name = "product_id")
    private Long productId;

    @Field(type = FieldType.Text,name = "product_name")
    private String productName;

    @Field(type = FieldType.Scaled_Float,scalingFactor = 100,name = "product_price")
    private Double productPrice;

    @Field(type = FieldType.Integer,name = "product_quantity")
    private Integer productQuantity;

    @Field(type = FieldType.Keyword,name = "product_sku")
    private String productSku;

    @Field(type = FieldType.Keyword,name = "product_image",index = false)
    private String productImage;

    @Field(type = FieldType.Keyword,name = "payment_method")
    private String paymentMethod;

    @Field(type = FieldType.Byte,name = "payment_status")
    private Byte paymentStatus;

    @Field(type = FieldType.Keyword,name = "logistics_company")
    private String logisticsCompany;

    @Field(type = FieldType.Keyword,name = "logistics_no")
    private String logisticsNo;

    @Field(type = FieldType.Keyword,name = "logistics_status")
    private String logisticsStatus;

    @Field(type = FieldType.Scaled_Float,scalingFactor = 100,name = "discount_amount")
    private Double discountAmount;

    @Field(type = FieldType.Long,name = "coupon_id")
    private Long couponId;

    @Field(type = FieldType.Scaled_Float,scalingFactor = 100,name = "coupon_amount")
    private Double couponAmount;

    @Field(type = FieldType.Text,name = "remark")
    private String remark;

    @Field(type = FieldType.Byte,name = "is_deleted")
    private Byte isDeleted;

}
