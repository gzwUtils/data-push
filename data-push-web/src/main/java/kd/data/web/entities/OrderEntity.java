package kd.data.web.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import kd.data.core.send.adapter.annotation.ColumnMapping;
import kd.data.core.send.adapter.annotation.TableMapping;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单
 *
 * @author gaozw
 * @date 2025/8/14 11:58
 */
@Data
@TableMapping("order_table")
public class OrderEntity {

    @ColumnMapping(value = "order_id", isCheckpoint = true)
    private Long orderId;

    @ColumnMapping("order_no")
    private String orderNo;

    @ColumnMapping("user_id")
    private Long userId;

    @ColumnMapping("user_name")
    private String userName;

    @ColumnMapping("user_phone")
    private String userPhone;

    @ColumnMapping("user_address")
    private String userAddress;

    @ColumnMapping("order_amount")
    private BigDecimal orderAmount;

    @ColumnMapping("order_status")
    private Integer orderStatus;

    @ColumnMapping("order_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS",timezone = "GMT+8")
    private LocalDateTime orderTime;

    @ColumnMapping("payment_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS",timezone = "GMT+8")
    private LocalDateTime paymentTime;

    @ColumnMapping("delivery_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS",timezone = "GMT+8")
    private LocalDateTime deliveryTime;

    @ColumnMapping("complete_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS",timezone = "GMT+8")
    private LocalDateTime completeTime;

    @ColumnMapping("cancel_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS",timezone = "GMT+8")
    private LocalDateTime cancelTime;

    @ColumnMapping("product_id")
    private Long productId;

    @ColumnMapping("product_name")
    private String productName;

    @ColumnMapping("product_price")
    private BigDecimal productPrice;

    @ColumnMapping("product_quantity")
    private Integer productQuantity;

    @ColumnMapping("product_sku")
    private String productSku;

    @ColumnMapping("product_image")
    private String productImage;

    @ColumnMapping("payment_method")
    private String paymentMethod;

    @ColumnMapping("payment_status")
    private Integer paymentStatus;

    @ColumnMapping("logistics_company")
    private String logisticsCompany;

    @ColumnMapping("logistics_no")
    private String logisticsNo;

    @ColumnMapping("logistics_status")
    private String logisticsStatus;

    @ColumnMapping("discount_amount")
    private BigDecimal discountAmount;

    @ColumnMapping("coupon_id")
    private Long couponId;

    @ColumnMapping("coupon_amount")
    private BigDecimal couponAmount;

    @ColumnMapping("remark")
    private String remark;

    @ColumnMapping("is_deleted")
    private Integer isDeleted;
}
