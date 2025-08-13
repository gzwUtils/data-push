package kd.data.persistence;

import kd.data.persistence.response.R;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 请求上下文
 *
 * @author gaozw
 * @date 2025/8/8 14:24
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessContext<T extends BaseModel> {

    /**策略code*/

    private String strategyCode;

    /**请求数据*/
    private T model;

    /**是否需要中断*/
    private Boolean needBreak;

    /**响应*/
    private R response;
}
