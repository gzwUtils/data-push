package kd.data.persistence.response;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author gaozw
 * @date 2025/8/8 14:28
 */
@SuppressWarnings("unused")
@AllArgsConstructor
@Data
public class R {

    private Integer code;

    private String message;


    private Boolean success;

    public static R success() {

        return new R(200, null, true);

    }

    public static R success(String message) {

        return new R(200, message, true);

    }


    public static R fail(String message) {

        return new R(500, message, false);

    }


}
