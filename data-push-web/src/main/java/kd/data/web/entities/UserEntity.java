package kd.data.web.entities;

import kd.data.core.data.adapter.annotation.ColumnMapping;
import kd.data.core.data.adapter.annotation.TableMapping;
import lombok.Data;

/**
 * 源实体类
 * @author gaozw
 * @date 2025/7/28 9:37
 */

@Data
@TableMapping("users")
public class UserEntity {

    @ColumnMapping(value = "id", isCheckpoint = true)
    private Long id;

    @ColumnMapping("username")
    private String name;

    @ColumnMapping("email")
    private String email;
}
