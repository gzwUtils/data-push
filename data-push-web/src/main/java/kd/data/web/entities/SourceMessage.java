package kd.data.web.entities;

import kd.data.core.send.adapter.annotation.ColumnMapping;
import kd.data.core.send.adapter.annotation.TableMapping;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * @author gaozw
 * @date 2025/7/29 9:51
 */
@TableMapping("message")
@Data
public class SourceMessage {

        @ColumnMapping(value = "id", isCheckpoint = true)
        private Integer id;

        @ColumnMapping("room_id")
        private Integer roomId;

        @ColumnMapping("from_uid")
        private Integer fromUid;

        @ColumnMapping("content")
        private String content;

        @ColumnMapping("reply_msg_id")
        private Integer replyMsgId;

        @ColumnMapping("status")
        private Integer status;

        @ColumnMapping("gap_count")
        private Integer gapCount;

        @ColumnMapping("type")
        private Integer type;

        @ColumnMapping("extra")
        private String extra;

        @ColumnMapping("create_time")
        private LocalDateTime createTime;

        @ColumnMapping( "update_time")
        private LocalDateTime updateTime;
}
