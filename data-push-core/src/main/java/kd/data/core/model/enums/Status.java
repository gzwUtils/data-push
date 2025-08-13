package kd.data.core.model.enums;

import lombok.Getter;

/**
 * 监控点
 *
 * @author gaozw
 * @date 2025/7/24 14:49
 */
@Getter
public enum Status {

   NOT_STARTED, RUNNING, STOPPED,COMPLETED, FAILED, TIMEOUT
}
