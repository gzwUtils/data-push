package kd.data.service.exception;

import kd.data.core.exception.SyncException;

/**
 * @author gaozw
 * @date 2025/7/24 17:52
 */
@SuppressWarnings("unused")
public class TaskException  extends SyncException {
    public TaskException(String message) {
        super(message);
    }

    public TaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
