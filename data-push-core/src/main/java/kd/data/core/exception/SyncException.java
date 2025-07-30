package kd.data.core.exception;

/**
 * 自定义异常
 *
 * @author gaozw
 * @date 2025/7/23 17:27
 */
@SuppressWarnings("unused")
public class SyncException  extends RuntimeException{

    public SyncException(String message) {
        super(message);
    }

    public SyncException(String message, Throwable cause) {
        super(message, cause);
    }
}
