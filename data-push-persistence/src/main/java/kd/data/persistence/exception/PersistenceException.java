package kd.data.persistence.exception;

/**
 * 自定义异常
 *
 * @author gaozw
 * @date 2025/8/8 15:40
 */
@SuppressWarnings("unused")
public class PersistenceException extends RuntimeException {

    public PersistenceException(String message) {
        super(message);
    }

    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
