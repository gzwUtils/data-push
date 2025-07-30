package kd.data.core.customer.target;
import java.io.Closeable;
import java.sql.SQLException;

/**
 * 目标连接器接口
 *  * 支持不同类型的目标连接
 * @author gaozw
 * @date 2025/7/24 16:35
 */
@SuppressWarnings("unused")
public interface TargetConnector extends Closeable {

    void connect() throws SQLException;
    boolean isConnected();

    @Override
    default void close() {
        // 默认空实现
    }

    // 获取连接类型 (JDBC, KAFKA, ES, etc)
    String getType();
}
