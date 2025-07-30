package kd.data.core.model;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * 同步上下文，保存任务运行时信息
 * @author gaozw
 * @date 2025/7/24 16:47
 */
@SuppressWarnings("unused")
public class SyncContext {

    private static final ThreadLocal<SyncContext> currentContext = new ThreadLocal<>();

    @Getter
    private final String sourceType;
    private final Map<String, Object> attributes = new HashMap<>();

    public SyncContext(String sourceType) {
        this.sourceType = sourceType;
    }

    public static SyncContext getCurrent() {
        return currentContext.get();
    }

    public static void setCurrent(SyncContext context) {
        currentContext.set(context);
    }

    public static void clear() {
        currentContext.remove();
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }
}
