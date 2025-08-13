package kd.data.persistence;

/**
 * 执行器
 * @author gaozw
 * @date 2025/8/8 14:18
 */

public interface KdProcess<T extends BaseModel> {


    /**
     * 真正处理逻辑
     * @param content 上下文
     */
    void process(ProcessContext<T> content);
}
