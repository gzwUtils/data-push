package kd.data.core.utils;

import kd.data.core.model.SyncContext;

import java.util.List;

/**
 * 数据转换接口
 * @author gaozw
 * @date 2025/7/24 16:45
 */
@FunctionalInterface
public interface DataTransformUtils<S,T> {

    T transform(List<S> sourceData, SyncContext context);
}
