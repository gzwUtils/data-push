package kd.data.core.customer.target.impl.es;

import kd.data.core.customer.annotation.ConsumerField;
import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import java.util.*;

/**
 * es 写入
 *
 * @author gaozw
 * @date 2025/8/1 13:46
 */
public class ElasticsearchWriter<T> implements TargetWriter<T> {


    public ElasticsearchWriter() {
        // 默认空构造
    }




    @Override
    public void write(List<T> batch, TargetConnector connector, ConsumerMetadata metadata) {
        ElasticsearchOperations operations = getOperations(connector);
        String indexName = metadata.getIndexName();

        // 准备批量索引请求
        List<IndexQuery> indexQueries = new ArrayList<>(batch.size());
        for (T item : batch) {
            IndexQueryBuilder builder = new IndexQueryBuilder()
                    .withObject(item);

            // 安全处理ID：仅当ID存在时才设置
            String id = extractId(item, metadata);
            if (id != null) {
                builder.withId(id);
            }

            indexQueries.add(builder.build());
        }

        // 执行批量索引
        if (!indexQueries.isEmpty()) {
            operations.bulkIndex(indexQueries, IndexCoordinates.of(indexName));
        }
    }

    private static ElasticsearchOperations getOperations(TargetConnector connector) {
        if (!(connector instanceof EsTargetConnector)) {
            throw new IllegalArgumentException("Invalid connector type. Expected ElasticsearchConnector");
        }

        EsTargetConnector esConnector = (EsTargetConnector) connector;
        if (!esConnector.isConnected()) {
            try {
                esConnector.connect();
            } catch (Exception e) {
                throw new SyncException("Failed to connect to Elasticsearch", e);
            }
        }

        return esConnector.getElasticsearchOperations();
    }


    // 从元数据提取ID字段值
    private String extractId(T item, ConsumerMetadata metadata) {
        return metadata.getFields().stream()
                .filter(f -> Objects.equals(f.getRole(), ConsumerField.FieldRole.ID.name()))
                .findFirst()
                .map(field -> {
                    try {
                        Object idVal = ReflectionUtils.getFieldValue(item, field.getFieldName());
                        return idVal != null ? idVal.toString() : null;
                    } catch (Exception e) {
                        throw new SyncException("Failed to extract ID", e);
                    }
                })
                .orElse(null);
    }
}
