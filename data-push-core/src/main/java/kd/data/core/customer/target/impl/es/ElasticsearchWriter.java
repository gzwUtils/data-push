package kd.data.core.customer.target.impl.es;

import kd.data.core.customer.meta.ConsumerMetadata;
import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.TargetWriter;
import kd.data.core.exception.SyncException;
import kd.data.core.utils.ReflectionUtils;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * es 写入
 *
 * @author gaozw
 * @date 2025/8/1 13:46
 */
@SuppressWarnings("unused")
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
            Map<String, Object> document = createDocument(item, metadata);
            IndexQueryBuilder builder = new IndexQueryBuilder()
                    .withObject(document);

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




    private Map<String, Object> createDocument(T item, ConsumerMetadata metadata) {
        Map<String, Object> document = new HashMap<>();
        Class<?> entityType = item.getClass();

        for (ConsumerMetadata.ConsumerFieldModel fieldModel : metadata.getFields()) {
            try {
                Object value = ReflectionUtils.getFieldValue(item, fieldModel.getFieldName());
                // 使用目标字段名（如果提供）
                String fieldName = fieldModel.getTargetName();
                if (fieldName == null || fieldName.isEmpty()) {
                    fieldName = fieldModel.getFieldName();
                }
                // 添加到文档
                document.put(fieldName, value);
            } catch (Exception e) {
                // 处理字段访问异常
                throw new SyncException("Failed to access field: " + fieldModel.getFieldName(), e);
            }
        }

        return document;
    }
}
