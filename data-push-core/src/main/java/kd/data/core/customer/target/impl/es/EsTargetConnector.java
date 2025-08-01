package kd.data.core.customer.target.impl.es;

import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.targetenums.TargetEnums;
import kd.data.core.exception.SyncException;
import lombok.Getter;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;

import java.util.Arrays;
import java.util.List;


/**
 * es
 *
 * @author gaozw
 * @date 2025/8/1 13:12
 */
@Getter
@SuppressWarnings("unused")
public class EsTargetConnector implements TargetConnector {


    private ElasticsearchOperations elasticsearchOperations;
    private final List<String> clusterNodes;
    private final int connectTimeout;
    private final int socketTimeout;

    private EsTargetConnector(Builder builder) {
        this.clusterNodes = builder.clusterNodes;
        this.connectTimeout = builder.connectTimeout;
        this.socketTimeout = builder.socketTimeout;
    }

    @Override
    public void connect() throws SyncException {
        if (isConnected()) {
            return;
        }

        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new SyncException("No Elasticsearch nodes provided");
        }

        try {
            // 构建客户端配置 (JDK 8 兼容)
            ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                    .connectedTo(clusterNodes.toArray(new String[0]))
                    .withConnectTimeout(connectTimeout)
                    .withSocketTimeout(socketTimeout)
                    .build();

            // 创建 Elasticsearch 客户端
            this.elasticsearchOperations = new ElasticsearchRestTemplate(
                    RestClients.create(clientConfiguration).rest()
            );

            // 测试连接
            if (!isClusterAvailable()) {
                throw new SyncException("Elasticsearch cluster ping failed");
            }
        } catch (Exception e) {
            throw new SyncException("ES connection failed: " + e.getMessage(), e);
        }
    }

    private boolean isClusterAvailable() {
        try {
            // 更兼容的集群可用性检查
            return elasticsearchOperations.indexOps(IndexCoordinates.of("_all")).exists();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isConnected() {
        return elasticsearchOperations != null;
    }

    @Override
    public void close() {
        // Spring 会自动管理客户端，这里只需清空引用
        elasticsearchOperations = null;
    }

    @Override
    public String getType() {
        return TargetEnums.ELASTICSEARCH.name();
    }




    public static class Builder {
        private List<String> clusterNodes;
        private int connectTimeout = 5000; // 5秒
        private int socketTimeout = 10000; // 10秒

        public Builder clusterNodes(List<String> nodes) {
            this.clusterNodes = nodes;
            return this;
        }

        public Builder clusterNodes(String... nodes) {
            this.clusterNodes = Arrays.asList(nodes);
            return this;
        }

        public Builder connectTimeout(int timeoutMs) {
            this.connectTimeout = timeoutMs;
            return this;
        }

        public Builder socketTimeout(int timeoutMs) {
            this.socketTimeout = timeoutMs;
            return this;
        }

        public EsTargetConnector build() {
            return new EsTargetConnector(this);
        }
    }
}
