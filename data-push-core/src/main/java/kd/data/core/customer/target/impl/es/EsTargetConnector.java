package kd.data.core.customer.target.impl.es;

import kd.data.core.customer.target.TargetConnector;
import kd.data.core.customer.target.targetenums.TargetEnums;
import kd.data.core.exception.SyncException;
import lombok.Getter;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.cluster.ClusterHealth;
import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * es
 *
 * @author gaozw
 * @date 2025/8/1 13:12
 */
@Getter
@SuppressWarnings("all")
public class EsTargetConnector implements TargetConnector {


    private ElasticsearchOperations elasticsearchOperations;
    private final List<String> clusterNodes;
    private final int connectTimeout;
    private final int socketTimeout;
    private final String username;
    private final String password;
    private final boolean disableSslVerification;

    private EsTargetConnector(Builder builder) {
        this.clusterNodes = builder.clusterNodes;
        this.connectTimeout = builder.connectTimeout;
        this.socketTimeout = builder.socketTimeout;
        this.username = builder.username;
        this.password = builder.password;
        this.disableSslVerification = builder.disableSslVerification;
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
            // 1. 解析节点协议
            List<NodeInfo> parsedNodes = parseNodes(clusterNodes);
            checkMixedProtocols(parsedNodes);

            // 2. 提取清理后的节点地址
            List<String> cleanNodes = parsedNodes.stream()
                    .map(NodeInfo::getAddress)
                    .collect(Collectors.toList());

            // 3. 创建基础配置
            ClientConfiguration.MaybeSecureClientConfigurationBuilder baseBuilder =
                    ClientConfiguration.builder().connectedTo(cleanNodes.toArray(new String[0]));

            // 4. 动态配置SSL
            ClientConfiguration.TerminalClientConfigurationBuilder configBuilder;
            boolean useSsl = parsedNodes.stream().anyMatch(NodeInfo::isHttps);

            if (useSsl) {
                // HTTPS配置
                if (disableSslVerification) {
                    // 信任自签名证书
                    SSLContext sslContext = createTrustSelfSignedSslContext();
                    configBuilder = baseBuilder.usingSsl(sslContext, NoopHostnameVerifier.INSTANCE);
                } else {
                    // 使用系统默认证书
                    configBuilder = baseBuilder.usingSsl();
                }
            } else {
                // HTTP配置
                configBuilder = baseBuilder;
            }

            // 5. 设置超时和认证
            configBuilder = configBuilder
                    .withConnectTimeout(connectTimeout)
                    .withSocketTimeout(socketTimeout);

            if (username != null && !username.isEmpty()) {
                String pwd = (password != null) ? password : "";
                configBuilder = configBuilder.withBasicAuth(username, pwd);
            }

            ClientConfiguration clientConfiguration = configBuilder.build();
            this.elasticsearchOperations = new ElasticsearchRestTemplate(
                    RestClients.create(clientConfiguration).rest()
            );

            // 6. 验证连接
            if (!isClusterAvailable()) {
                throw new SyncException("Elasticsearch cluster health check failed");
            }
        } catch (Exception e) {
            throw new SyncException("ES connection failed: " + e.getMessage(), e);
        }
    }

    // 创建信任自签名证书的SSL上下文
    private SSLContext createTrustSelfSignedSslContext() {
        try {
            return new SSLContextBuilder()
                    .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSL context for trusting self-signed certificates", e);
        }
    }

    private static class NodeInfo {
        private final String original;
        private final String address;
        private final boolean isHttps;

        NodeInfo(String node) {
            this.original = node;

            if (node.startsWith("https://")) {
                this.address = node.replaceFirst("^https://", "");
                this.isHttps = true;
            } else if (node.startsWith("http://")) {
                this.address = node.replaceFirst("^http://", "");
                this.isHttps = false;
            } else {
                // 默认无协议视为HTTP
                this.address = node;
                this.isHttps = false;
            }
        }

        String getAddress() {
            return address;
        }

        boolean isHttps() {
            return isHttps;
        }
    }

    private List<NodeInfo> parseNodes(List<String> nodes) {
        return nodes.stream()
                .map(NodeInfo::new)
                .collect(Collectors.toList());
    }

    private void checkMixedProtocols(List<NodeInfo> nodes) throws SyncException {
        boolean hasHttp = nodes.stream().anyMatch(node -> !node.isHttps());
        boolean hasHttps = nodes.stream().anyMatch(NodeInfo::isHttps);

        if (hasHttp && hasHttps) {
            throw new SyncException("Mixed HTTP and HTTPS nodes are not supported in the same connector");
        }
    }

    private void configureSsl(ClientConfiguration.MaybeSecureClientConfigurationBuilder configBuilder) {
        try {
            if (disableSslVerification) {
                // 信任自签名证书（开发环境）
                SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                        .build();
                configBuilder.usingSsl(sslContext, NoopHostnameVerifier.INSTANCE);
            } else {
                // 使用系统默认SSL配置（生产环境）
                configBuilder.usingSsl();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure SSL: " + e.getMessage(), e);
        }
    }

    private boolean isClusterAvailable() {
        try {
            ClusterHealth health = elasticsearchOperations.cluster().health();
            return health != null;
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
        private int connectTimeout = 5000;
        private int socketTimeout = 10000;
        private String username;
        private String password;
        private boolean disableSslVerification = true;

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

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder disableSslVerification(boolean disable) {
            this.disableSslVerification = disable;
            return this;
        }

        public EsTargetConnector build() {
            return new EsTargetConnector(this);
        }
    }
}
