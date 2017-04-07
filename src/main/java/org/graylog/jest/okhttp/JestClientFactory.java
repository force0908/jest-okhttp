package org.graylog.jest.okhttp;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import io.searchbox.client.config.discovery.NodeChecker;
import io.searchbox.client.config.idle.IdleConnectionReaper;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.graylog.jest.okhttp.config.HttpClientConfig;
import org.graylog.jest.okhttp.config.idle.HttpReapableConnectionManager;
import org.graylog.jest.okhttp.http.JestHttpClient;
import org.graylog.jest.okhttp.http.okhttp.GzipRequestInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Dogukan Sonmez
 */
public class JestClientFactory {

    private static final Logger log = LoggerFactory.getLogger(JestClientFactory.class);
    private HttpClientConfig httpClientConfig;

    public JestClient getObject() {
        JestHttpClient client = new JestHttpClient();

        if (httpClientConfig == null) {
            log.debug("There is no configuration to create http client. Going to create simple client with default values");
            httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();
        }

        client.setRequestCompressionEnabled(httpClientConfig.isRequestCompressionEnabled());
        client.setServers(httpClientConfig.getServerList());
        final ConnectionPool connectionPool = getConnectionPool();
        client.setOkHttpClient(createOkHttpClient(connectionPool));

        // set custom gson instance
        Gson gson = httpClientConfig.getGson();
        if (gson == null) {
            log.info("Using default GSON instance");
        } else {
            log.info("Using custom GSON instance");
            client.setGson(gson);
        }

        // set discovery (should be set after setting the httpClient on jestClient)
        if (httpClientConfig.isDiscoveryEnabled()) {
            log.info("Node Discovery enabled...");
            if (!Strings.isNullOrEmpty(httpClientConfig.getDiscoveryFilter())) {
                log.info("Node Discovery filtering nodes on \"{}\"", httpClientConfig.getDiscoveryFilter());
            }
            NodeChecker nodeChecker = createNodeChecker(client, httpClientConfig);
            client.setNodeChecker(nodeChecker);
            nodeChecker.startAsync();
            nodeChecker.awaitRunning();
        } else {
            log.info("Node Discovery disabled...");
        }

        // schedule idle connection reaping if configured
        if (httpClientConfig.getMaxConnectionIdleTime() > 0) {
            log.info("Idle connection reaping enabled...");

            IdleConnectionReaper reaper = new IdleConnectionReaper(httpClientConfig, new HttpReapableConnectionManager(connectionPool));
            client.setIdleConnectionReaper(reaper);
            reaper.startAsync();
            reaper.awaitRunning();
        } else {
            log.info("Idle connection reaping disabled...");
        }

        // TODO: Find out how to implement this in OkHttp. Maybe using an Interceptor?
        Set<HttpUrl> preemptiveAuthTargetHosts = httpClientConfig.getPreemptiveAuthTargetHosts();
        if (!preemptiveAuthTargetHosts.isEmpty()) {
            log.info("Authentication cache set for preemptive authentication");
            // client.setHttpClientContextTemplate(createPreemptiveAuthContext(preemptiveAuthTargetHosts));
        }

        return client;
    }

    public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
        this.httpClientConfig = httpClientConfig;
    }

    private OkHttpClient createOkHttpClient(ConnectionPool connectionPool) {
        final OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectionPool(connectionPool)
                .connectTimeout(httpClientConfig.getConnTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(httpClientConfig.getReadTimeout(), TimeUnit.MILLISECONDS)
                .writeTimeout(httpClientConfig.getWriteTimeout(), TimeUnit.MILLISECONDS)
                .authenticator(httpClientConfig.getAuthenticator())
                .socketFactory(httpClientConfig.getPlainSocketFactory())
                .sslSocketFactory(httpClientConfig.getSslSocketFactory(), httpClientConfig.getTrustManager())
                .proxy(httpClientConfig.getProxy())
                .proxyAuthenticator(httpClientConfig.getProxyAuthenticator())
                .proxySelector(httpClientConfig.getProxySelector());

        if (httpClientConfig.isRequestCompressionEnabled()) {
            clientBuilder.addInterceptor(new GzipRequestInterceptor());
        }

        return configureHttpClient(clientBuilder).build();
    }

    /**
     * Extension point
     * <p>
     * Example:
     * </p>
     * <pre>
     * final JestClientFactory factory = new JestClientFactory() {
     *    {@literal @Override}
     *  	protected OkHttpClient.Builder configureHttpClient(OkHttpClient.Builder builder) {
     *  		return builder.addInterceptor(...);
     *    }
     * }
     * </pre>
     */
    protected OkHttpClient.Builder configureHttpClient(final OkHttpClient.Builder builder) {
        return builder;
    }

    // Extension point
    protected ConnectionPool getConnectionPool() {
        // TODO
        ConnectionPool connectionPool;
        connectionPool = new ConnectionPool();

        return connectionPool;
    }

    // Extension point
    protected NodeChecker createNodeChecker(JestHttpClient client, HttpClientConfig httpClientConfig) {
        return new NodeChecker(client, httpClientConfig);
    }

    // Extension point
    /* TODO: Find out how to implement this in OkHttp. Maybe using an Interceptor?
    protected HttpClientContext createPreemptiveAuthContext(Set<HttpUrl> targetHosts) {
        /*
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(httpClientConfig.getCredentialsProvider());
        context.setAuthCache(createBasicAuthCache(targetHosts));

        return context;
    }

    private AuthCache createBasicAuthCache(Set<HttpHost> targetHosts) {
        AuthCache authCache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        for (HttpHost eachTargetHost : targetHosts) {
            authCache.put(eachTargetHost, basicAuth);
        }

        return authCache;
    }
    */
}
