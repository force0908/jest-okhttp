package org.graylog.jest.okhttp;

import io.searchbox.client.JestClient;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.discovery.NodeChecker;
import okhttp3.Authenticator;
import okhttp3.HttpUrl;
import org.apache.http.client.protocol.HttpClientContext;
import org.graylog.jest.okhttp.config.HttpClientConfig;
import org.graylog.jest.okhttp.http.JestHttpClient;
import org.graylog.jest.okhttp.http.okhttp.BasicAuthenticator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Dogukan Sonmez
 * @author cihat keser
 */
public class JestClientFactoryTest {
    @Test
    public void clientCreationWithDiscovery() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").discoveryEnabled(true).build());
        JestHttpClient jestClient = (JestHttpClient) factory.getObject();
        assertNotNull(jestClient);
        assertNotNull(jestClient.getOkHttpClient());
        assertNotNull(factory.getConnectionPool());
        assertEquals(jestClient.getServerPoolSize(), 1);
    }

    @Test
    public void clientCreationWithNullClientConfig() {
        JestClientFactory factory = new JestClientFactory();
        JestHttpClient jestClient = (JestHttpClient) factory.getObject();
        assertTrue(jestClient != null);
        assertNotNull(jestClient.getOkHttpClient());
        assertEquals(jestClient.getServerPoolSize(), 1);
        assertEquals("server list should contain localhost:9200",
                "http://localhost:9200", jestClient.getNextServer());
    }

    @Test
    public void multiThreadedClientCreation() {
        JestClientFactory factory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .maxTotalConnection(20)
                .defaultMaxTotalConnectionPerRoute(10)
                .build();

        factory.setHttpClientConfig(httpClientConfig);
        JestHttpClient jestClient = (JestHttpClient) factory.getObject();

        assertTrue(jestClient != null);
        assertEquals(jestClient.getServerPoolSize(), 1);
        assertEquals("server list should contain localhost:9200", "http://localhost:9200", jestClient.getNextServer());

        /*
        final NHttpClientConnectionManager nConnectionManager = factory.getAsyncConnectionManager();
        assertTrue(nConnectionManager instanceof PoolingNHttpClientConnectionManager);
        assertEquals(10, ((PoolingNHttpClientConnectionManager) nConnectionManager).getDefaultMaxPerRoute());
        assertEquals(20, ((PoolingNHttpClientConnectionManager) nConnectionManager).getMaxTotal());
        */
    }

    @Test
    public void clientCreationWithDiscoveryAndOverriddenNodeChecker() {
        JestClientFactory factory = Mockito.spy(new ExtendedJestClientFactory());
        HttpClientConfig httpClientConfig = Mockito.spy(new HttpClientConfig.Builder("http://localhost:9200")
                .discoveryEnabled(true)
                .build());
        factory.setHttpClientConfig(httpClientConfig);
        JestHttpClient jestClient = (JestHttpClient) factory.getObject();
        assertTrue(jestClient != null);
        assertNotNull(jestClient.getOkHttpClient());
        assertEquals(jestClient.getServerPoolSize(), 1);
        assertEquals("server list should contain localhost:9200",
                "http://localhost:9200", jestClient.getNextServer());
        Mockito.verify(factory, Mockito.times(1)).createNodeChecker(Mockito.any(JestHttpClient.class),
                                                                    Mockito.same(httpClientConfig));
    }

    @Test
    public void clientCreationWithPreemptiveAuth() {
        JestClientFactory factory = new JestClientFactory();
        Authenticator credentialsProvider = new BasicAuthenticator("someUser", "somePassword");
        HttpUrl targetHost1 = HttpUrl.parse("http://targetHostName1:80");
        HttpUrl targetHost2 = HttpUrl.parse("http://targetHostName2:80");

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("someUri")
                .authenticator(credentialsProvider)
                .preemptiveAuthTargetHosts(new HashSet<>(asList(targetHost1, targetHost2)))
                .build();

        factory.setHttpClientConfig(httpClientConfig);
        JestHttpClient jestHttpClient = (JestHttpClient) factory.getObject();
        HttpClientContext httpClientContext = jestHttpClient.getHttpClientContextTemplate();

        // assertNotNull(httpClientContext.getAuthCache().get(targetHost1));
        // assertNotNull(httpClientContext.getAuthCache().get(targetHost2));
        // assertEquals(credentialsProvider, httpClientContext.getCredentialsProvider());
    }

    class ExtendedJestClientFactory extends JestClientFactory {
        @Override
        protected NodeChecker createNodeChecker(JestHttpClient client, HttpClientConfig httpClientConfig) {
            return new OtherNodeChecker(client, httpClientConfig);
        }
    }

    class OtherNodeChecker extends NodeChecker {
        OtherNodeChecker(JestClient jestClient, ClientConfig clientConfig) {
            super(jestClient, clientConfig);
        }
    }
}
