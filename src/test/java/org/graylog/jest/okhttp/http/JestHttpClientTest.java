package org.graylog.jest.okhttp.http;

import io.searchbox.core.Search;
import io.searchbox.core.search.sort.Sort;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;
import okio.Okio;
import okio.Sink;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.graylog.jest.okhttp.JestClientFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Dogukan Sonmez
 */
public class JestHttpClientTest {

    private JestHttpClient client;

    @Before
    public void init() {
        client = new JestHttpClient();
    }

    @After
    public void cleanup() {
        client = null;
    }

    @Test
    public void constructGetHttpMethod() throws UnsupportedEncodingException {
        Request request = client.constructHttpMethod("GET", "http://localhost/jest/get", null).build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "get"), request.url().pathSegments());
        assertEquals(request.method(), "GET");
    }

    @Test
    public void constructCompressedPutHttpMethod() throws UnsupportedEncodingException {
        client.setRequestCompressionEnabled(true);

        Request request = client.constructHttpMethod("PUT", "http://localhost/jest/put", "data").build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "put"), request.url().pathSegments());
        assertEquals(request.method(), "PUT");
        assertNotNull(request.body());
    }

    @Test
    public void constructPutHttpMethod() throws UnsupportedEncodingException {
        Request request = client.constructHttpMethod("PUT", "http://localhost/jest/put", "data").build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "put"), request.url().pathSegments());
        assertEquals(request.method(), "PUT");
        assertNotNull(request.body());
    }

    @Test
    public void constructPostHttpMethod() throws UnsupportedEncodingException {
        Request request = client.constructHttpMethod("POST", "http://localhost/jest/post", "data").build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "post"), request.url().pathSegments());
        assertEquals(request.method(), "POST");
    }

    @Test
    public void constructDeleteHttpMethod() throws UnsupportedEncodingException {
        Request request = client.constructHttpMethod("DELETE", "http://localhost/jest/delete", null).build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "delete"), request.url().pathSegments());
        assertEquals(request.method(), "DELETE");
    }

    @Test
    public void constructHeadHttpMethod() throws UnsupportedEncodingException {
        Request request = client.constructHttpMethod("HEAD", "http://localhost/jest/head", null).build();
        assertNotNull(request);
        assertEquals(Arrays.asList("jest", "head"), request.url().pathSegments());
        assertEquals(request.method(), "HEAD");
    }

    @Test
    public void addHeadersToRequest() throws IOException {
        /*
        final String headerKey = "foo";
        final String headerValue = "bar";

        CloseableHttpResponse httpResponseMock = mock(CloseableHttpResponse.class);
        doReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK")).when(httpResponseMock).getStatusLine();
        doReturn(null).when(httpResponseMock).getEntity();

        OkHttpClient closeableHttpClientMock = mock(OkHttpClient.class);
        doReturn(httpResponseMock).when(closeableHttpClientMock).execute(any(Request.class));

        // Construct a new Jest client according to configuration via factory
        JestHttpClient clientWithMockedHttpClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").build());
        clientWithMockedHttpClient = (JestHttpClient) factory.getObject();
        clientWithMockedHttpClient.setOkHttpClient(closeableHttpClientMock);

        // could reuse the above setup for testing core types against expected
        // Request (more of an end to end test)

        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"filtered\" : {\n" +
                "            \"query\" : {\n" +
                "                \"query_string\" : {\n" +
                "                    \"query\" : \"test\"\n" +
                "                }\n" +
                "            },\n" +
                "            \"filter\" : {\n" +
                "                \"term\" : { \"user\" : \"kimchy\" }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("twitter")
                .addType("tweet")
                .setHeader(headerKey, headerValue)
                .build();
        // send request (not really)
        clientWithMockedHttpClient.execute(search);

        verify(closeableHttpClientMock).execute(argThat(new ArgumentMatcher<Request>() {
            @Override
            public boolean matches(Object o) {
                boolean retval = false;

                if (o instanceof Request) {
                    Request req = (Request) o;
                    Header header = req.getFirstHeader(headerKey);
                    if (header != null) {
                        retval = headerValue.equals(header.getValue());
                    }
                }

                return retval;
            }
        }));
        */
    }

    @SuppressWarnings("unchecked")
    @Test
    public void prepareShouldNotRewriteLongToDoubles() throws IOException {
        // Construct a new Jest client according to configuration via factory
        JestHttpClient clientWithMockedHttpClient = (JestHttpClient) new JestClientFactory().getObject();

        // Construct mock Sort
        Sort mockSort = mock(Sort.class);

        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\" : {\n" +
                "            \"should\" : [\n" +
                "                { \"term\" : { \"id\" : 1234 } },\n" +
                "                { \"term\" : { \"id\" : 567800000000000000000 } }\n" +
                "            ]\n" +
                "         }\n" +
                "     }\n" +
                "}";
        Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("twitter")
                .addType("tweet")
                .addSort(mockSort)
                .build();

        // Create Request
        Request request = clientWithMockedHttpClient.prepareRequest(search);

        // Extract Payload
        RequestBody entity = request.body();

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (final Sink sink = Okio.sink(byteArrayOutputStream);
             final BufferedSink buffer = Okio.buffer(sink)) {
            entity.writeTo(buffer);
        }
        String payload = new String(byteArrayOutputStream.toByteArray());

        // Verify payload does not have a double
        assertFalse(payload.contains("1234.0"));
        assertTrue(payload.contains("1234"));

        // Verify payload does not use scientific notation
        assertFalse(payload.contains("5.678E20"));
        assertTrue(payload.contains("567800000000000000000"));
    }

    @Test
    public void createContextInstanceWithPreemptiveAuth() {
        AuthCache authCacheMock = mock(AuthCache.class);
        CredentialsProvider credentialsProviderMock = mock(CredentialsProvider.class);

        HttpClientContext httpClientContextTemplate = HttpClientContext.create();
        httpClientContextTemplate.setAuthCache(authCacheMock);
        httpClientContextTemplate.setCredentialsProvider(credentialsProviderMock);

        JestHttpClient jestHttpClient = (JestHttpClient) new JestClientFactory().getObject();
        jestHttpClient.setHttpClientContextTemplate(httpClientContextTemplate);

        HttpClientContext httpClientContextResult = jestHttpClient.createContextInstance();

        assertEquals(authCacheMock, httpClientContextResult.getAuthCache());
        assertEquals(credentialsProviderMock, httpClientContextResult.getCredentialsProvider());
    }

}
