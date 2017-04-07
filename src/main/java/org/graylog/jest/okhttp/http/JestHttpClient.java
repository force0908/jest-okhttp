package org.graylog.jest.okhttp.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.searchbox.action.Action;
import io.searchbox.client.AbstractJestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.exception.CouldNotConnectException;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpHostConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dogukan Sonmez
 * @author cihat keser
 */
public class JestHttpClient extends AbstractJestClient {

    private final static Logger log = LoggerFactory.getLogger(JestHttpClient.class);

    protected MediaType requestContentType = MediaType.parse("application/json; utf-8");

    private OkHttpClient okHttpClient;

    private HttpClientContext httpClientContextTemplate;

    /**
     * @throws IOException              in case of a problem or the connection was aborted during request,
     *                                  or in case of a problem while reading the response stream
     * @throws CouldNotConnectException if an {@link HttpHostConnectException} is encountered
     */
    @Override
    public <T extends JestResult> T execute(Action<T> clientRequest) throws IOException {
        Request request = prepareRequest(clientRequest);
        Response response = null;
        try {
            response = executeRequest(request);
            return deserializeResponse(response, request, clientRequest);
        } catch (ConnectException ex) {
            throw new CouldNotConnectException(request.url().toString(), ex);
        } finally {
            if (response != null && response.body() != null) {
                response.close();
            }
        }
    }

    @Override
    public <T extends JestResult> void executeAsync(final Action<T> clientRequest, final JestResultHandler<? super T> resultHandler) {
        final Request request = prepareRequest(clientRequest);
        executeAsyncRequest(clientRequest, resultHandler, request);
    }

    @Override
    public void shutdownClient() {
        super.shutdownClient();

        if (okHttpClient != null) {
            okHttpClient.connectionPool().evictAll();
        }
    }

    protected <T extends JestResult> Request prepareRequest(final Action<T> clientRequest) {
        String elasticSearchRestUrl = getRequestURL(getNextServer(), clientRequest.getURI());
        Request.Builder requestBuilder = constructHttpMethod(clientRequest.getRestMethodName(), elasticSearchRestUrl, clientRequest.getData(gson));

        log.debug("Request method={} url={}", clientRequest.getRestMethodName(), elasticSearchRestUrl);

        // add headers added to action
        for (Entry<String, Object> header : clientRequest.getHeaders().entrySet()) {
            requestBuilder.addHeader(header.getKey(), header.getValue().toString());
        }

        return requestBuilder.build();
    }

    protected Response executeRequest(Request request) throws IOException {
        if (httpClientContextTemplate != null) {
            // return httpClient.execute(request, createContextInstance());
        }

        return okHttpClient.newCall(request).execute();
    }

    protected <T extends JestResult> void executeAsyncRequest(Action<T> clientRequest, JestResultHandler<? super T> resultHandler, Request request) {
        if (httpClientContextTemplate != null) {
            // return asyncClient.execute(request, createContextInstance(), new DefaultCallback<T>(clientRequest, resultHandler));
        }

        okHttpClient.newCall(request).enqueue(new DefaultCallback<T>(clientRequest, resultHandler));
    }

    protected HttpClientContext createContextInstance() {
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(httpClientContextTemplate.getCredentialsProvider());
        context.setAuthCache(httpClientContextTemplate.getAuthCache());

        return context;
    }

    protected Request.Builder constructHttpMethod(String methodName, String url, String payload) {
        Request.Builder requestBuilder = new Request.Builder()
                .url(url);

        final RequestBody requestBody = RequestBody.create(requestContentType, payload == null ? "" : payload);

        if (methodName.equalsIgnoreCase("POST")) {
            requestBuilder = requestBuilder.post(requestBody);
            log.debug("POST method created based on client request");
        } else if (methodName.equalsIgnoreCase("PUT")) {
            requestBuilder = requestBuilder.put(requestBody);
            log.debug("PUT method created based on client request");
        } else if (methodName.equalsIgnoreCase("DELETE")) {
            requestBuilder = requestBuilder.delete(requestBody);
            log.debug("DELETE method created based on client request");
        } else if (methodName.equalsIgnoreCase("GET")) {
            requestBuilder = requestBuilder.get();
            // Required for Multi GET, but throws Exception:
            //   java.lang.IllegalArgumentException: method GET must not have a request body.
            // requestBuilder = requestBuilder.method("GET", requestBody);
            log.debug("GET method created based on client request");
        } else if (methodName.equalsIgnoreCase("HEAD")) {
            requestBuilder = requestBuilder.head();
            log.debug("HEAD method created based on client request");
        }

        return requestBuilder;
    }

    private <T extends JestResult> T deserializeResponse(Response response, final Request httpRequest, Action<T> clientRequest) throws IOException {
        try {
            final ResponseBody responseBody = response.body();
            return clientRequest.createNewElasticSearchResult(
                    responseBody == null ? null : responseBody.string(),
                    response.code(),
                    response.message(),
                    gson
            );
        } catch (com.google.gson.JsonSyntaxException e) {
            for (String mimeType : response.headers("Content-Type")) {
                if (!mimeType.startsWith("application/json")) {
                    // probably a proxy that responded in text/html
                    final String message = "Request " + httpRequest.toString() + " yielded " + mimeType
                            + ", should be json: " + response.protocol() + " " + response.code() + " " + response.message();
                    throw new IOException(message, e);
                }
            }
            throw e;
        }
    }

    public OkHttpClient getOkHttpClient() {
        return okHttpClient;
    }

    public JestHttpClient setOkHttpClient(OkHttpClient okHttpClient) {
        this.okHttpClient = okHttpClient;
        return this;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        this.gson = gson;
    }

    public HttpClientContext getHttpClientContextTemplate() {
        return httpClientContextTemplate;
    }

    public void setHttpClientContextTemplate(HttpClientContext httpClientContext) {
        this.httpClientContextTemplate = httpClientContext;
    }

    @VisibleForTesting
    @Override
    public int getServerPoolSize() {
        return super.getServerPoolSize();
    }

    @VisibleForTesting
    @Override
    public String getNextServer() {
        return super.getNextServer();
    }

    protected class DefaultCallback<T extends JestResult> implements Callback {
        private final Action<T> clientRequest;
        private final JestResultHandler<? super T> resultHandler;

        public DefaultCallback(Action<T> clientRequest, JestResultHandler<? super T> resultHandler) {
            this.clientRequest = clientRequest;
            this.resultHandler = resultHandler;
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            T jestResult = null;
            try {
                jestResult = deserializeResponse(response, call.request(), clientRequest);
            } catch (Exception e) {
                onFailure(call, new IOException(e));
            } catch (Throwable t) {
                onFailure(call, new IOException("Problem during request processing", t));
            }
            if (jestResult != null) {
                resultHandler.completed(jestResult);
            }
        }

        @Override
        public void onFailure(Call call, IOException ex) {
            log.error("Exception occurred during async execution.", ex);
            if (ex instanceof HttpHostConnectException) {
                String host = ((HttpHostConnectException) ex).getHost().toURI();
                resultHandler.failed(new CouldNotConnectException(host, ex));
                return;
            }
            resultHandler.failed(ex);
        }
    }

}
