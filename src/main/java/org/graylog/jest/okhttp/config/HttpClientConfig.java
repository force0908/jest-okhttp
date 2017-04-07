package org.graylog.jest.okhttp.config;

import io.searchbox.client.config.ClientConfig;
import okhttp3.Authenticator;
import okhttp3.HttpUrl;
import org.graylog.jest.okhttp.http.okhttp.BasicAuthenticator;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.net.Proxy;
import java.net.ProxySelector;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Dogukan Sonmez
 * @author cihat keser
 */
public class HttpClientConfig extends ClientConfig {

    private int writeTimeout;
    private final Authenticator authenticator;
    private final SocketFactory plainSocketFactory;
    private final SSLSocketFactory sslSocketFactory;
    private final X509TrustManager trustManager;
    private final Proxy proxy;
    private final Authenticator proxyAuthenticator;
    private final ProxySelector proxySelector;
    private Set<HttpUrl> preemptiveAuthTargetHosts;

    public HttpClientConfig(Builder builder) {
        super(builder);
        this.writeTimeout = builder.writeTimeout;
        this.authenticator = builder.authenticator;
        this.plainSocketFactory = builder.plainSocketFactory;
        this.sslSocketFactory = builder.sslSocketFactory;
        this.trustManager = builder.trustManager;
        this.proxy = builder.proxy;
        this.proxyAuthenticator = builder.proxyAuthenticator;
        this.proxySelector = builder.proxySelector;
        this.preemptiveAuthTargetHosts = builder.preemptiveAuthTargetHosts;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public SocketFactory getPlainSocketFactory() {
        return plainSocketFactory;
    }

    public SSLSocketFactory getSslSocketFactory() {
        return sslSocketFactory;
    }

    public X509TrustManager getTrustManager() {
        return trustManager;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public Authenticator getProxyAuthenticator() {
        return proxyAuthenticator;
    }

    public ProxySelector getProxySelector() {
        return proxySelector;
    }

    public Set<HttpUrl> getPreemptiveAuthTargetHosts() {
        return preemptiveAuthTargetHosts;
    }

    public static class Builder extends ClientConfig.AbstractBuilder<HttpClientConfig, Builder> {

        private int writeTimeout = 3000;
        private Authenticator authenticator;
        private SocketFactory plainSocketFactory;
        private SSLSocketFactory sslSocketFactory;
        private X509TrustManager trustManager;
        private Proxy proxy;
        private Authenticator proxyAuthenticator;
        private ProxySelector proxySelector;
        private Set<HttpUrl> preemptiveAuthTargetHosts = Collections.emptySet();

        public Builder(HttpClientConfig httpClientConfig) {
            super(httpClientConfig);
            this.writeTimeout = httpClientConfig.writeTimeout;
            this.authenticator = httpClientConfig.authenticator;
            this.plainSocketFactory = httpClientConfig.plainSocketFactory;
            this.sslSocketFactory = httpClientConfig.sslSocketFactory;
            this.trustManager = httpClientConfig.trustManager;
            this.proxy = httpClientConfig.proxy;
            this.proxyAuthenticator = httpClientConfig.proxyAuthenticator;
            this.proxySelector = httpClientConfig.proxySelector;
            this.preemptiveAuthTargetHosts = httpClientConfig.preemptiveAuthTargetHosts;
        }

        public Builder(Collection<String> serverUris) {
            super(serverUris);
        }

        public Builder(String serverUri) {
            super(serverUri);
        }

        public Builder writeTimeout(int writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        /**
         * Set a custom instance of an implementation of <code>CredentialsProvider</code>.
         * This method will override any previous credential setting (including <code>defaultCredentials</code>) on this builder instance.
         */
        public Builder authenticator(Authenticator authenticator) {
            this.authenticator = authenticator;
            return this;
        }

        public Builder defaultCredentials(String username, String password) {
            this.authenticator = new BasicAuthenticator(username, password);
            return this;
        }

        /**
         * Sets the socket factory that will be used by <b>sync</b> client for HTTPS scheme.
         * <p>
         * <code>PlainConnectionSocketFactory.getSocketFactory()</code> is used by default.
         * </p>
         *
         * @param socketFactory socket factory instance that will be registered for <code>http</code> scheme.
         * @see SocketFactory
         */
        public Builder plainSocketFactory(SocketFactory socketFactory) {
            this.plainSocketFactory = socketFactory;
            return this;
        }

        /**
         * Sets the socket factory that will be used by <b>sync</b> client for HTTP scheme.
         * <p>
         * <code>SSLConnectionSocketFactory.getSocketFactory()</code> is used by default.
         * </p><p>
         * A bad example of trust-all socket factory creation can be done as below:
         * </p>
         * <pre>
         * // trust ALL certificates
         * SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
         *     public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
         *         return true;
         *     }
         * }).build();
         *
         * // skip hostname checks
         * HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
         *
         * SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
         * </pre>
         *
         * @param socketFactory socket factory instance that will be registered for <code>https</code> scheme.
         * @see SSLSocketFactory
         */
        public Builder sslSocketFactory(SSLSocketFactory socketFactory) {
            this.sslSocketFactory = socketFactory;
            return this;
        }

        public Builder trustManager(X509TrustManager trustManager) {
            this.trustManager = trustManager;
            return this;
        }

        /**
         * Sets preemptive authentication for the specified <b>target host</b> by pre-populating an authentication data cache.
         * <p>
         * It is mandatory to set a credentials provider to use preemptive authentication.
         * </p><p>
         * If preemptive authentication is set without setting a credentials provider an exception will be thrown.
         * </p>
         */
        public Builder setPreemptiveAuth(HttpUrl targetHost) {
            return preemptiveAuthTargetHosts(Collections.singleton(targetHost));
        }

        /**
         * Sets preemptive authentication for the specified set of <b>target hosts</b> by pre-populating an authentication data cache.
         * <p>
         * It is mandatory to set a credentials provider to use preemptive authentication.
         * </p><p>
         * If preemptive authentication is set without setting a credentials provider an exception will be thrown.
         * </p>
         *
         * @param preemptiveAuthTargetHosts set of hosts targeted for preemptive authentication
         */
        public Builder preemptiveAuthTargetHosts(Set<HttpUrl> preemptiveAuthTargetHosts) {
            if (preemptiveAuthTargetHosts != null) {
                this.preemptiveAuthTargetHosts = new HashSet<>(preemptiveAuthTargetHosts);
            }
            return this;
        }

        public Builder proxy(Proxy proxy) {
            this.proxy = proxy;
            return this;
        }

        public Builder proxyAuthenticator(Authenticator proxyAuthenticator) {
            this.proxyAuthenticator = proxyAuthenticator;
            return this;
        }

        public Builder proxySelector(ProxySelector proxySelector) {
            this.proxySelector = proxySelector;
            return this;
        }

        public HttpClientConfig build() {
            // Lazily initialize if necessary, as the call can be expensive when done eagerly.
            if (this.authenticator == null) {
                this.authenticator = Authenticator.NONE;
            }
            if (this.plainSocketFactory == null) {
                this.plainSocketFactory = SocketFactory.getDefault();
            }
            if (this.trustManager == null) {
                this.trustManager = systemDefaultTrustManager();
            }
            if (this.sslSocketFactory == null) {
                this.sslSocketFactory = systemDefaultSslSocketFactory(trustManager);
            }
            if (this.proxy == null) {
                this.proxy = Proxy.NO_PROXY;
            }
            if (this.proxyAuthenticator == null) {
                this.proxyAuthenticator = Authenticator.NONE;
            }
            if (this.proxySelector == null) {
                this.proxySelector = ProxySelector.getDefault();
            }

            if (preemptiveAuthSetWithoutCredentials()) {
                throw new IllegalArgumentException("Preemptive authentication set without credentials provider");
            }

            return new HttpClientConfig(this);
        }

        private boolean preemptiveAuthSetWithoutCredentials() {
            return !preemptiveAuthTargetHosts.isEmpty() && authenticator == null;
        }

        private SSLSocketFactory systemDefaultSslSocketFactory(X509TrustManager trustManager) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[]{trustManager}, null);
                return sslContext.getSocketFactory();
            } catch (GeneralSecurityException e) {
                throw new AssertionError(); // The system has no TLS. Just give up.
            }
        }

        private X509TrustManager systemDefaultTrustManager() {
            try {
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init((KeyStore) null);
                TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
                if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                    throw new IllegalStateException("Unexpected default trust managers:"
                            + Arrays.toString(trustManagers));
                }
                return (X509TrustManager) trustManagers[0];
            } catch (GeneralSecurityException e) {
                throw new AssertionError(); // The system has no TLS. Just give up.
            }
        }
    }
}
