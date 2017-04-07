package org.graylog.jest.okhttp.config;

import okhttp3.Address;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Dns;
import okhttp3.HttpUrl;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author cihat keser
 */
public class HttpClientConfigTest {

    @Test
    public void defaultInstances() {
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("localhost").build();

        assertNotNull(httpClientConfig.getSslSocketFactory());
        assertNotNull(httpClientConfig.getPlainSocketFactory());
    }

    @Test
    public void defaultCredentials() throws IOException {
        String user = "ceo";
        String password = "12345";

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("localhost")
                .defaultCredentials(user, password)
                .build();

        Authenticator authenticator = httpClientConfig.getAuthenticator();
        final Address address = new Address(
                "localhost", 80,
                Dns.SYSTEM, SocketFactory.getDefault(), null, null, null,
                Authenticator.NONE, Proxy.NO_PROXY, Collections.emptyList(), Collections.emptyList(),
                ProxySelector.getDefault());
        final Route route = new Route(address, Proxy.NO_PROXY, InetSocketAddress.createUnresolved("localhost", 80));
        final Request stubRequest = new Request.Builder()
                .url("http://localhost:80/")
                .get()
                .build();
        final Response response = new Response.Builder()
                .request(stubRequest)
                .protocol(Protocol.HTTP_1_1)
                .code(401)
                .message("Unauthorized")
                .build();

        final Request request = authenticator.authenticate(route, response);
        assertEquals(Credentials.basic(user, password), request.header("Authorization"));
    }

    @Test
    public void customCredentialProvider() {
        Authenticator customAuthenticator = (route, response) -> null;

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("localhost")
                .authenticator(customAuthenticator)
                .build();

        assertEquals(customAuthenticator, httpClientConfig.getAuthenticator());
    }

    @Test
    public void preemptiveAuth() {
        HttpUrl targetHost = HttpUrl.parse("http://targetHost:80");
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("localhost")
                .defaultCredentials("someUser", "somePassword")
                .setPreemptiveAuth(targetHost)
                .build();

        assertThat(httpClientConfig.getPreemptiveAuthTargetHosts(), hasItem(targetHost));
    }

    @Test
    public void preemptiveAuthWithMultipleTargetHosts() {
        final Set<HttpUrl> targetHosts = new HashSet<>(Arrays.asList(
                HttpUrl.parse("http://host1:80"),
                HttpUrl.parse("https://host1:81")
        ));
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("localhost")
                .defaultCredentials("someUser", "somePassword")
                .preemptiveAuthTargetHosts(new HashSet<>(targetHosts))
                .build();

        assertThat(httpClientConfig.getPreemptiveAuthTargetHosts(), is(targetHosts));
    }

    @Test(expected = IllegalArgumentException.class)
    @Ignore
    public void preemptiveAuthWithoutCredentials() {
        new HttpClientConfig.Builder("localhost")
                .setPreemptiveAuth(HttpUrl.parse("http://localhost:80"))
                .build();
        fail("Builder should have thrown an exception if preemptive authentication is set without setting credentials");
    }

}
