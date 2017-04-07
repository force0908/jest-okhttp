package org.graylog.jest.okhttp.http.okhttp;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import java.io.IOException;

public class BasicAuthenticator implements Authenticator {
    private final String userName;
    private final String password;

    public BasicAuthenticator(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    @Override
    public Request authenticate(Route route, Response response) throws IOException {
        if (response.request().header("Authorization") != null) {
            return null; // Give up, we've already failed to authenticate.
        }

        String credential = Credentials.basic(userName, password);
        return response.request().newBuilder()
                .header("Authorization", credential)
                .build();
    }
}
