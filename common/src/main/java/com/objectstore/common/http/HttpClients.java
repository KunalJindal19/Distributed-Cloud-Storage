package com.objectstore.common.http;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/** Builds the HTTP clients used for internal calls: explicit time-outs, optional fault injection. */
public final class HttpClients {

    private HttpClients() {
    }

    public static RestClient restClient(RestClient.Builder builder, Duration connectTimeout, Duration readTimeout,
                                        FaultSettings faults) {
        return builder.clone().requestFactory(requestFactory(connectTimeout, readTimeout, faults)).build();
    }

    public static ClientHttpRequestFactory requestFactory(Duration connectTimeout, Duration readTimeout,
                                                          FaultSettings faults) {
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(readTimeout);
        return faults.enabled() ? new FaultInjectingRequestFactory(factory, faults) : factory;
    }

    public static Duration seconds(double value) {
        return Duration.ofMillis(Math.round(value * 1000));
    }
}
