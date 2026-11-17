package com.objectstore.common.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.AbstractBufferingClientHttpRequest;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;

/**
 * Wraps a request factory and makes the network unreliable on purpose: delayed, lost (request or
 * response) and duplicated calls. The protocol must survive all of these; the chaos test proves it.
 */
public final class FaultInjectingRequestFactory implements ClientHttpRequestFactory {

    private final ClientHttpRequestFactory delegate;
    private final FaultSettings faults;

    public FaultInjectingRequestFactory(ClientHttpRequestFactory delegate, FaultSettings faults) {
        this.delegate = delegate;
        this.faults = faults;
    }

    @Override
    public ClientHttpRequest createRequest(URI uri, HttpMethod method) {
        return new FaultyRequest(uri, method);
    }

    private final class FaultyRequest extends AbstractBufferingClientHttpRequest {

        private final URI uri;
        private final HttpMethod method;

        FaultyRequest(URI uri, HttpMethod method) {
            this.uri = uri;
            this.method = method;
        }

        @Override
        public HttpMethod getMethod() {
            return method;
        }

        @Override
        public URI getURI() {
            return uri;
        }

        @Override
        protected ClientHttpResponse executeInternal(HttpHeaders headers, byte[] body) throws IOException {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (faults.delayMs() > 0) {
                try {
                    Thread.sleep(random.nextLong(faults.delayMs() + 1));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
            }
            if (random.nextDouble() < faults.dropRate()) {
                if (random.nextBoolean()) {
                    throw new SocketTimeoutException("fault injection: request lost");
                }
                discard(send(headers, body));
                throw new SocketTimeoutException("fault injection: response lost");
            }
            if (random.nextDouble() < faults.dupRate()) {
                discard(send(headers, body));
            }
            return send(headers, body);
        }

        private ClientHttpResponse send(HttpHeaders headers, byte[] body) throws IOException {
            ClientHttpRequest request = delegate.createRequest(uri, method);
            request.getHeaders().putAll(headers);
            if (body.length > 0) {
                request.getBody().write(body);
            }
            return request.execute();
        }

        private static void discard(ClientHttpResponse response) throws IOException {
            try (response; InputStream in = response.getBody()) {
                in.transferTo(java.io.OutputStream.nullOutputStream());
            }
        }
    }
}
