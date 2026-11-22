package com.objectstore.controller.cluster;

/**
 * Any failed call to a worker: a non-2xx answer, a time-out, or a connection error. Callers never
 * see transport-specific exceptions, only this one.
 */
public class WorkerException extends RuntimeException {

    private final String workerUrl;
    private final Integer status;

    public WorkerException(String workerUrl, Integer status, String detail, Throwable cause) {
        super(detail, cause);
        this.workerUrl = workerUrl;
        this.status = status;
    }

    public String workerUrl() {
        return workerUrl;
    }

    /** The HTTP status the worker answered with, or {@code null} if it did not answer at all. */
    public Integer status() {
        return status;
    }

    public boolean isNotFound() {
        return status != null && status == 404;
    }

    public boolean isConflict() {
        return status != null && status == 409;
    }

    /**
     * True if the request certainly never reached the worker: the connection was refused, could not
     * be opened in time, or the host could not be resolved. A time-out or a lost answer after the
     * request was sent is not that: the worker may have acted on it.
     */
    public boolean neverSent() {
        if (status != null) {
            return false;
        }
        for (Throwable t = getCause(); t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof java.net.ConnectException || t instanceof java.net.http.HttpConnectTimeoutException
                    || t instanceof java.net.UnknownHostException
                    || t instanceof java.nio.channels.UnresolvedAddressException) {
                return true;
            }
        }
        return false;
    }
}
