package com.objectstore.client;

/** Ends the command with a specific exit code after printing {@code message} to stderr. */
final class CliException extends RuntimeException {

    private final int exitCode;

    CliException(int exitCode, String message) {
        super(message);
        this.exitCode = exitCode;
    }

    int exitCode() {
        return exitCode;
    }
}
