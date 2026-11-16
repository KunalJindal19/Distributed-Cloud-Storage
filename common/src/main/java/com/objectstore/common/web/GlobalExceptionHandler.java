package com.objectstore.common.web;

import com.objectstore.common.dto.ApiError;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.util.DisconnectedClientHelper;

/**
 * Turns every failure into the same JSON shape, {@code {"error": "...", "detail": "..."}}, so a
 * client never receives an HTML or plain-text error page.
 *
 * <ul>
 *   <li>{@link ApiException}: the status and code chosen by the code that raised it.</li>
 *   <li>Spring MVC's own exceptions (missing part, bad parameter type, unknown route, unsupported
 *       method, upload too large, ...): the status Spring assigns, with a generic code.</li>
 *   <li>Anything else: 500 {@code internal_error}, with the stack trace in the log only.</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiError> handleApiException(ApiException ex, HttpServletResponse response) {
        if (response.isCommitted()) {
            // Part of the body is already on the wire (a download failed mid-stream). No error
            // body can be sent any more; rethrowing makes the container drop the connection so
            // the client sees a truncated transfer rather than a seemingly complete one.
            throw ex;
        }
        if (ex.status().is5xxServerError()) {
            log.warn("[api] {} {}: {}", ex.status().value(), ex.code(), ex.getMessage());
        }
        return json(ex.status(), new HttpHeaders(), new ApiError(ex.code(), ex.getMessage()));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiError> handleConstraintViolation(ConstraintViolationException ex) {
        String detail = ex.getConstraintViolations().stream()
                .map(ConstraintViolation::getMessage)
                .sorted()
                .collect(Collectors.joining("; "));
        return json(HttpStatus.BAD_REQUEST, new HttpHeaders(), new ApiError(ErrorCodes.BAD_REQUEST, detail));
    }

    /** A request that was expected to be multipart/form-data but is not, or is malformed. */
    @ExceptionHandler(MultipartException.class)
    public ResponseEntity<ApiError> handleMultipart(MultipartException ex) {
        return json(HttpStatus.BAD_REQUEST, new HttpHeaders(), new ApiError(ErrorCodes.BAD_REQUEST,
                "expected a valid multipart/form-data request with a 'file' part"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex, HttpServletResponse response) throws Exception {
        if (response.isCommitted() || DisconnectedClientHelper.isClientDisconnectedException(ex)) {
            throw ex;
        }
        log.error("[api] unhandled exception", ex);
        return json(HttpStatus.INTERNAL_SERVER_ERROR, new HttpHeaders(),
                new ApiError(ErrorCodes.INTERNAL_ERROR, "an unexpected error occurred"));
    }

    /** Single exit point for all the exceptions Spring MVC itself knows how to map to a status. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
            Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
        if (request instanceof ServletWebRequest servletRequest
                && servletRequest.getResponse() != null
                && servletRequest.getResponse().isCommitted()) {
            return null;
        }
        ApiError error;
        if (ex instanceof NoResourceFoundException || ex instanceof NoHandlerFoundException) {
            error = new ApiError(ErrorCodes.NOT_FOUND, "route not found");
        } else {
            error = new ApiError(ErrorCodes.forStatus(statusCode.value()), detailOf(ex, statusCode));
        }
        if (statusCode.is5xxServerError()) {
            log.error("[api] {} {}", statusCode.value(), error.detail(), ex);
        }
        return ResponseEntity.status(statusCode)
                .headers(headers)
                .contentType(MediaType.APPLICATION_JSON)
                .body(error);
    }

    private static String detailOf(Exception ex, HttpStatusCode statusCode) {
        if (ex instanceof MethodArgumentNotValidException invalid) {
            return invalid.getBindingResult().getAllErrors().stream()
                    .map(MessageSourceResolvable::getDefaultMessage)
                    .sorted()
                    .collect(Collectors.joining("; "));
        }
        if (ex instanceof HandlerMethodValidationException invalid) {
            return invalid.getAllErrors().stream()
                    .map(MessageSourceResolvable::getDefaultMessage)
                    .sorted()
                    .collect(Collectors.joining("; "));
        }
        if (ex instanceof MethodArgumentTypeMismatchException mismatch) {
            Class<?> required = mismatch.getRequiredType();
            return "parameter '%s' must be of type %s".formatted(
                    mismatch.getName(), required == null ? "the expected type" : required.getSimpleName());
        }
        if (ex instanceof HttpMessageNotReadableException) {
            return "request body is missing or is not valid JSON";
        }
        if (ex instanceof ErrorResponse errorResponse) {
            String detail = errorResponse.getBody().getDetail();
            if (detail != null && !detail.isBlank()) {
                return detail;
            }
        }
        HttpStatus status = HttpStatus.resolve(statusCode.value());
        return status == null ? "request failed" : status.getReasonPhrase();
    }

    private static ResponseEntity<ApiError> json(HttpStatus status, HttpHeaders headers, ApiError body) {
        return ResponseEntity.status(status)
                .headers(headers)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body);
    }
}
