package com.objectstore.common.web;

import com.objectstore.common.dto.ApiError;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.webmvc.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Replaces Spring Boot's default error page for failures that never reach a controller (and
 * therefore never reach {@link GlobalExceptionHandler}), keeping the JSON error shape there too.
 */
@RestController
public class JsonErrorController implements ErrorController {

    @RequestMapping("${server.error.path:/error}")
    public ResponseEntity<ApiError> error(HttpServletRequest request) {
        Object statusAttribute = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        int status = statusAttribute instanceof Integer code ? code : 500;
        HttpStatus resolved = HttpStatus.resolve(status);
        if (resolved == null) {
            resolved = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        String detail = resolved == HttpStatus.NOT_FOUND ? "route not found" : resolved.getReasonPhrase();
        return ResponseEntity.status(resolved)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new ApiError(ErrorCodes.forStatus(resolved.value()), detail));
    }
}
