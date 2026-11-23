package com.objectstore.controller.api;

import com.objectstore.common.web.ApiException;
import com.objectstore.controller.auth.AccountService;
import com.objectstore.controller.auth.AuthInterceptor;
import com.objectstore.controller.auth.AuthUser;
import com.objectstore.controller.util.Timestamps;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Client accounts: {@code register} and {@code login} are open; {@code me} and {@code logout} need a
 * signed-in account (see {@link AuthInterceptor}).
 */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    /** The body of register and login: {@code {"email": "...", "password": "..."}}. */
    public record Credentials(String email, String password) {
    }

    /** An account as the API shows it. */
    public record AccountResponse(String userId, String email, boolean admin) {
        static AccountResponse of(AuthUser user) {
            return new AccountResponse(user.userId(), user.email(), user.admin());
        }
    }

    /** A new session: send {@code Authorization: Bearer <token>} with every request until it expires. */
    public record SessionResponse(String token, String tokenType, String expiresAt, AccountResponse account) {
    }

    private final AccountService accounts;

    public AuthController(AccountService accounts) {
        this.accounts = accounts;
    }

    /** Creates an account. 201; 400 for an invalid email or password; 409 {@code email_taken}. */
    @PostMapping("/register")
    public ResponseEntity<AccountResponse> register(@RequestBody(required = false) Credentials body) {
        Credentials credentials = require(body);
        AuthUser user = accounts.register(credentials.email(), credentials.password());
        return ResponseEntity.created(URI.create("/api/v1/auth/me")).body(AccountResponse.of(user));
    }

    /**
     * Signs in. The token is in the body (for programs) and in an HttpOnly cookie (for the web
     * pages). 401 {@code invalid_credentials} for a wrong email or password.
     */
    @PostMapping("/login")
    public ResponseEntity<SessionResponse> login(@RequestBody(required = false) Credentials body) {
        Credentials credentials = require(body);
        AccountService.Session session = accounts.login(credentials.email(), credentials.password());
        Duration lifetime = Duration.between(Instant.now(), session.expiresAt());
        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, cookie(session.token(), lifetime.isNegative() ? Duration.ZERO : lifetime))
                .body(new SessionResponse(session.token(), "Bearer", Timestamps.format(session.expiresAt()),
                        AccountResponse.of(session.user())));
    }

    /** The signed-in account. */
    @GetMapping("/me")
    public AccountResponse me(@RequestAttribute(AuthInterceptor.USER) AuthUser user) {
        return AccountResponse.of(user);
    }

    /** Ends the session the request was made with (its token or cookie). 204. */
    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest request) {
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            accounts.logout(authorization.substring(7).trim());
        }
        if (request.getCookies() != null) {
            for (Cookie c : request.getCookies()) {
                if (AccountService.COOKIE.equals(c.getName())) {
                    accounts.logout(c.getValue());
                }
            }
        }
        return ResponseEntity.noContent().header(HttpHeaders.SET_COOKIE, cookie("", Duration.ZERO)).build();
    }

    private static Credentials require(Credentials body) {
        if (body == null || body.email() == null || body.password() == null) {
            throw ApiException.badRequest("expected a JSON body {\"email\": \"...\", \"password\": \"...\"}");
        }
        return body;
    }

    private static String cookie(String token, Duration maxAge) {
        return ResponseCookie.from(AccountService.COOKIE, token)
                .httpOnly(true)
                .sameSite("Strict")
                .path("/")
                .maxAge(maxAge)
                .build()
                .toString();
    }
}
