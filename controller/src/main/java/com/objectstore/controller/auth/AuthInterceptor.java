package com.objectstore.controller.auth;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Every public API request ({@code /api/v1/**} except registering and signing in) must say which
 * account it is made with, in one of three ways:
 *
 * <ul>
 *   <li>{@code Authorization: Bearer <token>}, the token returned by {@code POST /api/v1/auth/login}
 *       (the command-line client);</li>
 *   <li>{@code Authorization: Basic <email:password>} on every request (scripts, curl -u);</li>
 *   <li>the {@code os_session} cookie set by the sign-in (the web pages). A cookie is sent by the
 *       browser on its own, so a request that changes something must also carry
 *       {@code X-Requested-With}, which another site's page cannot add without the controller's
 *       permission (the cookie is SameSite=Strict as well).</li>
 * </ul>
 *
 * <p>The account is checked before the handler runs, so an upload without valid credentials is
 * refused before its body is read. {@code /internal/**} (the worker protocol), {@code /health} and
 * the static pages are not affected.
 */
@Component
public class AuthInterceptor implements HandlerInterceptor {

    /** The request attribute holding the {@link AuthUser}. */
    public static final String USER = "objectstore.user";
    public static final String CSRF_HEADER = "X-Requested-With";
    private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD", "OPTIONS");
    /**
     * Paths only the admin may use: the cluster's inner state (the dashboard's data, which names
     * every client's objects), fsck (it reads every disk) and {@code /api/v1/admin/**}.
     */
    private static final Set<String> ADMIN_ONLY = Set.of("/api/v1/fsck", "/api/v1/cluster/status");
    private static final String ADMIN_PREFIX = "/api/v1/admin/";

    private final AccountService accounts;

    public AuthInterceptor(AccountService accounts) {
        this.accounts = accounts;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        AuthUser user;
        boolean byCookie = false;
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization != null) {
            user = fromHeader(authorization);
        } else {
            String token = cookie(request);
            if (token == null) {
                throw unauthorized(ErrorCodes.UNAUTHORIZED, "sign in first: POST /api/v1/auth/login, then send "
                        + "'Authorization: Bearer <token>' (or 'Authorization: Basic' with your email and password)");
            }
            user = accounts.byToken(token).orElseThrow(() -> unauthorized(ErrorCodes.UNAUTHORIZED,
                    "the session has expired or was signed out; sign in again"));
            byCookie = true;
        }
        if (byCookie && !SAFE_METHODS.contains(request.getMethod()) && request.getHeader(CSRF_HEADER) == null) {
            throw ApiException.forbidden("a browser request that changes something must carry the " + CSRF_HEADER
                    + " header");
        }
        String path = request.getRequestURI();
        if ((ADMIN_ONLY.contains(path) || path.startsWith(ADMIN_PREFIX)) && !user.admin()) {
            throw ApiException.forbidden("only the admin may use " + request.getRequestURI());
        }
        request.setAttribute(USER, user);
        return true;
    }

    private AuthUser fromHeader(String authorization) {
        int space = authorization.indexOf(' ');
        String scheme = space < 0 ? authorization : authorization.substring(0, space);
        String value = space < 0 ? "" : authorization.substring(space + 1).trim();
        if (scheme.equalsIgnoreCase("Bearer")) {
            return accounts.byToken(value).orElseThrow(() -> unauthorized(ErrorCodes.UNAUTHORIZED,
                    "the session token is invalid, has expired or was signed out; sign in again"));
        }
        if (scheme.equalsIgnoreCase("Basic")) {
            String decoded;
            try {
                decoded = new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                throw unauthorized(ErrorCodes.UNAUTHORIZED, "the Basic credentials are not valid base64");
            }
            int colon = decoded.indexOf(':');
            if (colon < 0) {
                throw unauthorized(ErrorCodes.UNAUTHORIZED, "the Basic credentials must be 'email:password'");
            }
            return accounts.byPassword(decoded.substring(0, colon), decoded.substring(colon + 1))
                    .orElseThrow(() -> unauthorized(ErrorCodes.INVALID_CREDENTIALS, "wrong email or password"));
        }
        throw unauthorized(ErrorCodes.UNAUTHORIZED, "unsupported Authorization scheme '" + scheme
                + "'; use Bearer or Basic");
    }

    private static String cookie(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (AccountService.COOKIE.equals(cookie.getName()) && !cookie.getValue().isEmpty()) {
                    return cookie.getValue();
                }
            }
        }
        return null;
    }

    /** A 401 with a Bearer challenge (not Basic, which would make browsers pop up their own dialog). */
    static ApiException unauthorized(String code, String detail) {
        return ApiException.unauthorized(code, detail).withHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer realm=\"objectstore\"");
    }
}
