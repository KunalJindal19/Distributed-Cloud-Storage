package com.objectstore.controller.auth;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.repository.AccountRepository;
import com.objectstore.controller.repository.AccountRepository.Account;
import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Client accounts: registration, sign-in sessions, and checking the credentials of a request.
 *
 * <ul>
 *   <li>Passwords are stored as salted PBKDF2 hashes ({@link PasswordHasher}).</li>
 *   <li>A sign-in creates a random 256-bit session token. Only its SHA-256 is stored, so a copy of
 *       the database does not let anyone sign in.</li>
 *   <li>Checked sessions and email/password pairs are remembered in memory for the life of the
 *       process, so the (deliberately slow) password hash is computed once per client, not once
 *       per request.</li>
 *   <li>Exactly one account is the admin ({@code ADMIN_EMAIL}). It is created, or its password set
 *       from {@code ADMIN_PASSWORD}, at every start-up.</li>
 * </ul>
 */
@Service
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    /** The browser session cookie (HttpOnly, SameSite=Strict). */
    public static final String COOKIE = "os_session";
    static final int MIN_PASSWORD_LENGTH = 8;
    static final int MAX_PASSWORD_LENGTH = 128;
    private static final int MAX_EMAIL_LENGTH = 254;
    private static final int MAX_TOKEN_LENGTH = 128;
    private static final int MAX_CACHED = 50_000;
    private static final Pattern EMAIL = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");

    /** A new sign-in session. {@code token} is shown to the client once and never stored. */
    public record Session(String token, AuthUser user, Instant expiresAt) {
    }

    private record CachedSession(AuthUser user, long expiresAt) {
    }

    private final AccountRepository accounts;
    private final AuthProperties properties;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    private final ConcurrentHashMap<String, CachedSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AuthUser> passwords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> emails = new ConcurrentHashMap<>();
    private final byte[] cacheKey = new byte[32];
    /** Checked against when an email has no account, so a wrong email takes as long as a wrong password. */
    private final String decoyHash;

    public AccountService(AccountRepository accounts, AuthProperties properties, Clock clock) {
        this.accounts = accounts;
        this.properties = properties;
        this.clock = clock;
        random.nextBytes(cacheKey);
        decoyHash = PasswordHasher.hash(UUID.randomUUID().toString());
    }

    /** Creates the admin account or updates its password, and gives it the objects nobody owns. */
    @PostConstruct
    void ensureAdmin() {
        String email = normalizeEmail(properties.adminEmail());
        if (!isValidEmail(email)) {
            throw new IllegalStateException("ADMIN_EMAIL '" + properties.adminEmail() + "' is not a valid email address");
        }
        String password = properties.adminPassword();
        if (password == null || password.isEmpty()) {
            password = randomToken(12);
            log.warn("[auth] ADMIN_PASSWORD is not set: the admin account {} has the generated password {} "
                    + "(set ADMIN_PASSWORD to choose one)", email, password);
        }
        Optional<Account> existing = accounts.findByEmail(email);
        String adminId;
        if (existing.isEmpty()) {
            adminId = UUID.randomUUID().toString();
            accounts.insert(new Account(adminId, email, PasswordHasher.hash(password), true, clock.millis()));
            log.info("[auth] created the admin account {}", email);
        } else {
            adminId = existing.get().userId();
            if (!PasswordHasher.verify(password, existing.get().passwordHash())) {
                accounts.updatePasswordHash(adminId, PasswordHasher.hash(password));
                log.info("[auth] the password of the admin account {} was set from ADMIN_PASSWORD", email);
            }
        }
        accounts.makeOnlyAdmin(adminId);
        int adopted = accounts.adoptOwnerlessObjects(adminId);
        if (adopted > 0) {
            log.info("[auth] {} object(s) stored before accounts existed now belong to the admin {}", adopted, email);
        }
    }

    /** Creates a regular account. 400 for an invalid email or password, 409 if the email is taken. */
    public AuthUser register(String rawEmail, String password) {
        String email = normalizeEmail(rawEmail);
        if (!isValidEmail(email)) {
            throw ApiException.badRequest("'email' must be a valid email address of at most " + MAX_EMAIL_LENGTH + " characters");
        }
        if (password == null || password.length() < MIN_PASSWORD_LENGTH || password.length() > MAX_PASSWORD_LENGTH) {
            throw ApiException.badRequest("'password' must be " + MIN_PASSWORD_LENGTH + "-" + MAX_PASSWORD_LENGTH
                    + " characters long");
        }
        Account account = new Account(UUID.randomUUID().toString(), email, PasswordHasher.hash(password), false,
                clock.millis());
        if (!accounts.insert(account)) {
            throw ApiException.conflict(ErrorCodes.EMAIL_TAKEN, "an account with the email " + email + " already exists");
        }
        log.info("[auth] registered {}", email);
        return toUser(account);
    }

    /** Checks the password and opens a session. 401 {@code invalid_credentials} if either is wrong. */
    public Session login(String rawEmail, String password) {
        AuthUser user = checkPassword(normalizeEmail(rawEmail), password)
                .orElseThrow(() -> ApiException.unauthorized(ErrorCodes.INVALID_CREDENTIALS, "wrong email or password"));
        String token = randomToken(32);
        long now = clock.millis();
        long expiresAt = now + (long) (properties.sessionHours() * 3_600_000);
        accounts.deleteExpiredSessions(now);
        accounts.insertSession(sha256(token), user.userId(), now, expiresAt);
        remember(sessions, sha256(token), new CachedSession(user, expiresAt));
        log.info("[auth] {} signed in", user.email());
        return new Session(token, user, Instant.ofEpochMilli(expiresAt));
    }

    /** Ends a session; unknown tokens are ignored. */
    public void logout(String token) {
        if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_LENGTH) {
            return;
        }
        String hash = sha256(token);
        sessions.remove(hash);
        accounts.deleteSession(hash);
    }

    /** The account of a session token, if the session exists and has not expired. */
    public Optional<AuthUser> byToken(String token) {
        if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_LENGTH) {
            return Optional.empty();
        }
        String hash = sha256(token);
        long now = clock.millis();
        CachedSession cached = sessions.get(hash);
        if (cached == null) {
            Optional<AccountRepository.SessionRow> row = accounts.findSession(hash);
            if (row.isEmpty()) {
                return Optional.empty();
            }
            cached = new CachedSession(toUser(row.get().account()), row.get().expiresAt());
            remember(sessions, hash, cached);
        }
        if (cached.expiresAt() <= now) {
            sessions.remove(hash);
            accounts.deleteSession(hash);
            return Optional.empty();
        }
        return Optional.of(cached.user());
    }

    /** The account of an email and password sent with the request itself (HTTP Basic). */
    public Optional<AuthUser> byPassword(String rawEmail, String password) {
        String email = normalizeEmail(rawEmail);
        String key = cacheKey(email, password);
        AuthUser cached = passwords.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        Optional<AuthUser> user = checkPassword(email, password);
        user.ifPresent(u -> remember(passwords, key, u));
        return user;
    }

    /** The email of an account, for showing an object's owner; null if unknown. */
    public String emailOf(String userId) {
        if (userId == null) {
            return null;
        }
        String email = emails.get(userId);
        if (email == null) {
            email = accounts.findById(userId).map(Account::email).orElse(null);
            if (email != null) {
                remember(emails, userId, email);
            }
        }
        return email;
    }

    private Optional<AuthUser> checkPassword(String email, String password) {
        if (password == null || password.length() > MAX_PASSWORD_LENGTH || email.length() > MAX_EMAIL_LENGTH) {
            return Optional.empty();
        }
        Optional<Account> account = accounts.findByEmail(email);
        if (account.isEmpty()) {
            PasswordHasher.verify(password, decoyHash);
            return Optional.empty();
        }
        return PasswordHasher.verify(password, account.get().passwordHash())
                ? Optional.of(toUser(account.get()))
                : Optional.empty();
    }

    private AuthUser toUser(Account account) {
        remember(emails, account.userId(), account.email());
        return new AuthUser(account.userId(), account.email(), account.admin());
    }

    static String normalizeEmail(String email) {
        return email == null ? "" : email.trim().toLowerCase(Locale.ROOT);
    }

    static boolean isValidEmail(String email) {
        return email.length() <= MAX_EMAIL_LENGTH && EMAIL.matcher(email).matches();
    }

    private String randomToken(int bytes) {
        byte[] token = new byte[bytes];
        random.nextBytes(token);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(token);
    }

    private String cacheKey(String email, String password) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(cacheKey, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal((email + "\0" + password).getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static <K, V> void remember(ConcurrentHashMap<K, V> cache, K key, V value) {
        if (cache.size() >= MAX_CACHED) {
            cache.clear();
        }
        cache.put(key, value);
    }
}
