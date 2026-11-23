package com.objectstore.controller.repository;

import java.util.Optional;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Client accounts and their sign-in sessions: {@code users} and {@code sessions}. */
@Repository
public class AccountRepository {

    /** One account. {@code passwordHash} is a {@code PasswordHasher} string, never a password. */
    public record Account(String userId, String email, String passwordHash, boolean admin, long createdAt) {
    }

    /** A session and the account it belongs to. */
    public record SessionRow(Account account, long expiresAt) {
    }

    private static final RowMapper<Account> ACCOUNT = (rs, rowNum) -> new Account(
            rs.getString("user_id"),
            rs.getString("email"),
            rs.getString("password_hash"),
            rs.getInt("is_admin") == 1,
            rs.getLong("created_at"));

    private final JdbcClient jdbc;

    public AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Creates the account; false (and nothing changed) if its email already has one. */
    public boolean insert(Account account) {
        return jdbc.sql("""
                        INSERT OR IGNORE INTO users (user_id, email, password_hash, is_admin, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """)
                .params(account.userId(), account.email(), account.passwordHash(), account.admin() ? 1 : 0,
                        account.createdAt())
                .update() == 1;
    }

    public Optional<Account> findByEmail(String email) {
        return jdbc.sql("SELECT * FROM users WHERE email = ?").param(email).query(ACCOUNT).optional();
    }

    public Optional<Account> findById(String userId) {
        return jdbc.sql("SELECT * FROM users WHERE user_id = ?").param(userId).query(ACCOUNT).optional();
    }

    public void updatePasswordHash(String userId, String passwordHash) {
        jdbc.sql("UPDATE users SET password_hash = ? WHERE user_id = ?").params(passwordHash, userId).update();
    }

    /** Makes {@code userId} the only admin account. */
    public void makeOnlyAdmin(String userId) {
        jdbc.sql("UPDATE users SET is_admin = CASE WHEN user_id = ? THEN 1 ELSE 0 END").param(userId).update();
    }

    /** Gives every object without an owner (stored before accounts existed) to {@code userId}. */
    public int adoptOwnerlessObjects(String userId) {
        return jdbc.sql("UPDATE objects SET owner_id = ? WHERE owner_id IS NULL").param(userId).update();
    }

    public void insertSession(String tokenHash, String userId, long createdAt, long expiresAt) {
        jdbc.sql("INSERT INTO sessions (token_hash, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)")
                .params(tokenHash, userId, createdAt, expiresAt)
                .update();
    }

    public Optional<SessionRow> findSession(String tokenHash) {
        return jdbc.sql("""
                        SELECT u.*, s.expires_at FROM sessions s JOIN users u ON u.user_id = s.user_id
                        WHERE s.token_hash = ?
                        """)
                .param(tokenHash)
                .query((rs, rowNum) -> new SessionRow(ACCOUNT.mapRow(rs, rowNum), rs.getLong("expires_at")))
                .optional();
    }

    public void deleteSession(String tokenHash) {
        jdbc.sql("DELETE FROM sessions WHERE token_hash = ?").param(tokenHash).update();
    }

    public int deleteExpiredSessions(long now) {
        return jdbc.sql("DELETE FROM sessions WHERE expires_at <= ?").param(now).update();
    }
}
