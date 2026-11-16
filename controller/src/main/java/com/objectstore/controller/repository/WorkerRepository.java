package com.objectstore.controller.repository;

import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.domain.WorkerStatus;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionOperations;

@Repository
public class WorkerRepository {

    private static final RowMapper<Worker> WORKER = (rs, rowNum) -> new Worker(
            rs.getString("worker_id"),
            rs.getString("url"),
            WorkerStatus.valueOf(rs.getString("status")),
            Instant.ofEpochMilli(rs.getLong("last_seen")),
            Instant.ofEpochMilli(rs.getLong("registered_at")));

    /**
     * The result of a registration or heartbeat.
     *
     * @param worker         the row after the update
     * @param previousStatus the status before it, or {@code null} if the worker was not known
     */
    public record Transition(Worker worker, WorkerStatus previousStatus) {

        public boolean isNewWorker() {
            return previousStatus == null;
        }

        public boolean cameBack() {
            return previousStatus == WorkerStatus.DEAD;
        }
    }

    private final JdbcClient jdbc;
    private final TransactionOperations tx;

    public WorkerRepository(JdbcClient jdbc, TransactionOperations tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    public List<Worker> findAll() {
        return jdbc.sql("SELECT * FROM workers ORDER BY worker_id").query(WORKER).list();
    }

    public List<Worker> findAlive() {
        return jdbc.sql("SELECT * FROM workers WHERE status = 'ALIVE' ORDER BY worker_id").query(WORKER).list();
    }

    public Optional<Worker> findById(String workerId) {
        return jdbc.sql("SELECT * FROM workers WHERE worker_id = ?").param(workerId).query(WORKER).optional();
    }

    /** Insert-or-update: registering is safe to repeat and always leaves the worker ALIVE. */
    public Transition register(String workerId, String url, Instant now) {
        return tx.execute(status -> {
            Optional<Worker> existing = findById(workerId);
            if (existing.isEmpty()) {
                jdbc.sql("""
                        INSERT INTO workers (worker_id, url, status, last_seen, registered_at)
                        VALUES (?, ?, 'ALIVE', ?, ?)
                        """)
                        .params(workerId, url, now.toEpochMilli(), now.toEpochMilli())
                        .update();
            } else {
                jdbc.sql("UPDATE workers SET url = ?, status = 'ALIVE', last_seen = ? WHERE worker_id = ?")
                        .params(url, now.toEpochMilli(), workerId)
                        .update();
            }
            return new Transition(findById(workerId).orElseThrow(), existing.map(Worker::status).orElse(null));
        });
    }

    /** Records a heartbeat; empty if the worker has never registered. */
    public Optional<Transition> heartbeat(String workerId, Instant now) {
        return tx.execute(status -> {
            Optional<Worker> existing = findById(workerId);
            if (existing.isEmpty()) {
                return Optional.<Transition>empty();
            }
            jdbc.sql("UPDATE workers SET status = 'ALIVE', last_seen = ? WHERE worker_id = ?")
                    .params(now.toEpochMilli(), workerId)
                    .update();
            return Optional.of(new Transition(findById(workerId).orElseThrow(), existing.get().status()));
        });
    }

    /** Marks every ALIVE worker last heard from before {@code cutoff} as DEAD and returns them. */
    public List<Worker> markSilentWorkersDead(Instant cutoff) {
        return tx.execute(status -> {
            List<Worker> silent = jdbc.sql(
                            "SELECT * FROM workers WHERE status = 'ALIVE' AND last_seen < ? ORDER BY worker_id")
                    .param(cutoff.toEpochMilli())
                    .query(WORKER)
                    .list();
            for (Worker worker : silent) {
                jdbc.sql("UPDATE workers SET status = 'DEAD' WHERE worker_id = ?").param(worker.workerId()).update();
            }
            return silent;
        });
    }

    /** worker_id &rarr; number of copies recorded on it (any status). Used for least-loaded choices. */
    public Map<String, Integer> replicaCounts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        jdbc.sql("""
                SELECT w.worker_id, COUNT(r.chunk_id) AS replicas
                FROM workers w LEFT JOIN chunk_replicas r ON r.worker_id = w.worker_id
                GROUP BY w.worker_id
                ORDER BY w.worker_id
                """)
                .query((rs, rowNum) -> counts.put(rs.getString("worker_id"), rs.getInt("replicas")))
                .list();
        return counts;
    }
}
