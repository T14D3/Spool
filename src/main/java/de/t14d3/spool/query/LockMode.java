package de.t14d3.spool.query;

/**
 * Row-level locking modes for {@code EntityManager.lock} and
 * {@code EntityRepository.lock}.
 * <p>
 * Locking is translated to the dialect's {@code SELECT ... FOR UPDATE} /
 * {@code FOR SHARE} syntax and therefore requires an active transaction.
 */
public enum LockMode {
    /**
     * Exclusive row lock. Supported by H2, MySQL/MariaDB and PostgreSQL.
     */
    PESSIMISTIC_WRITE,

    /**
     * Shared row lock ({@code FOR SHARE}) on MySQL/MariaDB and PostgreSQL.
     * H2 upgrades this to {@code FOR UPDATE} because it does not implement
     * shared row locks. SQLite does not support either mode.
     */
    PESSIMISTIC_READ
}
