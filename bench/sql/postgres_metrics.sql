\pset tuples_only on

SELECT
    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS timestamp_ms,
    pg_current_wal_lsn()::TEXT AS current_wal_lsn,
    slot.confirmed_flush_lsn::TEXT,
    pg_wal_lsn_diff(pg_current_wal_lsn(), slot.confirmed_flush_lsn)::BIGINT
        AS retained_wal_bytes,
    (slot.active_pid IS NOT NULL)::INTEGER AS slot_active,
    COALESCE(stats.spill_txns, 0) AS postgres_spill_transactions,
    COALESCE(stats.spill_count, 0) AS postgres_spill_count,
    COALESCE(stats.spill_bytes, 0) AS postgres_spill_bytes
FROM pg_replication_slots AS slot
LEFT JOIN pg_stat_replication_slots AS stats USING (slot_name)
WHERE slot.slot_name = :'slot_name';
