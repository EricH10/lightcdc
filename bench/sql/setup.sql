CREATE TABLE IF NOT EXISTS public.lightcdc_benchmark_events (
    id BIGSERIAL PRIMARY KEY,
    producer_id INTEGER NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE public.lightcdc_benchmark_events REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication_tables
        WHERE pubname = 'lightcdc_publication'
          AND schemaname = 'public'
          AND tablename = 'lightcdc_benchmark_events'
    ) THEN
        ALTER PUBLICATION lightcdc_publication
        ADD TABLE public.lightcdc_benchmark_events;
    END IF;
END
$$;

TRUNCATE TABLE public.lightcdc_benchmark_events RESTART IDENTITY;
