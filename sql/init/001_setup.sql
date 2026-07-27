CREATE TABLE IF NOT EXISTS public.orders (
    id BIGSERIAL PRIMARY KEY,
    customer_email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    total_cents BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'lightcdc_publication'
    ) THEN
        EXECUTE 'CREATE PUBLICATION lightcdc_publication FOR TABLE public.orders';
    END IF;
END
$$;

SELECT pg_create_logical_replication_slot('lightcdc_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = 'lightcdc_slot'
);
