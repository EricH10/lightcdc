BEGIN;

CREATE TEMP TABLE lightcdc_demo_order_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

WITH inserted AS (
    INSERT INTO public.orders (customer_email, status, total_cents)
    SELECT
        format('lightcdc-demo-%s-%s@example.com', txid_current(), n),
        'created',
        1000 + (n * 137)
    FROM generate_series(1, 20) AS n
    RETURNING id
)
INSERT INTO lightcdc_demo_order_ids (id)
SELECT id
FROM inserted;

UPDATE public.orders
SET
    status = 'paid',
    updated_at = now()
WHERE id IN (
    SELECT id
    FROM lightcdc_demo_order_ids
    ORDER BY id
    LIMIT 8
);

DELETE FROM public.orders
WHERE id IN (
    SELECT id
    FROM lightcdc_demo_order_ids
    ORDER BY id DESC
    LIMIT 4
);

COMMIT;
