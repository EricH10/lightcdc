INSERT INTO public.lightcdc_benchmark_events (producer_id, payload)
SELECT
    :client_id,
    repeat('x', :payload_bytes)
FROM generate_series(1, :rows_per_transaction);
