INSERT INTO public.lightcdc_benchmark_unrelated (producer_id, payload)
SELECT
    :client_id,
    repeat('u', :payload_bytes)
FROM generate_series(1, :unrelated_rows_per_transaction);
