\set target_id random(1, :max_id)

UPDATE public.lightcdc_benchmark_events
SET
    version = version + 1,
    payload = repeat('u', :payload_bytes),
    updated_at = clock_timestamp()
WHERE id = :target_id;
