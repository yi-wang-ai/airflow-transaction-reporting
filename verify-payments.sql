SELECT jsonb_build_object(
    'raw_rows', (SELECT count(*) FROM payment_demo.raw_transactions),
    'distinct_ids', (SELECT count(DISTINCT tx_id) FROM payment_demo.raw_transactions),
    'raw_digest', (
        SELECT md5(jsonb_agg(to_jsonb(t) ORDER BY tx_id)::text)
        FROM payment_demo.raw_transactions t
    ),
    'report_digest', (
        SELECT md5(jsonb_agg(to_jsonb(t) ORDER BY report_date, currency)::text)
        FROM payment_demo.daily_transaction_report t
    ),
    'raw_sample', (
        SELECT jsonb_agg(to_jsonb(t)) FROM (
            SELECT * FROM payment_demo.raw_transactions ORDER BY tx_id LIMIT 5
        ) t
    ),
    'report', (
        SELECT jsonb_agg(to_jsonb(t) ORDER BY report_date, currency)
        FROM payment_demo.daily_transaction_report t
    )
);
