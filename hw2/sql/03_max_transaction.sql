-- Category of the single largest transaction, not category with largest sum.
-- Bind run_id to the UUID printed by load_csv.py or run_pipeline.py.
SELECT
    us_state,
    tupleElement(best, 1) AS cat_id,
    tupleElement(best, 2) AS max_amount,
    tupleElement(best, 3) AS source_row
FROM
(
    SELECT
        us_state,
        argMax(tuple(cat_id, amount, source_row),
               tuple(amount, -toInt64(source_row))) AS best
    FROM hw2.transactions_baseline
    WHERE run_id = {run_id:UUID}
    GROUP BY us_state
)
ORDER BY us_state;
