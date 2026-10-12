-- Isolate the effect of optimized raw storage from pre-aggregation.
SELECT
    us_state,
    tupleElement(best, 1) AS cat_id,
    tupleElement(best, 2) AS max_amount,
    tupleElement(best, 3) AS source_row
FROM
(
    SELECT
        us_state,
        argMax(tuple(toString(cat_id), amount, source_row),
               tuple(amount, -toInt64(source_row))) AS best
    FROM hw2.transactions
    WHERE run_id = {run_id:UUID}
    GROUP BY us_state
)
ORDER BY us_state;
