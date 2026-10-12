SELECT
    us_state,
    tupleElement(best, 1) AS cat_id,
    tupleElement(best, 2) AS max_amount,
    tupleElement(best, 3) AS source_row
FROM
(
    SELECT us_state, argMaxMerge(winner) AS best
    FROM hw2.state_max
    WHERE run_id = {run_id:UUID}
    GROUP BY us_state
)
ORDER BY us_state;
