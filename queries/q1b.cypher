// QUERY 3.b - Outlier detection e marcatura
// Trova transazioni >30% sopra mediana trimestre precedente
// e marca con potentialOutlier = true

MATCH (tx:Transaction)-[:IN_QUARTER]->(q:Quarter)
WHERE q.prev_median IS NOT NULL
  AND toFloat(q.prev_median) > 0
  AND (toFloat(tx.amount) / toFloat(q.prev_median)) > 1.3
SET tx.potentialOutlier = true
WITH tx, q,
     toFloat(tx.amount) / toFloat(q.prev_median) AS ratio
RETURN 
    q.quarterId,
    tx.transactionId,
    toFloat(tx.amount) AS transaction_amount,
    toFloat(q.prev_median) AS previous_quarter_median,
    toFloat(q.prev_median) * 1.3 AS threshold,
    ratio,
    round((ratio - 1) * 100, 2) AS percentage_above,
    'POTENTIAL_OUTLIER' AS status
ORDER BY ratio DESC;