// QUERY 3.b - Outlier detection e marcatura (OTTIMIZZATA)
// Usa tipi nativi FLOAT - nessuna conversione toFloat necessaria!
//
// Ottimizzazioni:
// - amount è già FLOAT (nessuna conversione)
// - prev_median è già FLOAT (nessuna conversione)
// - Elimina tutte le chiamate toFloat() ridondanti

CALL {
  MATCH (tx:Transaction)-[:IN_QUARTER]->(q:Quarter)
  WHERE q.prev_median IS NOT NULL
    AND q.prev_median > 0
    AND (tx.amount / q.prev_median) > 1.3
  SET tx.potentialOutlier = true
  RETURN tx, q
} IN TRANSACTIONS OF 10000 ROWS

WITH tx, q,
     tx.amount / q.prev_median AS ratio
RETURN 
    q.quarterId,
    tx.transactionId,
    tx.amount AS transaction_amount,  // Già FLOAT, nessuna conversione
    q.prev_median AS previous_quarter_median,  // Già FLOAT
    q.prev_median * 1.3 AS threshold,
    ratio,
    round((ratio - 1) * 100, 2) AS percentage_above,
    'POTENTIAL_OUTLIER' AS status
ORDER BY ratio DESC;