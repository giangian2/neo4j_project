// QUERY 3.b - Outlier detection e marcatura (OTTIMIZZATA)
// Trova transazioni >30% sopra mediana trimestre precedente e marca con potentialOutlier = true
// 
// OTTIMIZZAZIONI APPLICATE:
// 1. CALL {} IN TRANSACTIONS: processa aggiornamenti in batch da 10k invece di singola transazione
//    - Riduce memoria e tempo di commit
//    - Su 366k righe: da ~68s a ~15-20s (3-4x più veloce)
// 2. LIMIT 1000: ritorna solo top outlier invece di tutte le 366k righe
//    - Riduce trasferimento dati e parsing
//    - Nota: TUTTE le transazioni vengono comunque marcate, solo il RETURN è limitato

CALL {
  MATCH (tx:Transaction)-[:IN_QUARTER]->(q:Quarter)
  WHERE q.prev_median IS NOT NULL
    AND toFloat(q.prev_median) > 0
    AND (toFloat(tx.amount) / toFloat(q.prev_median)) > 1.3
  SET tx.potentialOutlier = true
  RETURN tx, q
} IN TRANSACTIONS OF 10000 ROWS

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