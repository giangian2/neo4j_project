MATCH (m:Customer)-[s:SHARES_TERMINAL]->(n:Customer)
WHERE m <> n
  AND abs(
        toInteger(m.total_tx_count) -
        toInteger(n.total_tx_count)
      ) <= 100  // Rilassato da 2 a 50 per dataset realistici
WITH m, n, count(s) AS shared_terminals
WHERE shared_terminals >= 4
RETURN
  m.customerId AS customer_M,
  n.customerId AS customer_N,
  shared_terminals,
  m.total_tx_count AS tx_M,
  n.total_tx_count AS tx_N
ORDER BY shared_terminals DESC;