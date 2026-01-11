// QUERY 3.a - Customer con terminali condivisi e pattern simili
// Trova coppie (M, N) che:
// 1. Condividono almeno 4 terminali
// 2. Hanno numero di transazioni SUI TERMINALI CONDIVISI che differisce di max 2
// Nota: usa USED_TERMINAL.tx_count (transazioni per terminal) non total_tx_count globale

MATCH (m:Customer)-[u1:USED_TERMINAL]->(t:Terminal)<-[u2:USED_TERMINAL]-(n:Customer)
WHERE m.customerId < n.customerId  // Evita duplicati (M,N) e (N,M)
WITH m, n, 
     collect(DISTINCT t.terminalId) as shared_terminal_ids,
     sum(toInteger(u1.tx_count)) as m_tx_on_shared,
     sum(toInteger(u2.tx_count)) as n_tx_on_shared
WHERE size(shared_terminal_ids) >= 4  // Almeno 4 terminali condivisi
  AND abs(m_tx_on_shared - n_tx_on_shared) <= 2  // Differenza tx sui condivisi <= 2
RETURN
  m.customerId AS customer_M,
  n.customerId AS customer_N,
  size(shared_terminal_ids) AS shared_terminals,
  m_tx_on_shared AS tx_M,
  n_tx_on_shared AS tx_N
ORDER BY shared_terminals DESC, abs(m_tx_on_shared - n_tx_on_shared) ASC;