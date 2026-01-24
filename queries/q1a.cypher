MATCH (c1:Customer)-[t1:USED_TERMINAL]->(ter:Terminal)<-[t2:USED_TERMINAL]-(c2:Customer)
WHERE c1.customerId < c2.customerId  // Filtra per evitare duplicati (c1,c2) e (c2,c1)

WITH c1, c2, count(DISTINCT ter) AS Num_Same_Ter  // Conta terminali condivisi distinti
WHERE Num_Same_Ter >= 4  // Almeno 4 terminali condivisi

// QUERY 3.a - OTTIMIZZATA: usa tipi nativi INT (nessuna conversione toInteger)
// Usa total_tx_count precalcolato (già INT, nessuna conversione necessaria)
WITH c1, c2, Num_Same_Ter, 
     c1.total_tx_count AS TOT_Transactions_C1,  // Già INT
     c2.total_tx_count AS TOT_Transactions_C2    // Già INT

WHERE abs(TOT_Transactions_C1 - TOT_Transactions_C2) <= 2

RETURN 
  c1.customerId AS customer_M,
  c2.customerId AS customer_N,
  Num_Same_Ter AS shared_terminals,
  TOT_Transactions_C1 AS tx_M,
  TOT_Transactions_C2 AS tx_N
ORDER BY Num_Same_Ter DESC, abs(TOT_Transactions_C1 - TOT_Transactions_C2) ASC;