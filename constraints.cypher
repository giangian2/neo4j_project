// Constraints e indici per Neo4j
// Questo file viene eseguito automaticamente all'avvio di Neo4j

// Constraints per garantire unicità degli ID
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS 
FOR (c:Customer) REQUIRE c.customerId IS UNIQUE;

CREATE CONSTRAINT terminal_id_unique IF NOT EXISTS 
FOR (t:Terminal) REQUIRE t.terminalId IS UNIQUE;

CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS 
FOR (tx:Transaction) REQUIRE tx.transactionId IS UNIQUE;

// Indici per migliorare le performance delle query
CREATE INDEX transaction_fraud_idx IF NOT EXISTS 
FOR (tx:Transaction) ON (tx.fraud);

CREATE INDEX transaction_datetime_idx IF NOT EXISTS 
FOR (tx:Transaction) ON (tx.datetime);

// Indici per Query 3.b - migliorano lookup su prev_median e amount
CREATE INDEX quarter_prev_median_idx IF NOT EXISTS 
FOR (q:Quarter) ON (q.prev_median);

CREATE INDEX transaction_amount_idx IF NOT EXISTS 
FOR (tx:Transaction) ON (tx.amount);

