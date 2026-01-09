// FREQUENT_COLLABORATOR - Usa relazioni pre-calcolate USED_TERMINAL
// Customer con ≥5 transazioni sullo stesso terminal e rating medio entro 0.5

// Trova coppie di customer che condividono terminal con ≥5 tx ciascuno
MATCH (c1:Customer)-[u1:USED_TERMINAL]->(t:Terminal)<-[u2:USED_TERMINAL]-(c2:Customer)
WHERE c1.customerId < c2.customerId  // Evita duplicati
  AND u1.tx_count >= 5
  AND u2.tx_count >= 5

// Calcola rating medio per ogni customer
WITH c1, c2, collect(t.terminalId) AS shared_terminals
MATCH (c1)-[:MADE_TRANSACTION]->(tx1:Transaction)
WITH c1, c2, shared_terminals, avg(tx1.satisfaction_rating) AS c1_avg_rating
MATCH (c2)-[:MADE_TRANSACTION]->(tx2:Transaction)
WITH c1, c2, shared_terminals, c1_avg_rating, avg(tx2.satisfaction_rating) AS c2_avg_rating

// Filtra per differenza rating ≤0.5
WHERE abs(c1_avg_rating - c2_avg_rating) <= 0.5

// Crea relazione
MERGE (c1)-[r:FREQUENT_COLLABORATOR]-(c2)
SET r.shared_terminals_count = size(shared_terminals),
    r.avg_rating_diff = abs(c1_avg_rating - c2_avg_rating)
RETURN count(r) as frequent_collaborator_relations_created;

