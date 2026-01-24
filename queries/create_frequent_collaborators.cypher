// FREQUENT_COLLABORATOR - Ottimizzata: usa avg_satisfaction_rating pre-calcolato
// Customer con ≥5 transazioni sullo stesso terminal e rating medio entro 0.5

MATCH (c1:Customer)-[u1:USED_TERMINAL]->(t:Terminal)<-[u2:USED_TERMINAL]-(c2:Customer)
WHERE c1.customerId < c2.customerId
  AND u1.tx_count >= 5
  AND u2.tx_count >= 5
  AND c1.avg_satisfaction_rating IS NOT NULL
  AND c2.avg_satisfaction_rating IS NOT NULL
  AND abs(c1.avg_satisfaction_rating - c2.avg_satisfaction_rating) <= 0.5

WITH c1, c2, collect(DISTINCT t.terminalId) AS shared_terminals

MERGE (c1)-[r:FREQUENT_COLLABORATOR]-(c2)
ON CREATE SET r.shared_terminals_count = size(shared_terminals),
                r.avg_rating_diff = abs(c1.avg_satisfaction_rating - c2.avg_satisfaction_rating)
RETURN count(r) as frequent_collaborator_relations_created;

