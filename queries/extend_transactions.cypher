// EXTEND - Estendi transazioni con nuove proprietà in batch
// Aggiunge: payment_method, promotional_offer, satisfaction_rating
// Pre-calcola avg_satisfaction_rating per ogni Customer

CALL {
    MATCH (tx:Transaction)
    WHERE tx.payment_method IS NULL
    WITH tx LIMIT 5000
    SET tx.payment_method = ['credit_card', 'mobile_payment', 'paypal', 'debit_card'][toInteger(rand() * 4)],
        tx.promotional_offer = CASE WHEN rand() < 0.3 THEN true ELSE false END,
        tx.satisfaction_rating = toInteger(rand() * 5) + 1
    RETURN count(tx) as batch_count
} IN TRANSACTIONS OF 5000 ROWS

WITH sum(batch_count) as transactions_extended

MATCH (c:Customer)-[:MADE_TRANSACTION]->(tx:Transaction)
WHERE tx.satisfaction_rating IS NOT NULL
WITH transactions_extended, c, avg(toInteger(tx.satisfaction_rating)) AS avg_rating
SET c.avg_satisfaction_rating = avg_rating
WITH transactions_extended, count(c) as customers_with_rating

RETURN transactions_extended, customers_with_rating;

