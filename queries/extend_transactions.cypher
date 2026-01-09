// EXTEND - Estendi transazioni con nuove proprietà in batch
// Aggiunge: payment_method, promotional_offer, satisfaction_rating
// Usa CALL IN TRANSACTIONS per evitare out of memory

CALL {
    MATCH (tx:Transaction)
    WHERE tx.payment_method IS NULL
    WITH tx LIMIT 5000
    SET tx.payment_method = ['credit_card', 'mobile_payment', 'paypal', 'debit_card'][toInteger(rand() * 4)],
        tx.promotional_offer = CASE WHEN rand() < 0.3 THEN true ELSE false END,
        tx.satisfaction_rating = toInteger(rand() * 5) + 1
    RETURN count(tx) as batch_count
} IN TRANSACTIONS OF 5000 ROWS

RETURN sum(batch_count) as transactions_extended;

