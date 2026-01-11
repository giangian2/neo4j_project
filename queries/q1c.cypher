// QUERY 3.c - Co-Customer Network di grado 3 (CN3)
// Dato un customer u, trova tutti i customer y raggiungibili attraverso
// un percorso di grado 3: u1-t1-u2-t2-u3 dove u1=u, u3=y
// CN3 = 3 customer (u, u2, y) collegati da 2 terminali (t1, t2)

MATCH path = (u:Customer {customerId: $customerId})
    -[:USED_TERMINAL]->(t1:Terminal)
    <-[:USED_TERMINAL]-(u2:Customer)
    -[:USED_TERMINAL]->(t2:Terminal)
    <-[:USED_TERMINAL]-(y:Customer)
WHERE u <> u2 AND u <> y AND u2 <> y
WITH DISTINCT u, y, 
     count(DISTINCT t1) + count(DISTINCT t2) as terminals_in_path
RETURN 
    u.customerId AS customer_start,
    y.customerId AS customer_end,
    terminals_in_path,
    'CN3' AS network_type
ORDER BY y.customerId;

