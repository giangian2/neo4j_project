// QUERY 3.c - Co-Customer Network di grado 3 (CC3)
// Dato un customer u, trova tutti i customer y raggiungibili attraverso
// un percorso di grado 3: u -> t1 -> u2 -> t2 -> u3 -> t3 -> y

MATCH path = (u:Customer {customerId: $customerId})
    -[:USED_TERMINAL]->(t1:Terminal)
    <-[:USED_TERMINAL]-(u2:Customer)
    -[:USED_TERMINAL]->(t2:Terminal)
    <-[:USED_TERMINAL]-(u3:Customer)
    -[:USED_TERMINAL]->(t3:Terminal)
    <-[:USED_TERMINAL]-(y:Customer)
WHERE u <> u2 AND u <> u3 AND u <> y
  AND u2 <> u3 AND u2 <> y
  AND u3 <> y
WITH DISTINCT u, y, 
     count(DISTINCT t1) + count(DISTINCT t2) + count(DISTINCT t3) as terminals_in_path
RETURN 
    u.customerId AS customer_start,
    y.customerId AS customer_end,
    terminals_in_path,
    'CC3' AS network_type
ORDER BY y.customerId;

