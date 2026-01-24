// QUERY - Statistiche transazioni per giorno della settimana
// Usa tipi temporali nativi Neo4j (DATETIME) - nessuna conversione necessaria!
// 
// Scopo: Analizzare pattern settimanali nelle transazioni e outlier
// - Identifica giorni con più transazioni
// - Calcola percentuale outlier per giorno
// - Utile per rilevare anomalie comportamentali (es. più frodi nei weekend)
//
// Ottimizzazioni:
// - datetime è già tipo DATETIME nativo (nessuna conversione)
// - dayOfWeek() funzione nativa (1=Lunedì, 7=Domenica)
// - amount è già FLOAT (nessuna conversione toFloat)

MATCH (tx:Transaction)
WITH tx,
     // Usa date() per estrarre la data, poi .dayOfWeek (1=Lunedì, 7=Domenica)
     date(tx.datetime).dayOfWeek AS dow_num
WITH 
     dow_num,  // Mantieni dow_num per l'ordinamento
     // Converte numero in nome giorno
     CASE dow_num
       WHEN 1 THEN 'Mon'
       WHEN 2 THEN 'Tue'
       WHEN 3 THEN 'Wed'
       WHEN 4 THEN 'Thu'
       WHEN 5 THEN 'Fri'
       WHEN 6 THEN 'Sat'
       WHEN 7 THEN 'Sun'
     END AS day_of_week,
     count(tx) AS total_transactions,
     sum(CASE WHEN coalesce(tx.potentialOutlier, false) = true THEN 1 ELSE 0 END) AS outlier_count
RETURN day_of_week,
       total_transactions,
       outlier_count,
       // amount è già FLOAT, nessuna conversione necessaria
       round(outlier_count * 100.0 / total_transactions, 2) AS outlier_percentage
ORDER BY dow_num;  // Ordina direttamente per numero (più efficiente)

