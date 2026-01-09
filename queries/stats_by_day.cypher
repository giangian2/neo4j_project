// STATS BY DAY - Statistiche per giorno della settimana
// Usa proprietà potentialOutlier marcata dalla query 3.b

MATCH (tx:Transaction)
WITH tx,
     // Estrai giorno settimana da datetime "2018-04-02 02:30:18"
     split(tx.datetime, ' ')[0] AS date_part
WITH tx, date_part,
     toInteger(split(date_part, '-')[2]) AS day,
     toInteger(split(date_part, '-')[1]) AS month,
     toInteger(split(date_part, '-')[0]) AS year
WITH tx,
     // Formula Zeller per day of week
     ((day + toInteger((13 * (month + 1)) / 5) + year + toInteger(year / 4) - toInteger(year / 100) + toInteger(year / 400)) % 7) AS dow_num
WITH 
     CASE dow_num
       WHEN 0 THEN 'Sun'
       WHEN 1 THEN 'Mon'
       WHEN 2 THEN 'Tue'
       WHEN 3 THEN 'Wed'
       WHEN 4 THEN 'Thu'
       WHEN 5 THEN 'Fri'
       WHEN 6 THEN 'Sat'
     END AS day_of_week,
     count(tx) AS total_transactions,
     sum(CASE WHEN coalesce(tx.potentialOutlier, false) = true THEN 1 ELSE 0 END) AS outlier_count
RETURN day_of_week,
       total_transactions,
       outlier_count,
       round(toFloat(outlier_count) / total_transactions * 100, 2) AS outlier_percentage
ORDER BY 
  CASE day_of_week
    WHEN 'Mon' THEN 1
    WHEN 'Tue' THEN 2
    WHEN 'Wed' THEN 3
    WHEN 'Thu' THEN 4
    WHEN 'Fri' THEN 5
    WHEN 'Sat' THEN 6
    WHEN 'Sun' THEN 7
  END;

