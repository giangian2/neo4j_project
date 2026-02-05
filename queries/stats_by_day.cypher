MATCH (tx:Transaction)
WITH tx,
     date(tx.datetime).dayOfWeek AS dow_num
WITH 
     dow_num,
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
       round(outlier_count * 100.0 / total_transactions, 2) AS outlier_percentage
ORDER BY dow_num;

