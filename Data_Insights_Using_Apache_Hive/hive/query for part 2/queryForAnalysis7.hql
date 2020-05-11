SELECT *
FROM
(
    SELECT a.vc AS vc, time_period, RANK() OVER (PARTITION BY a.vc ORDER BY sum(time_period) desc) AS vc_time_period_rnk
    FROM
    (
      SELECT violation_code AS vc, RANK() OVER (PARTITION BY null ORDER BY count(*) desc) AS vc_rnk
      FROM hive_db.parking_violation_data
      WHERE month  = '201710'
      GROUP BY violation_code
    )a
    left outer join
    (
      SELECT ROUND(HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(issue_date,' ',violation_time,'M'), 'MM/dd/yyyy hhmma')))/4) AS time_period,  violation_code AS vc, count(*) AS vc_time_period_count
      FROM hive_db.parking_violation_data
      WHERE month = '201710'
      GROUP BY ROUND(HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(issue_date,' ',violation_time,'M'), 'MM/dd/yyyy hhmma')))/4) ,  violation_code
    )b
    on(a.vc = b.vc)
    WHERE vc_rnk < 4
    GROUP BY a.vc, time_period
)c
WHERE vc_time_period_rnk < 4;

