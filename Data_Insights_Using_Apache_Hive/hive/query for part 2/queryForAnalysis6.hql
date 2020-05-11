SELECT * FROM
(
SELECT time_period, vc, RANK() OVER (PARTITION BY time_period ORDER BY COUNT(*) DESC) AS cv_rnk, COUNT(*) AS n_violations
FROM
(
  SELECT ROUND(HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(issue_date,' ',violation_time,'M'), 'MM/dd/yyyy hhmma')))/4) AS time_period, violation_code AS vc
  FROM hive_db.parking_violation_data
  WHERE month = '201710'
)a
GROUP BY time_period, vc
)b
WHERE cv_rnk < 4 ORDER BY time_period ASC, cv_rnk ASC;
