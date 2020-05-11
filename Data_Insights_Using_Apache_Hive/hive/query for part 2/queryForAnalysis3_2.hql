SELECT issuer_precinct, COUNT(issuer_precinct) AS Violation_Frequency 
FROM hive_db.parking_violation_data 
GROUP BY issuer_precinct 
ORDER BY Violation_Frequency DESC LIMIT 5;
