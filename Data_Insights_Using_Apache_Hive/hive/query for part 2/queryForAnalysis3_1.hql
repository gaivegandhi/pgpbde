SELECT violation_precinct, COUNT(violation_precinct) AS Violation_Frequency 
FROM hive_db.parking_violation_data 
GROUP BY violation_precinct 
ORDER BY Violation_Frequency DESC LIMIT 5;
