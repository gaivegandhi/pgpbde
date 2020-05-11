WITH Issuer_Precinct_TMP AS 
( 
SELECT issuer_precinct, COUNT(issuer_precinct) AS Top_3_Issuer_Precinct 
FROM hive_db.parking_violation_data 
GROUP BY issuer_precinct 
ORDER BY Top_3_Issuer_Precinct DESC LIMIT 3
) 
SELECT Issuer_Precinct_TMP.issuer_precinct, COUNT(Parking_Data.violation_code) AS Violation_Frequency 
FROM Issuer_Precinct_TMP, hive_db.parking_violation_data as Parking_Data 
WHERE Issuer_Precinct_TMP.issuer_precinct = Parking_Data.issuer_precinct 
GROUP BY Issuer_Precinct_TMP.issuer_precinct 
ORDER BY Violation_Frequency DESC;
