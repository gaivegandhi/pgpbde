SELECT COUNT(DISTINCT registration_state) 
FROM hive_db.parking_violation_data 
WHERE summons_number > 0;
