SELECT *
FROM
(
    SELECT *, RANK() OVER (PARTITION BY season ORDER BY vc_count DESC) AS vc_rnk
    FROM
    (
        SELECT CASE WHEN SUBSTR(month,5,2) IN ('03','04','05') THEN 'spring' 
                    WHEN SUBSTR(month,5,2) IN ('06','07','08') THEN 'summer'
                    WHEN SUBSTR(month,5,2) IN ('09','10','11') THEN 'fall'
                    WHEN SUBSTR(month,5,2) IN ('12','01','02') THEN 'wINter'
                    end AS season, violation_code, count(*) AS vc_count 
        FROM hive_db.parkINg_violation_data
        GROUP BY CASE WHEN SUBSTR(month,5,2) IN ('03','04','05') THEN 'spring' 
                    WHEN SUBSTR(month,5,2) IN ('06','07','08') THEN 'summer'
                    WHEN SUBSTR(month,5,2) IN ('09','10','11') THEN 'fall'
                    WHEN SUBSTR(month,5,2) IN ('12','01','02') THEN 'wINter'
                    end, violation_code
    )a
)b
WHERE vc_rnk < 4;
