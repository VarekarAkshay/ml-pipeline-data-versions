-- Sample Feature Store Queries
-- Step 7: Feature Store Implementation
-- Created: 2025-08-23

-- =====================================
-- FEATURE DISCOVERY AND EXPLORATION
-- =====================================

-- 1. List all available features
SELECT 
    f.name,
    f.description,
    f.data_type,
    f.version,
    fg.name as feature_group,
    f.source_table,
    f.tags,
    f.status
FROM features_metadata f
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE
ORDER BY fg.name, f.name;

-- 2. Search features by tags
SELECT 
    f.name,
    f.description,
    fg.name as feature_group,
    f.tags
FROM features_metadata f
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE 
  AND (f.tags LIKE '%financial%' OR f.tags LIKE '%demographic%')
ORDER BY f.name;

-- 3. Find features by data type
SELECT 
    f.name,
    f.description,
    f.data_type,
    fg.name as feature_group
FROM features_metadata f
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.data_type = 'float'
  AND f.is_active = TRUE
ORDER BY fg.name, f.name;

-- 4. Get feature metadata with statistics
SELECT 
    f.name,
    f.description,
    f.data_type,
    f.statistics,
    f.quality_metrics,
    f.last_updated
FROM features_metadata f
WHERE f.name = 'balance_mean'
  AND f.is_active = TRUE;

-- =====================================
-- ONLINE FEATURE SERVING QUERIES
-- =====================================

-- 5. Get all online features for a specific customer
SELECT 
    fm.name as feature_name,
    JSON_EXTRACT(of.feature_value, '$') as feature_value,
    fm.data_type,
    of.last_updated
FROM online_features of
JOIN features_metadata fm ON of.feature_id = fm.feature_id
WHERE of.entity_id = '12345'
  AND fm.is_active = TRUE
ORDER BY fm.name;

-- 6. Get specific features for a customer (feature serving)
SELECT 
    fm.name as feature_name,
    JSON_EXTRACT(of.feature_value, '$') as feature_value,
    of.last_updated
FROM online_features of
JOIN features_metadata fm ON of.feature_id = fm.feature_id
WHERE of.entity_id = '12345'
  AND fm.name IN ('balance_mean', 'credit_score_mean', 'high_value_customer')
  AND fm.is_active = TRUE;

-- 7. Get features for multiple customers (batch serving)
SELECT 
    of.entity_id,
    fm.name as feature_name,
    JSON_EXTRACT(of.feature_value, '$') as feature_value,
    of.last_updated
FROM online_features of
JOIN features_metadata fm ON of.feature_id = fm.feature_id
WHERE of.entity_id IN ('12345', '67890', '11111')
  AND fm.name IN ('balance_mean', 'credit_score_mean')
  AND fm.is_active = TRUE
ORDER BY of.entity_id, fm.name;

-- 8. Get features that are fresh (recently updated)
SELECT 
    of.entity_id,
    fm.name as feature_name,
    JSON_EXTRACT(of.feature_value, '$') as feature_value,
    of.last_updated
FROM online_features of
JOIN features_metadata fm ON of.feature_id = fm.feature_id
WHERE of.entity_id = '12345'
  AND of.last_updated >= datetime('now', '-1 hour')
  AND fm.is_active = TRUE
ORDER BY of.last_updated DESC;

-- =====================================
-- HISTORICAL FEATURE QUERIES
-- =====================================

-- 9. Get historical features for training data
SELECT 
    off.entity_id,
    fm.name as feature_name,
    JSON_EXTRACT(off.feature_value, '$') as feature_value,
    off.feature_timestamp
FROM offline_features off
JOIN features_metadata fm ON off.feature_id = fm.feature_id
WHERE off.entity_id IN ('12345', '67890', '11111')
  AND fm.name IN ('balance_mean', 'credit_score_mean', 'high_value_customer')
  AND off.feature_timestamp BETWEEN '2025-08-01' AND '2025-08-20'
  AND fm.is_active = TRUE
ORDER BY off.entity_id, off.feature_timestamp, fm.name;

-- 10. Point-in-time feature lookup for specific timestamp
SELECT 
    off.entity_id,
    fm.name as feature_name,
    JSON_EXTRACT(off.feature_value, '$') as feature_value,
    off.feature_timestamp
FROM offline_features off
JOIN features_metadata fm ON off.feature_id = fm.feature_id
WHERE off.entity_id = '12345'
  AND off.feature_timestamp <= '2025-08-15 10:00:00'
  AND fm.is_active = TRUE
  AND (off.entity_id, fm.feature_id, off.feature_timestamp) IN (
    SELECT entity_id, feature_id, MAX(feature_timestamp)
    FROM offline_features
    WHERE entity_id = '12345' AND feature_timestamp <= '2025-08-15 10:00:00'
    GROUP BY entity_id, feature_id
  )
ORDER BY fm.name;

-- 11. Feature evolution over time for a customer
SELECT 
    fm.name as feature_name,
    JSON_EXTRACT(off.feature_value, '$') as feature_value,
    off.feature_timestamp,
    DATE(off.feature_timestamp) as feature_date
FROM offline_features off
JOIN features_metadata fm ON off.feature_id = fm.feature_id
WHERE off.entity_id = '12345'
  AND fm.name = 'balance_mean'
  AND off.feature_timestamp >= '2025-08-01'
  AND fm.is_active = TRUE
ORDER BY off.feature_timestamp;

-- =====================================
-- FEATURE USAGE ANALYTICS
-- =====================================

-- 12. Most accessed features
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    COUNT(*) as access_count,
    COUNT(DISTINCT al.entity_id) as unique_entities,
    AVG(al.response_time_ms) as avg_response_time
FROM feature_access_logs al
JOIN features_metadata fm ON al.feature_id = fm.feature_id
JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE al.access_time >= datetime('now', '-7 days')
GROUP BY fm.feature_id, fm.name, fg.name
ORDER BY access_count DESC
LIMIT 10;

-- 13. Feature access patterns by type
SELECT 
    access_type,
    COUNT(*) as total_requests,
    COUNT(DISTINCT entity_id) as unique_entities,
    AVG(response_time_ms) as avg_response_time,
    DATE(access_time) as access_date
FROM feature_access_logs
WHERE access_time >= datetime('now', '-30 days')
GROUP BY access_type, DATE(access_time)
ORDER BY access_date DESC, access_type;

-- 14. Feature usage by request source
SELECT 
    request_source,
    COUNT(*) as request_count,
    COUNT(DISTINCT feature_id) as unique_features_accessed,
    AVG(response_time_ms) as avg_response_time
FROM feature_access_logs
WHERE access_time >= datetime('now', '-7 days')
GROUP BY request_source
ORDER BY request_count DESC;

-- 15. Slowest performing features
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    COUNT(*) as request_count,
    AVG(al.response_time_ms) as avg_response_time,
    MAX(al.response_time_ms) as max_response_time
FROM feature_access_logs al
JOIN features_metadata fm ON al.feature_id = fm.feature_id
JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE al.access_time >= datetime('now', '-24 hours')
  AND al.response_time_ms IS NOT NULL
GROUP BY fm.feature_id, fm.name, fg.name
HAVING COUNT(*) >= 10  -- Only features with significant usage
ORDER BY avg_response_time DESC
LIMIT 10;

-- =====================================
-- FEATURE QUALITY MONITORING
-- =====================================

-- 16. Latest data quality metrics for all features
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    fqm.metric_name,
    fqm.metric_value,
    fqm.status,
    fqm.measurement_time
FROM feature_quality_metrics fqm
JOIN features_metadata fm ON fqm.feature_id = fm.feature_id
JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE fqm.measurement_time >= datetime('now', '-24 hours')
  AND fm.is_active = TRUE
ORDER BY fg.name, fm.name, fqm.metric_name;

-- 17. Features with quality issues
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    COUNT(CASE WHEN fqm.status = 'fail' THEN 1 END) as failed_checks,
    COUNT(CASE WHEN fqm.status = 'warning' THEN 1 END) as warning_checks,
    MAX(fqm.measurement_time) as last_check_time
FROM features_metadata fm
LEFT JOIN feature_quality_metrics fqm ON fm.feature_id = fqm.feature_id
LEFT JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE fm.is_active = TRUE
  AND fqm.measurement_time >= datetime('now', '-7 days')
GROUP BY fm.feature_id, fm.name, fg.name
HAVING failed_checks > 0 OR warning_checks > 0
ORDER BY failed_checks DESC, warning_checks DESC;

-- 18. Feature drift detection results
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    fdd.drift_score,
    fdd.is_drift_detected,
    fdd.baseline_period_start,
    fdd.baseline_period_end,
    fdd.comparison_period_start,
    fdd.comparison_period_end,
    fdd.detection_time
FROM feature_drift_detection fdd
JOIN features_metadata fm ON fdd.feature_id = fm.feature_id
JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE fdd.detection_time >= datetime('now', '-30 days')
  AND fm.is_active = TRUE
ORDER BY fdd.detection_time DESC;

-- 19. Features requiring attention (quality + drift)
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    CASE 
        WHEN fdd.is_drift_detected = 1 THEN 'Drift Detected'
        WHEN fqm.status = 'fail' THEN 'Quality Failed'
        WHEN fqm.status = 'warning' THEN 'Quality Warning'
        ELSE 'Unknown Issue'
    END as issue_type,
    COALESCE(fdd.drift_score, fqm.metric_value) as score_value,
    COALESCE(fdd.detection_time, fqm.measurement_time) as issue_time
FROM features_metadata fm
LEFT JOIN feature_drift_detection fdd ON fm.feature_id = fdd.feature_id AND fdd.is_drift_detected = 1
LEFT JOIN feature_quality_metrics fqm ON fm.feature_id = fqm.feature_id AND fqm.status IN ('fail', 'warning')
LEFT JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE (fdd.is_drift_detected = 1 OR fqm.status IN ('fail', 'warning'))
  AND fm.is_active = TRUE
ORDER BY issue_time DESC;

-- =====================================
-- FEATURE LINEAGE AND DEPENDENCIES
-- =====================================

-- 20. Complete feature lineage
SELECT 
    f.name as feature_name,
    fg.name as feature_group,
    fl.upstream_source_type,
    fl.upstream_source_name,
    fl.upstream_source_column,
    fl.transformation_logic
FROM features_metadata f
JOIN feature_lineage fl ON f.feature_id = fl.downstream_feature_id
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE
ORDER BY fg.name, f.name;

-- 21. Feature dependencies (parent-child relationships)
SELECT 
    pf.name as parent_feature,
    cf.name as child_feature,
    fd.dependency_type,
    fd.description,
    pfg.name as parent_group,
    cfg.name as child_group
FROM feature_dependencies fd
JOIN features_metadata pf ON fd.parent_feature_id = pf.feature_id
JOIN features_metadata cf ON fd.child_feature_id = cf.feature_id
JOIN feature_groups pfg ON pf.feature_group_id = pfg.group_id
JOIN feature_groups cfg ON cf.feature_group_id = cfg.group_id
WHERE pf.is_active = TRUE AND cf.is_active = TRUE
ORDER BY pfg.name, pf.name;

-- 22. Find all features derived from a specific source
SELECT 
    f.name as derived_feature,
    fg.name as feature_group,
    fl.transformation_logic,
    fl.created_date
FROM feature_lineage fl
JOIN features_metadata f ON fl.downstream_feature_id = f.feature_id
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE fl.upstream_source_name = 'fact_customer_features'
  AND f.is_active = TRUE
ORDER BY fg.name, f.name;

-- =====================================
-- OPERATIONAL QUERIES
-- =====================================

-- 23. Feature store health check
SELECT 
    'Total Features' as metric,
    COUNT(*) as value
FROM features_metadata 
WHERE is_active = TRUE

UNION ALL

SELECT 
    'Feature Groups' as metric,
    COUNT(*) as value
FROM feature_groups 
WHERE is_active = TRUE

UNION ALL

SELECT 
    'Online Features Available' as metric,
    COUNT(DISTINCT entity_id) as value
FROM online_features

UNION ALL

SELECT 
    'Total Feature Values' as metric,
    COUNT(*) as value
FROM online_features

UNION ALL

SELECT 
    'API Requests Today' as metric,
    COUNT(*) as value
FROM feature_access_logs 
WHERE DATE(access_time) = DATE('now');

-- 24. Feature freshness report
SELECT 
    fg.name as feature_group,
    fm.name as feature_name,
    COUNT(DISTINCT of.entity_id) as entities_count,
    MIN(of.last_updated) as oldest_update,
    MAX(of.last_updated) as newest_update,
    CASE 
        WHEN MAX(of.last_updated) >= datetime('now', '-1 hour') THEN 'Fresh'
        WHEN MAX(of.last_updated) >= datetime('now', '-24 hours') THEN 'Stale'
        ELSE 'Very Stale'
    END as freshness_status
FROM features_metadata fm
LEFT JOIN online_features of ON fm.feature_id = of.feature_id
LEFT JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE fm.is_active = TRUE
GROUP BY fg.group_id, fm.feature_id, fg.name, fm.name
ORDER BY fg.name, fm.name;

-- 25. Feature store capacity and growth
SELECT 
    'Current Month' as period,
    COUNT(*) as total_requests,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT feature_id) as features_accessed,
    AVG(response_time_ms) as avg_response_time
FROM feature_access_logs 
WHERE access_time >= date('now', 'start of month')

UNION ALL

SELECT 
    'Previous Month' as period,
    COUNT(*) as total_requests,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT feature_id) as features_accessed,
    AVG(response_time_ms) as avg_response_time
FROM feature_access_logs 
WHERE access_time >= date('now', 'start of month', '-1 month')
  AND access_time < date('now', 'start of month')

UNION ALL

SELECT 
    'Last 7 Days' as period,
    COUNT(*) as total_requests,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT feature_id) as features_accessed,
    AVG(response_time_ms) as avg_response_time
FROM feature_access_logs 
WHERE access_time >= datetime('now', '-7 days');

-- =====================================
-- FEATURE CLEANUP AND MAINTENANCE
-- =====================================

-- 26. Unused features (no access in last 30 days)
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    fm.creation_date,
    fm.last_updated,
    MAX(al.access_time) as last_accessed
FROM features_metadata fm
LEFT JOIN feature_access_logs al ON fm.feature_id = al.feature_id
LEFT JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE fm.is_active = TRUE
GROUP BY fm.feature_id, fm.name, fg.name, fm.creation_date, fm.last_updated
HAVING last_accessed IS NULL OR last_accessed < datetime('now', '-30 days')
ORDER BY fm.creation_date;

-- 27. Features with expired TTL in online store
SELECT 
    fm.name as feature_name,
    fg.name as feature_group,
    COUNT(*) as expired_count,
    MIN(of.ttl_expires) as earliest_expiry,
    MAX(of.ttl_expires) as latest_expiry
FROM online_features of
JOIN features_metadata fm ON of.feature_id = fm.feature_id
JOIN feature_groups fg ON fm.feature_group_id = fg.group_id
WHERE of.ttl_expires IS NOT NULL 
  AND of.ttl_expires < datetime('now')
  AND fm.is_active = TRUE
GROUP BY fm.feature_id, fm.name, fg.name
ORDER BY expired_count DESC;

-- 28. Storage usage by feature group
SELECT 
    fg.name as feature_group,
    COUNT(DISTINCT fm.feature_id) as features_count,
    COUNT(DISTINCT of.entity_id) as entities_with_online_features,
    COUNT(of.entity_id) as total_online_values,
    COUNT(off.entity_id) as total_offline_values
FROM feature_groups fg
LEFT JOIN features_metadata fm ON fg.group_id = fm.feature_group_id AND fm.is_active = TRUE
LEFT JOIN online_features of ON fm.feature_id = of.feature_id
LEFT JOIN offline_features off ON fm.feature_id = off.feature_id
GROUP BY fg.group_id, fg.name
ORDER BY total_online_values DESC;

-- =====================================
-- BUSINESS INTELLIGENCE QUERIES
-- =====================================

-- 29. Customer segmentation using features
SELECT 
    of1.entity_id,
    CAST(JSON_EXTRACT(of1.feature_value, '$') AS REAL) as balance_mean,
    CAST(JSON_EXTRACT(of2.feature_value, '$') AS REAL) as credit_score_mean,
    CAST(JSON_EXTRACT(of3.feature_value, '$') AS INTEGER) as high_value_customer,
    JSON_EXTRACT(of4.feature_value, '$') as geography,
    CASE 
        WHEN CAST(JSON_EXTRACT(of3.feature_value, '$') AS INTEGER) = 1 
             AND CAST(JSON_EXTRACT(of2.feature_value, '$') AS REAL) >= 750 THEN 'Premium'
        WHEN CAST(JSON_EXTRACT(of3.feature_value, '$') AS INTEGER) = 1 THEN 'High Value'
        WHEN CAST(JSON_EXTRACT(of2.feature_value, '$') AS REAL) >= 700 THEN 'Good Credit'
        ELSE 'Standard'
    END as customer_segment
FROM online_features of1
JOIN online_features of2 ON of1.entity_id = of2.entity_id
JOIN online_features of3 ON of1.entity_id = of3.entity_id
JOIN online_features of4 ON of1.entity_id = of4.entity_id
JOIN features_metadata fm1 ON of1.feature_id = fm1.feature_id AND fm1.name = 'balance_mean'
JOIN features_metadata fm2 ON of2.feature_id = fm2.feature_id AND fm2.name = 'credit_score_mean'
JOIN features_metadata fm3 ON of3.feature_id = fm3.feature_id AND fm3.name = 'high_value_customer'
JOIN features_metadata fm4 ON of4.feature_id = fm4.feature_id AND fm4.name = 'geography_mode'
WHERE fm1.is_active = TRUE AND fm2.is_active = TRUE AND fm3.is_active = TRUE AND fm4.is_active = TRUE
ORDER BY balance_mean DESC
LIMIT 100;

-- 30. Feature correlation analysis (sample data)
WITH feature_values AS (
    SELECT 
        of1.entity_id,
        CAST(JSON_EXTRACT(of1.feature_value, '$') AS REAL) as balance_mean,
        CAST(JSON_EXTRACT(of2.feature_value, '$') AS REAL) as credit_score_mean,
        CAST(JSON_EXTRACT(of3.feature_value, '$') AS REAL) as estimated_salary_mean
    FROM online_features of1
    JOIN online_features of2 ON of1.entity_id = of2.entity_id
    JOIN online_features of3 ON of1.entity_id = of3.entity_id
    JOIN features_metadata fm1 ON of1.feature_id = fm1.feature_id AND fm1.name = 'balance_mean'
    JOIN features_metadata fm2 ON of2.feature_id = fm2.feature_id AND fm2.name = 'credit_score_mean'
    JOIN features_metadata fm3 ON of3.feature_id = fm3.feature_id AND fm3.name = 'estimated_salary_mean'
    WHERE fm1.is_active = TRUE AND fm2.is_active = TRUE AND fm3.is_active = TRUE
)
SELECT 
    'balance_vs_credit_score' as correlation_pair,
    COUNT(*) as sample_size,
    AVG(balance_mean) as avg_balance,
    AVG(credit_score_mean) as avg_credit_score,
    -- Simple correlation approximation
    (AVG(balance_mean * credit_score_mean) - AVG(balance_mean) * AVG(credit_score_mean)) /
    (SQRT((AVG(balance_mean * balance_mean) - AVG(balance_mean) * AVG(balance_mean)) *
          (AVG(credit_score_mean * credit_score_mean) - AVG(credit_score_mean) * AVG(credit_score_mean)))) as correlation_approx
FROM feature_values
WHERE balance_mean IS NOT NULL AND credit_score_mean IS NOT NULL;