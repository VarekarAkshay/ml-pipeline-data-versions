-- Sample Queries for Customer Data Warehouse
-- Step 6: Data Transformation & Storage
-- Created: 2025-08-23

-- =====================================
-- BASIC DATA RETRIEVAL QUERIES
-- =====================================

-- 1. Get all customer information with features
SELECT 
    c.customer_id,
    c.customer_num,
    c.geography,
    c.gender,
    c.age,
    c.age_group,
    f.balance_mean,
    f.credit_score_mean,
    f.estimated_salary_mean,
    f.balance_to_salary_ratio,
    f.high_value_customer,
    f.geographic_risk_score
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
ORDER BY c.customer_id;

-- 2. Get customer summary using view
SELECT * FROM view_customer_summary
ORDER BY balance_mean DESC
LIMIT 100;

-- =====================================
-- HIGH-VALUE CUSTOMER ANALYSIS
-- =====================================

-- 3. Identify high-value customers by geography
SELECT 
    c.geography,
    COUNT(*) as total_customers,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_customers,
    ROUND(COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as high_value_percentage,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(MAX(f.balance_mean), 2) as max_balance
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
GROUP BY c.geography
ORDER BY high_value_percentage DESC;

-- 4. Top 10 customers by balance
SELECT 
    c.customer_id,
    c.geography,
    c.gender,
    c.age,
    f.balance_mean,
    f.credit_score_mean,
    f.balance_to_salary_ratio
FROM dim_customers c
JOIN fact_customer_features f ON c.customer_id = f.customer_id
ORDER BY f.balance_mean DESC
LIMIT 10;

-- 5. High-value customers with low risk scores
SELECT 
    c.customer_id,
    c.geography,
    f.balance_mean,
    f.credit_score_mean,
    f.geographic_risk_score,
    f.balance_to_salary_ratio
FROM dim_customers c
JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE f.high_value_customer = 1 
  AND f.geographic_risk_score < 1.0
ORDER BY f.balance_mean DESC;

-- =====================================
-- DEMOGRAPHIC ANALYSIS
-- =====================================

-- 6. Customer distribution by age group and geography
SELECT 
    c.age_group,
    c.geography,
    COUNT(*) as customer_count,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE c.age_group IS NOT NULL
GROUP BY c.age_group, c.geography
ORDER BY c.age_group, customer_count DESC;

-- 7. Gender-based financial analysis
SELECT 
    c.gender,
    COUNT(*) as total_customers,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    ROUND(AVG(f.estimated_salary_mean), 2) as avg_salary,
    ROUND(AVG(f.balance_to_salary_ratio), 3) as avg_balance_salary_ratio,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_count
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
GROUP BY c.gender;

-- =====================================
-- RISK ANALYSIS QUERIES
-- =====================================

-- 8. Risk distribution by geography
SELECT 
    c.geography,
    COUNT(*) as total_customers,
    ROUND(AVG(f.geographic_risk_score), 3) as avg_risk_score,
    ROUND(MIN(f.geographic_risk_score), 3) as min_risk_score,
    ROUND(MAX(f.geographic_risk_score), 3) as max_risk_score,
    COUNT(CASE WHEN f.geographic_risk_score > 1.0 THEN 1 END) as high_risk_customers
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
GROUP BY c.geography
ORDER BY avg_risk_score DESC;

-- 9. Customers with high risk and low credit scores
SELECT 
    c.customer_id,
    c.geography,
    c.age,
    f.credit_score_mean,
    f.balance_mean,
    f.geographic_risk_score,
    f.balance_to_salary_ratio
FROM dim_customers c
JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE f.geographic_risk_score > 1.0 
  AND f.credit_score_mean < 600
ORDER BY f.geographic_risk_score DESC, f.credit_score_mean ASC;

-- =====================================
-- CREDIT SCORE ANALYSIS
-- =====================================

-- 10. Credit score distribution by category
SELECT 
    c.credit_score_category,
    COUNT(*) as customer_count,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.estimated_salary_mean), 2) as avg_salary,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_count
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE c.credit_score_category IS NOT NULL
GROUP BY c.credit_score_category
ORDER BY 
    CASE c.credit_score_category 
        WHEN 'Excellent' THEN 1
        WHEN 'Very Good' THEN 2
        WHEN 'Good' THEN 3
        WHEN 'Fair' THEN 4
        WHEN 'Poor' THEN 5
        ELSE 6
    END;

-- 11. Credit score vs balance correlation
SELECT 
    CASE 
        WHEN f.credit_score_mean >= 750 THEN '750+'
        WHEN f.credit_score_mean >= 700 THEN '700-749'
        WHEN f.credit_score_mean >= 650 THEN '650-699'
        WHEN f.credit_score_mean >= 600 THEN '600-649'
        ELSE 'Below 600'
    END as credit_score_range,
    COUNT(*) as customer_count,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(MIN(f.balance_mean), 2) as min_balance,
    ROUND(MAX(f.balance_mean), 2) as max_balance
FROM fact_customer_features f
WHERE f.credit_score_mean IS NOT NULL
GROUP BY 1
ORDER BY AVG(f.credit_score_mean) DESC;

-- =====================================
-- BALANCE AND SALARY ANALYSIS
-- =====================================

-- 12. Balance to salary ratio analysis
SELECT 
    CASE 
        WHEN f.balance_to_salary_ratio >= 2.0 THEN 'Very High (2.0+)'
        WHEN f.balance_to_salary_ratio >= 1.0 THEN 'High (1.0-2.0)'
        WHEN f.balance_to_salary_ratio >= 0.5 THEN 'Medium (0.5-1.0)'
        WHEN f.balance_to_salary_ratio >= 0.1 THEN 'Low (0.1-0.5)'
        ELSE 'Very Low (<0.1)'
    END as balance_salary_ratio_category,
    COUNT(*) as customer_count,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.estimated_salary_mean), 2) as avg_salary,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score
FROM fact_customer_features f
WHERE f.balance_to_salary_ratio IS NOT NULL
GROUP BY 1
ORDER BY AVG(f.balance_to_salary_ratio) DESC;

-- 13. Salary distribution by geography and gender
SELECT 
    c.geography,
    c.gender,
    COUNT(*) as customer_count,
    ROUND(AVG(f.estimated_salary_mean), 2) as avg_salary,
    ROUND(MIN(f.estimated_salary_mean), 2) as min_salary,
    ROUND(MAX(f.estimated_salary_mean), 2) as max_salary,
    ROUND(STDEV(f.estimated_salary_mean), 2) as salary_std_dev
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE f.estimated_salary_mean IS NOT NULL
GROUP BY c.geography, c.gender
ORDER BY c.geography, avg_salary DESC;

-- =====================================
-- DATA QUALITY AND MONITORING
-- =====================================

-- 14. Data completeness check
SELECT 
    'dim_customers' as table_name,
    COUNT(*) as total_records,
    COUNT(customer_id) as customer_id_count,
    COUNT(geography) as geography_count,
    COUNT(gender) as gender_count,
    COUNT(age) as age_count,
    ROUND(COUNT(geography) * 100.0 / COUNT(*), 2) as geography_completeness,
    ROUND(COUNT(gender) * 100.0 / COUNT(*), 2) as gender_completeness,
    ROUND(COUNT(age) * 100.0 / COUNT(*), 2) as age_completeness
FROM dim_customers

UNION ALL

SELECT 
    'fact_customer_features' as table_name,
    COUNT(*) as total_records,
    COUNT(customer_id) as customer_id_count,
    COUNT(balance_mean) as balance_mean_count,
    COUNT(credit_score_mean) as credit_score_mean_count,
    COUNT(estimated_salary_mean) as salary_count,
    ROUND(COUNT(balance_mean) * 100.0 / COUNT(*), 2) as balance_completeness,
    ROUND(COUNT(credit_score_mean) * 100.0 / COUNT(*), 2) as credit_score_completeness,
    ROUND(COUNT(estimated_salary_mean) * 100.0 / COUNT(*), 2) as salary_completeness
FROM fact_customer_features;

-- 15. Feature statistics summary
SELECT 
    'balance_mean' as feature_name,
    COUNT(*) as non_null_count,
    ROUND(MIN(balance_mean), 2) as min_value,
    ROUND(MAX(balance_mean), 2) as max_value,
    ROUND(AVG(balance_mean), 2) as avg_value,
    ROUND(STDEV(balance_mean), 2) as std_dev
FROM fact_customer_features
WHERE balance_mean IS NOT NULL

UNION ALL

SELECT 
    'credit_score_mean' as feature_name,
    COUNT(*) as non_null_count,
    ROUND(MIN(credit_score_mean), 2) as min_value,
    ROUND(MAX(credit_score_mean), 2) as max_value,
    ROUND(AVG(credit_score_mean), 2) as avg_value,
    ROUND(STDEV(credit_score_mean), 2) as std_dev
FROM fact_customer_features
WHERE credit_score_mean IS NOT NULL

UNION ALL

SELECT 
    'estimated_salary_mean' as feature_name,
    COUNT(*) as non_null_count,
    ROUND(MIN(estimated_salary_mean), 2) as min_value,
    ROUND(MAX(estimated_salary_mean), 2) as max_value,
    ROUND(AVG(estimated_salary_mean), 2) as avg_value,
    ROUND(STDEV(estimated_salary_mean), 2) as std_dev
FROM fact_customer_features
WHERE estimated_salary_mean IS NOT NULL;

-- =====================================
-- TRANSFORMATION MONITORING
-- =====================================

-- 16. Latest transformation summary
SELECT 
    transformation_id,
    transformation_date,
    transformation_type,
    input_records,
    output_records,
    features_created,
    ROUND(processing_time_seconds, 2) as processing_time_sec,
    ROUND(data_quality_score, 3) as quality_score,
    status
FROM transformation_metadata
ORDER BY transformation_date DESC
LIMIT 10;

-- 17. Transformation success rate
SELECT 
    DATE(transformation_date) as transformation_date,
    COUNT(*) as total_transformations,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_transformations,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_transformations,
    ROUND(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate,
    ROUND(AVG(processing_time_seconds), 2) as avg_processing_time
FROM transformation_metadata
WHERE transformation_date >= DATE('now', '-30 days')
GROUP BY DATE(transformation_date)
ORDER BY transformation_date DESC;

-- =====================================
-- BUSINESS INTELLIGENCE QUERIES
-- =====================================

-- 18. Customer segmentation for marketing
SELECT 
    CASE 
        WHEN f.high_value_customer = 1 AND f.credit_score_mean >= 750 THEN 'Premium'
        WHEN f.high_value_customer = 1 AND f.credit_score_mean >= 650 THEN 'High Value'
        WHEN f.balance_mean > 50000 AND f.credit_score_mean >= 650 THEN 'Growth Potential'
        WHEN f.credit_score_mean >= 700 THEN 'Stable'
        ELSE 'Standard'
    END as customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    ROUND(AVG(f.estimated_salary_mean), 2) as avg_salary,
    ROUND(AVG(f.geographic_risk_score), 3) as avg_risk_score
FROM fact_customer_features f
LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY 1
ORDER BY AVG(f.balance_mean) DESC;

-- 19. Geographic expansion opportunity analysis
SELECT 
    c.geography,
    COUNT(*) as total_customers,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(SUM(f.balance_mean), 2) as total_balance,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_customers,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    ROUND(AVG(f.geographic_risk_score), 3) as avg_risk_score,
    -- Opportunity score calculation
    ROUND(
        (AVG(f.balance_mean) / 1000 + COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END)) 
        / (AVG(f.geographic_risk_score) + 0.1), 
        2
    ) as opportunity_score
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
GROUP BY c.geography
ORDER BY opportunity_score DESC;

-- 20. Age group profitability analysis
SELECT 
    c.age_group,
    COUNT(*) as customer_count,
    ROUND(AVG(f.balance_mean), 2) as avg_balance,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_count,
    ROUND(COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as high_value_percentage,
    ROUND(AVG(f.balance_to_salary_ratio), 3) as avg_balance_salary_ratio
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE c.age_group IS NOT NULL
GROUP BY c.age_group
ORDER BY 
    CASE c.age_group 
        WHEN 'Young' THEN 1
        WHEN 'Middle-aged' THEN 2
        WHEN 'Senior' THEN 3
        WHEN 'Elderly' THEN 4
        ELSE 5
    END;

-- =====================================
-- ADVANCED ANALYTICS QUERIES
-- =====================================

-- 21. Customer lifetime value estimation (simplified)
SELECT 
    c.customer_id,
    c.geography,
    c.age_group,
    f.balance_mean,
    f.estimated_salary_mean,
    f.credit_score_mean,
    -- Simple CLV estimation
    ROUND(
        f.balance_mean * 0.1 + 
        f.estimated_salary_mean * 0.05 + 
        (f.credit_score_mean - 300) * 10,
        2
    ) as estimated_clv,
    f.high_value_customer
FROM dim_customers c
JOIN fact_customer_features f ON c.customer_id = f.customer_id
ORDER BY estimated_clv DESC
LIMIT 50;

-- 22. Correlation analysis between features
SELECT 
    'balance_vs_credit_score' as correlation_pair,
    COUNT(*) as sample_size,
    ROUND(
        (COUNT(*) * SUM(balance_mean * credit_score_mean) - SUM(balance_mean) * SUM(credit_score_mean)) /
        (SQRT(COUNT(*) * SUM(balance_mean * balance_mean) - SUM(balance_mean) * SUM(balance_mean)) *
         SQRT(COUNT(*) * SUM(credit_score_mean * credit_score_mean) - SUM(credit_score_mean) * SUM(credit_score_mean))),
        4
    ) as correlation_coefficient
FROM fact_customer_features
WHERE balance_mean IS NOT NULL AND credit_score_mean IS NOT NULL

UNION ALL

SELECT 
    'balance_vs_salary' as correlation_pair,
    COUNT(*) as sample_size,
    ROUND(
        (COUNT(*) * SUM(balance_mean * estimated_salary_mean) - SUM(balance_mean) * SUM(estimated_salary_mean)) /
        (SQRT(COUNT(*) * SUM(balance_mean * balance_mean) - SUM(balance_mean) * SUM(balance_mean)) *
         SQRT(COUNT(*) * SUM(estimated_salary_mean * estimated_salary_mean) - SUM(estimated_salary_mean) * SUM(estimated_salary_mean))),
        4
    ) as correlation_coefficient
FROM fact_customer_features
WHERE balance_mean IS NOT NULL AND estimated_salary_mean IS NOT NULL;

-- =====================================
-- EXPORT QUERIES FOR REPORTING
-- =====================================

-- 23. Executive dashboard summary
SELECT 
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT c.geography) as total_geographies,
    ROUND(AVG(f.balance_mean), 2) as avg_customer_balance,
    ROUND(SUM(f.balance_mean), 2) as total_customer_balances,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_customers,
    ROUND(COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) * 100.0 / COUNT(DISTINCT c.customer_id), 2) as high_value_percentage,
    ROUND(AVG(f.credit_score_mean), 2) as avg_credit_score,
    ROUND(AVG(f.geographic_risk_score), 3) as avg_risk_score,
    MAX(f.last_updated) as last_data_update
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id;

-- 24. Data lineage and audit trail
SELECT 
    tm.transformation_id,
    tm.transformation_date,
    tm.input_records,
    tm.output_records,
    tm.features_created,
    tm.data_quality_score,
    tm.status,
    COUNT(DISTINCT f.customer_id) as customers_with_features,
    AVG(f.feature_completeness) as avg_feature_completeness
FROM transformation_metadata tm
LEFT JOIN fact_customer_features f ON DATE(f.last_updated) = DATE(tm.transformation_date)
GROUP BY tm.transformation_id, tm.transformation_date, tm.input_records, 
         tm.output_records, tm.features_created, tm.data_quality_score, tm.status
ORDER BY tm.transformation_date DESC;