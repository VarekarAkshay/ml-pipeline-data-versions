# Feature Store Documentation

Generated: 2025-08-23T12:13:21.740761

## Summary

- Total Features: 14
- Feature Groups: 3

## Feature Group: customer_demographics

**Description**: Customer demographic and profile features

**Source Table**: dim_customers

### Features

| Feature Name | Type | Description | Version | Tags |
|--------------|------|-------------|---------|------|
| customer_id | integer | Unique customer identifier | 1.0 |  |
| age_mean | float | Average customer age | 1.0 | demographic, age |
| geography_mode | string | Most common customer geography | 1.0 | demographic, location |
| gender_mode | string | Customer gender | 1.0 | demographic, gender |
| age_group | string | Customer age category | 1.0 | demographic, category |

## Feature Group: customer_financial

**Description**: Customer financial and account features

**Source Table**: fact_customer_features

### Features

| Feature Name | Type | Description | Version | Tags |
|--------------|------|-------------|---------|------|
| balance_mean | float | Average account balance | 1.0 | financial, balance |
| balance_sum | float | Total account balance | 1.0 | financial, balance, aggregate |
| balance_std | float | Standard deviation of balance | 1.0 | financial, balance, variability |
| credit_score_mean | float | Average credit score | 1.0 | financial, credit |
| estimated_salary_mean | float | Average estimated salary | 1.0 | financial, salary |

## Feature Group: customer_derived

**Description**: Derived and engineered customer features

**Source Table**: fact_customer_features

### Features

| Feature Name | Type | Description | Version | Tags |
|--------------|------|-------------|---------|------|
| balance_to_salary_ratio | float | Balance as proportion of estimated salary | 1.0 | derived, ratio, financial |
| high_value_customer | boolean | High value customer flag (top 25% by balance) | 1.0 | derived, flag, segmentation |
| geographic_risk_score | float | Risk score based on geographic location | 1.0 | derived, risk, geographic |
| credit_score_category | string | Categorical credit score ranges | 1.0 | derived, category, credit |

