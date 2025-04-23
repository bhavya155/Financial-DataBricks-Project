# 🏦 Financials - Bank Loan Analysis Using Databricks Delta Lakehouse

This project implements a real-world data pipeline for a U.S.-based bank using **Databricks**, **Delta Lake**, and **incremental loading techniques** to support both **marketing** and **finance** teams with enriched and aggregated data for analysis and reporting.

---

## 🚀 Objective

Build an end-to-end pipeline that:
- Ingests loan transactions, customer master data, and customer driver metrics.
- Applies incremental loads using Delta Lake with deduplication and upsert logic.
- Creates enriched and aggregated reporting tables for analytics and visualization.

---

## 🧱 Data Sources

### 1. **Customer**
- Unique and modified records delivered daily.
- Requires UPSERT using `MERGE` in Delta Lake.

| Column       | Description               |
|--------------|---------------------------|
| customerId   | Unique customer identifier |
| firstName    | First name                |
| lastName     | Last name                 |
| phone        | Phone number              |
| email        | Email address             |
| gender       | Gender                    |
| address      | Customer address          |
| is_active    | Active flag               |

---

### 2. **Customer Drivers**
- Delivered as daily snapshot.
- Requires partitioned inserts with deduplication.

| Column         | Description                         |
|----------------|-------------------------------------|
| date           | Snapshot date                       |
| customerId     | Customer ID                         |
| monthlySalary  | Monthly income in USD               |
| healthScore    | Bank-assigned customer score        |
| currentDebt    | Current debt with the bank          |
| category       | Customer segment/category           |

---

### 3. **Loan Transactions**
- Delivered daily.
- Requires incremental ingestion.

| Column            | Description                         |
|-------------------|-------------------------------------|
| date              | Transaction date                    |
| customerId        | Customer ID                         |
| paymentPeriod     | Term of loan                        |
| loanAmount        | Loan amount requested               |
| currencyType      | Currency (USD, EUR)                 |
| evaluationChannel | Channel used to sell the loan       |
| interestRate      | Interest rate applied               |

---

## 🛠️ Data Processing Stages

### 🔹 Bronze Layer
Raw ingestion from source files into Delta tables with minimal transformation.

### 🔹 Silver Layer
Cleaned and deduplicated data, with:
- UPSERTs for `customer` table.
- Partitioned daily loads for `customer drivers` and `loan transactions`.

### 🔹 Gold Layer
Analytics-ready tables for reporting:
- `featureLoanTrx`: Enriched transactional data joined with customer drivers.
- `aggLoanTrx`: Aggregated metrics grouped by date, payment period, channel, currency, and category.

---

## 📊 Reporting Tables

### `featureLoanTrx`
| Column            | Description                    |
|-------------------|--------------------------------|
| date              | Transaction date               |
| customerId        | Customer ID                    |
| paymentPeriod     | Loan term                      |
| loanAmount        | Loan amount                    |
| currencyType      | Currency                       |
| evaluationChannel | Sales channel                  |
| interestRate      | Interest rate                  |
| monthlySalary     | Customer salary                |
| healthScore       | Customer score                 |
| currentDebt       | Customer’s current debt        |
| category          | Customer category              |
| is_risk_customer  | Derived from health score < 40 |

---

### `aggLoanTrx`
Aggregated KPIs per combination of:
- `date`, `paymentPeriod`, `currencyType`, `evaluationChannel`, `category`

| Metric                | Description                   |
|-----------------------|-------------------------------|
| total_loan_amount     | Total loan amount             |
| avg_loan_amount       | Average loan amount           |
| sum_current_debt      | Sum of current debt           |
| avg_interest_rate     | Average interest rate         |
| max_interest_rate     | Maximum interest rate         |
| min_interest_rate     | Minimum interest rate         |
| avg_score             | Average health score          |
| avg_monthly_salary    | Average monthly salary        |

---

## ♻️ Incremental Load Strategy

- **Delta Lake** used for time-travel and versioning.
- `MERGE` statements for UPSERTs (e.g., customer).
- Partition-based inserts and deduplication for snapshots.
- Safe reprocessing with idempotent logic.

---

## 🧠 Key Learnings

- End-to-end pipeline development using Delta Lakehouse.
- Implementing incremental load strategies with `MERGE`, `NOT EXISTS`, and partitioning.
- Creating analytical and enriched tables for business insights.


