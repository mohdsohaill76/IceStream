# IceStream - Data Quality & Circuit Breaker Developer

## Overview

This module is responsible for detecting bad data and protecting the
IceStream pipeline from data-quality failures.

It validates incoming e-commerce transactions, calculates the error rate,
and activates the Circuit Breaker when the error rate exceeds the **2% threshold**.

---

## Responsibilities

- Validate incoming transaction data
- Detect NULL values
- Detect invalid values
- Detect schema changes
- Calculate data error rate
- Activate the Circuit Breaker
- Route bad records to the DLQ
- Create incident information
- Manage pipeline health and recovery status

---

## Data Quality Flow

```text
Incoming Data
      ↓
Quality Validation
      ↓
Error Rate Calculation
      ↓
Error Rate > 2% ?
    /       \
   NO       YES
   ↓         ↓
Normal     Circuit Breaker
Processing      ↓
               DLQ
