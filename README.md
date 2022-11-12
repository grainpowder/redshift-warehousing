# Sparkify Data Warehousing

An imaginary startup company Sparkify provides music streaming service, and has been stored data on songs and user activity in JSON file format in AWS S3 storage. Since data stored in individual files cannot be easily utilized, they configured their own on-premise relational database through process described in [this project](https://github.com/grainpowder/postgresql-data-modeling). However, storage and maintenance cost for actual server has been increased as their service and corresponding log data size grows. As a result,they are planning to transfer their data into cloud data warehouse; AWS Redshift.

## Repository

## Execute

### 1. Create VPC to run Redshift cluster

### 2. Initiate Redshift cluster and create corresponding IAM role

### 3. Define and execute ETL job in need