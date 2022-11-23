# Sparkify Data Warehousing

An imaginary startup company Sparkify provides music streaming service, and has stored data on songs and user activity in JSON file format in AWS S3 storage. Since data stored in individual files cannot be easily utilized, they configured their own relational database through process described in [this project](https://github.com/grainpowder/postgresql-data-modeling). However, storage and maintenance cost for actual on-premise server has increased as their service and corresponding log data size grows. As a result, they are planning to transfer their data into cloud data warehouse; AWS Redshift.

## Repository

* `aws_setup.py`: CLI app to initiate and delete VPC, IAM and Redshift resources
* `sql_queries.py`: collection of SQL commands that defines job on initiated Redshift cluster
* `create_tables.py`: executor of table creation queries defined in `sql_queries.py`
* `etl.py`:  executor of ETL job queries defined in `sql_queries.py`

## Execute

First, create Python 3.10 virtual environment to install required packages to execute predefined programs.

```
python3 -m venv venv
source venv/bin/activate
python --version  # Python 3.10.8
pip install -r requirements.txt
```

Then, initiate AWS resources using Administrator permission granted to corresponding IAM user(if there is no Administrator user in the account, create one by following steps explained in [official document](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started_create-admin-group.html)). For example, if you set profile name of the Administrator user as `homer.simpson` and wish to set `Doh!nuts123` as master user password of Redshift cluster, execute following command.

```
python aws_setup.py build-resources homer.simpson Doh!nuts123
```

After creating Redshift cluster, tables to store required information from log data have to be created prior to any ETL jobs. This can be achieved by executing following command.

```
python create_tables.py
```

Finally, execute command below to insert log data into the tables defined in Redshift cluster. This process utilizes Redshift `COPY` function, in order to execute insert job in parallel.

```
python etl.py
```

After all the tryouts, be sure to delete every running instances that can cause unexpected charges.

```
python aws_setup.py delete-resources
```