How to use:

1 - Create an .env file with the following contents:
- only avaliable for mssql or my sql servers
- driver not required for mysql users

```
DB_SERVER_TYPE= (mssql or mysql)
DB_SERVER=
DB_PORT=
DB_NAME=SPARTA_DATABASE
DB_USER=
DB_PASSWORD=
DB_DRIVER=
```

2 - Run s3_database.sql
- ensure no errors occur during run
- if re-running, ensure you switch to master before run

3 - Run s3_script.py
- all data from the bucket will be placed into the database
- database is ready to use