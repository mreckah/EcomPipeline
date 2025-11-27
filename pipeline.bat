@echo off
setlocal enabledelayedexpansion

REM ---------------- CONFIG ----------------
set DATASET_LOCAL_PATH=.\data\input\sales.csv
set NIFI_LOCAL_INBOX=.\data
set HDFS_TARGET_PATH=/marketing/sales/sales.csv

set NIFI_USER=admin
set NIFI_PASS=admin123456789
set NIFI_URL=https://psychic-space-spork-xr5jppvpwj42vwj7-8443.app.github.dev
set NIFI_PROCESS_GROUP_ID=d260d54e-0198-1000-2efb-5494af2dbccf

set SPARK_MASTER_CONTAINER=spark-master
set SPARK_APP_DIR=/opt/bitnami/spark/app
set SPARK_APP_PATH=%SPARK_APP_DIR%/gold_layer.py
set SPARK_APP_PATH_=%SPARK_APP_DIR%/marketing_job.py
set SPARK_MASTER_URL=spark://spark-master:7077

set NAMENODE_CONTAINER=namenode

set PG_CONTAINER=warehouse-db
set PG_DB=warehouse
set PG_USER=warehouse_user
set PG_PASS=warehouse_pass
set PG_TABLE=sales_data

set GRAFANA_URL=http://localhost:3000
set GRAFANA_USER=admin
set GRAFANA_PASS=admin
REM -----------------------------------------

echo == 1) Checking dataset exists ==
if not exist "%DATASET_LOCAL_PATH%" (
    echo ERROR: Dataset not found at %DATASET_LOCAL_PATH%
    exit /b 1
)

echo == 2) Placing dataset for NiFi ==
copy /Y "%DATASET_LOCAL_PATH%" "%NIFI_LOCAL_INBOX%\sales.csv"

echo == 3) Copy dataset into HDFS ==
REM Check if datanode container is running
docker ps --filter "name=datanode" --format "{{.Names}}" | findstr /C:"datanode" >nul
if errorlevel 1 (
    echo ERROR: DataNode container is not running. Please start it with: docker-compose up -d datanode
    exit /b 1
)

REM Wait for datanode to be ready and registered with namenode
echo Waiting for DataNode to be ready and registered with NameNode...
set datanode_attempts=0
:DATANODE_WAIT_LOOP
set /a datanode_attempts+=1
docker exec %NAMENODE_CONTAINER% hdfs dfsadmin -report 2>nul | findstr /C:"Live datanodes" | findstr /C:"1" >nul
if not errorlevel 1 (
    echo DataNode is ready and registered.
    goto :DATANODE_READY
)
if !datanode_attempts! lss 30 (
    echo   ...waiting for DataNode (attempt !datanode_attempts!/30)...
    timeout /t 2 /nobreak >nul
    goto :DATANODE_WAIT_LOOP
)
echo ERROR: DataNode did not become ready after 30 attempts.
echo Please check DataNode status: docker logs datanode
echo Please check NameNode status: docker logs namenode
docker exec %NAMENODE_CONTAINER% hdfs dfsadmin -report
exit /b 1

:DATANODE_READY
docker cp "%DATASET_LOCAL_PATH%" %NAMENODE_CONTAINER%:/tmp/sales.csv
docker exec %NAMENODE_CONTAINER% hdfs dfs -mkdir -p /marketing/sales
docker exec %NAMENODE_CONTAINER% hdfs dfs -put -f /tmp/sales.csv "%HDFS_TARGET_PATH%"
if errorlevel 1 (
    echo ERROR: Failed to copy file to HDFS. Checking DataNode status...
    docker exec %NAMENODE_CONTAINER% hdfs dfsadmin -report
    exit /b 1
)

echo == 4) Starting NiFi process group ==
curl -k -u %NIFI_USER%:%NIFI_PASS% -X PUT ^
  "%NIFI_URL%/nifi-api/flow/process-groups/%NIFI_PROCESS_GROUP_ID%" ^
  -H "Content-Type: application/json" ^
  -d "{\"id\":\"%NIFI_PROCESS_GROUP_ID%\",\"state\":\"RUNNING\"}"

timeout /t 10 /nobreak >nul

curl -k -u %NIFI_USER%:%NIFI_PASS% -X PUT ^
  "%NIFI_URL%/nifi-api/flow/process-groups/%NIFI_PROCESS_GROUP_ID%" ^
  -H "Content-Type: application/json" ^
  -d "{\"id\":\"%NIFI_PROCESS_GROUP_ID%\",\"state\":\"STOPPED\"}"
echo NiFi process group stopped ✅

echo == 5) Waiting for file in HDFS ==
set hdfs_attempts=0
:HDFS_CHECK_LOOP
set /a hdfs_attempts+=1
docker exec %NAMENODE_CONTAINER% hdfs dfs -test -e "%HDFS_TARGET_PATH%" 2>nul
if not errorlevel 1 (
    echo File detected in HDFS.
    goto :HDFS_DONE
)
if !hdfs_attempts! lss 60 (
    echo   ...not yet (attempt !hdfs_attempts!/60); checking again in 5s
    timeout /t 5 /nobreak >nul
    goto :HDFS_CHECK_LOOP
)
echo ERROR: Timed out waiting for file in HDFS
exit /b 1
:HDFS_DONE

echo == 6) Copy Spark job and run it ==
docker exec %SPARK_MASTER_CONTAINER% mkdir -p "%SPARK_APP_DIR%/libs"
docker cp ".\scripts\gold_layer.py" "%SPARK_MASTER_CONTAINER%:%SPARK_APP_PATH%"
docker cp ".\scripts\marketing_job.py" "%SPARK_MASTER_CONTAINER%:%SPARK_APP_PATH%"
docker cp ".\libs\postgresql-42.6.0.jar" "%SPARK_MASTER_CONTAINER%:%SPARK_APP_DIR%/libs/postgresql-42.6.0.jar"

docker exec %SPARK_MASTER_CONTAINER% ^
  "/opt/bitnami/spark/bin/spark-submit" ^
  --master %SPARK_MASTER_URL% ^
  --jars "%SPARK_APP_DIR%/libs/postgresql-42.6.0.jar" ^
  "%SPARK_APP_PATH%"

docker exec %SPARK_MASTER_CONTAINER% ^
  "/opt/bitnami/spark/bin/spark-submit" ^
  --master %SPARK_MASTER_URL% ^
  --jars "%SPARK_APP_DIR%/libs/postgresql-42.6.0.jar" ^
  "%SPARK_APP_PATH_%"

echo == 7) Run profit prediction with anomalies and visualization ==
python .\scripts\predict_profits.py

echo == 8) Reload Grafana provisioning ==
curl -sS -u %GRAFANA_USER%:%GRAFANA_PASS% -X POST "%GRAFANA_URL%/api/admin/provisioning/datasources/reload" >nul 2>&1
curl -sS -u %GRAFANA_USER%:%GRAFANA_PASS% -X POST "%GRAFANA_URL%/api/admin/provisioning/dashboards/reload" >nul 2>&1

echo ✅ Pipeline finished successfully.
echo == 9) Run Streamlit App ==
streamlit run .\scripts\streamlit_app.py

pause

