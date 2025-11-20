#!/usr/bin/env bash
set -euo pipefail

### --------- CONFIG ---------
DATASET_LOCAL_PATH="./data/input/sales.csv"
NIFI_LOCAL_INBOX="./data"
HDFS_TARGET_PATH="/marketing/sales/sales.csv"

# NiFi
NIFI_USER="admin"
NIFI_PASS="admin123456789"
NIFI_URL="https://psychic-space-spork-xr5jppvpwj42vwj7-8443.app.github.dev"
NIFI_PROCESS_GROUP_ID="d260d54e-0198-1000-2efb-5494af2dbccf"

# Spark
SPARK_MASTER_CONTAINER="spark-master"
SPARK_APP_DIR="/opt/bitnami/spark/app"
SPARK_APP_PATH="${SPARK_APP_DIR}/marketing_job.py"
SPARK_MASTER_URL="spark://spark-master:7077"

# HDFS
NAMENODE_CONTAINER="namenode"

# Postgres
PG_CONTAINER="warehouse-db"
PG_DB="warehouse"
PG_USER="warehouse_user"
PG_PASS="warehouse_pass"
PG_TABLE="sales_data"

# Grafana
GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASS="admin"
### ------------------------

echo "==> 1) Checking dataset exists"
if [[ ! -f "${DATASET_LOCAL_PATH}" ]]; then
  echo "ERROR: Dataset not found at ${DATASET_LOCAL_PATH}"
  exit 1
fi

echo "==> 2) Placing dataset for NiFi"
cp -f "${DATASET_LOCAL_PATH}" "${NIFI_LOCAL_INBOX}/sales.csv"

echo "==> 3) Copy dataset into HDFS"
docker cp "${DATASET_LOCAL_PATH}" namenode:/tmp/sales.csv
docker exec namenode hdfs dfs -mkdir -p /marketing/sales
docker exec namenode hdfs dfs -put -f /tmp/sales.csv "${HDFS_TARGET_PATH}"

echo "==> 4) Starting NiFi process group"
curl -k -u "${NIFI_USER}:${NIFI_PASS}" -X PUT \
  "${NIFI_URL}/nifi-api/flow/process-groups/${NIFI_PROCESS_GROUP_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${NIFI_PROCESS_GROUP_ID}\",\"state\":\"RUNNING\"}"

sleep 10

curl -k -u "${NIFI_USER}:${NIFI_PASS}" -X PUT \
  "${NIFI_URL}/nifi-api/flow/process-groups/${NIFI_PROCESS_GROUP_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${NIFI_PROCESS_GROUP_ID}\",\"state\":\"STOPPED\"}"
echo "NiFi process group stopped ✅"

echo "==> 5) Waiting for file in HDFS"
for i in {1..60}; do
  if docker exec "${NAMENODE_CONTAINER}" hdfs dfs -test -e "${HDFS_TARGET_PATH}"; then
    echo "File detected in HDFS."
    break
  fi
  echo "  ...not yet (attempt $i); sleeping 5s"
  sleep 5
  if [[ $i -eq 60 ]]; then
    echo "ERROR: Timed out waiting for file in HDFS"
    exit 1
  fi
done

echo "==> 6) Copy Spark job and run it"
docker exec "${SPARK_MASTER_CONTAINER}" mkdir -p "${SPARK_APP_DIR}/libs"
docker cp "./scripts/marketing_job.py" "${SPARK_MASTER_CONTAINER}:${SPARK_APP_PATH}"
docker cp "./libs/postgresql-42.6.0.jar" "${SPARK_MASTER_CONTAINER}:${SPARK_APP_DIR}/libs/postgresql-42.6.0.jar"

docker exec "${SPARK_MASTER_CONTAINER}" \
  "/opt/bitnami/spark/bin/spark-submit" \
  --master "${SPARK_MASTER_URL}" \
  --jars "${SPARK_APP_DIR}/libs/postgresql-42.6.0.jar" \
  "${SPARK_APP_PATH}"

echo "==> 7) Run profit prediction with anomalies and visualization"
python3 ./scripts/predict_profits.py

echo "==> 8) Reload Grafana provisioning"
curl -sS -u "${GRAFANA_USER}:${GRAFANA_PASS}" -X POST "${GRAFANA_URL}/api/admin/provisioning/datasources/reload" >/dev/null || true
curl -sS -u "${GRAFANA_USER}:${GRAFANA_PASS}" -X POST "${GRAFANA_URL}/api/admin/provisioning/dashboards/reload" >/dev/null || true

echo "✅ Pipeline finished successfully."
