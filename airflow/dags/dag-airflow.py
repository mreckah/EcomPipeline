from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import socket


def copy_dataset():
    """Copy CSV dataset to HDFS."""
    import time
    src = "/opt/airflow/data/input/sales.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(f"Source dataset not found: {src}")
    
    try:
        # Step 1: Copy from Airflow worker to namenode
        print("Step 1: Copying CSV from airflow-worker to namenode...")
        docker_cp_cmd = ["docker", "cp", f"airflow-worker:{src}", "/tmp/sales.csv"]
        result = subprocess.run(docker_cp_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"⚠️  Docker cp to host failed: {result.stderr}")
            # Try alternative: copy directly in namenode
            raise Exception("Direct docker cp failed")
        
        # Step 2: Copy from host to namenode
        print("Step 2: Copying CSV from host to namenode...")
        docker_cp_to_nn = ["docker", "cp", "/tmp/sales.csv", "namenode:/tmp/sales.csv"]
        result = subprocess.run(docker_cp_to_nn, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"⚠️  Docker cp to namenode failed: {result.stderr}")
            raise Exception("Docker cp to namenode failed")
        
        # Step 3: Put file in HDFS
        print("Step 3: Putting CSV into HDFS /data/sales.csv...")
        hdfs_cmd = ["docker", "exec", "namenode", "hadoop", "fs", "-put", "-f", "/tmp/sales.csv", "/data/sales.csv"]
        result = subprocess.run(hdfs_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ Dataset successfully copied to HDFS: /data/sales.csv")
        else:
            print(f"⚠️  HDFS put: {result.stderr}")
            raise Exception("HDFS put failed")
            
    except Exception as e:
        print(f"⚠️  Error during copy: {str(e)}; continuing anyway")


def run_spark_gold_layer():
    """Run Spark gold layer job using docker exec."""
    print("🔥 Starting Spark gold layer job...")
    
    try:
        # Run spark-submit with mounted /opt/airflow/scripts
        # The scripts volume is shared from docker-compose, so spark-master can access it
        result = subprocess.run(
            ["docker", "exec", "spark-master", "/opt/bitnami/spark/bin/spark-submit",
             "--master", "spark://spark-master:7077",
             "--driver-memory", "1g",
             "--executor-memory", "1g",
             "--packages", "org.postgresql:postgresql:42.6.0",
             "/opt/airflow/scripts/gold_layer.py"],
            capture_output=True, text=True, timeout=120, cwd="/opt/airflow/scripts"
        )
        
        print(f"📊 Spark job completed with return code: {result.returncode}")
        if result.stdout:
            print(f"  Stdout:\n{result.stdout}")
        if result.stderr:
            print(f"  Stderr:\n{result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("⚠️  Spark job timed out (120s)")
    except Exception as e:
        print(f"⚠️  Error running Spark job: {str(e)}")


def verify_postgres():
    """Verify Postgres (warehouse) is accepting connections."""
    host = "warehouse-db"
    port = 5432
    user = "warehouse_user"
    pwd = "warehouse_pass"
    db = "warehouse"

    try:
        import psycopg2
        conn = psycopg2.connect(host=host, port=port, user=user, password=pwd, dbname=db, connect_timeout=5)
        conn.close()
        print("✅ Postgres is reachable and accepted a connection")
        return
    except Exception as e:
        print(f"psycopg2 failed ({e}); trying TCP socket check")

    try:
        with socket.create_connection((host, port), timeout=5):
            print("✅ Postgres TCP port is open")
            return
    except Exception as e:
        print(f"⚠️  Postgres not reachable: {e}; continuing anyway")


def run_python_model():
    """Run profit prediction model script inside Airflow container."""
    script = "/opt/airflow/scripts/predict_profits.py"
    if not os.path.exists(script):
        print(f"⚠️  Model script not found at {script}; skipping")
        return
    try:
        result = subprocess.run(["/usr/bin/python3", script], capture_output=True, text=True, timeout=60)
        print(f"Model output:\n{result.stdout}")
        if result.returncode != 0:
            print(f"⚠️  Model script returned {result.returncode}:\n{result.stderr}")
        else:
            print("✅ Profit prediction model completed")
    except Exception as e:
        print(f"⚠️  Error running model: {e}")


def reload_grafana():
    """Reload Grafana provisioning."""
    cmd = "curl -s -u admin:admin -X POST http://grafana:3000/api/admin/provisioning/datasources/reload && curl -s -u admin:admin -X POST http://grafana:3000/api/admin/provisioning/dashboards/reload"
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        print(f"⚠️  Grafana reload returned {r.returncode}; continuing anyway")
    else:
        print("✅ Grafana reloaded")


# Define the DAG
with DAG(
    "sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "sales", "pipeline"],
) as dag:
    t1_copy = PythonOperator(
        task_id="copy_dataset",
        python_callable=copy_dataset,
        doc_md="Copy dataset into working location for Spark",
    )

    t2_spark = PythonOperator(
        task_id="run_spark_gold_layer",
        python_callable=run_spark_gold_layer,
        doc_md="Run Spark gold layer job",
    )

    t3_verify_pg = PythonOperator(
        task_id="verify_postgres",
        python_callable=verify_postgres,
        doc_md="Verify warehouse Postgres is accepting connections",
    )

    t4_model = PythonOperator(
        task_id="profit_prediction",
        python_callable=run_python_model,
        doc_md="Run profit prediction model",
    )

    t5_grafana = PythonOperator(
        task_id="grafana_reload",
        python_callable=reload_grafana,
        doc_md="Reload Grafana provisioning",
    )

    t1_copy >> t2_spark >> t3_verify_pg >> t4_model >> t5_grafana
