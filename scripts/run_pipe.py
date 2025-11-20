import schedule
import time
import subprocess

def run_pipeline():
    subprocess.call(["/bin/bash", "./pipeline.sh"])

# Run immediately
run_pipeline()

# Schedule: every 5 minutes
schedule.every(2).minutes.do(run_pipeline)

# Keep the script running
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Scheduler stopped manually.")
