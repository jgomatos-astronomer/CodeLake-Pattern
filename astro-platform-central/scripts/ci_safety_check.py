import sys
import os
from airflow.models import DagBag

sys.path.append(os.path.join(os.getcwd(), 'dags'))

print("--- 🕵️‍♀️ Starting DAG Integrity Check ---")
dag_bag = DagBag(dag_folder='dags/', include_examples=False)

if dag_bag.import_errors:
    print(f"❌ CRITICAL: Found {len(dag_bag.import_errors)} DAG import errors!")
    for f, e in dag_bag.import_errors.items():
        print(f"File: {f}\nError: {e}\n---")
    sys.exit(1) # Fail the CI pipeline

print(f"✅ Success: Parsed {len(dag_bag.dags)} DAGs.")