# MASTER_LOAD.py
import subprocess

load_order = [
    "COUNTRY_LOAD.py",
    "REGION_LOAD.py",
    "STATE_LOAD.py",
    "CITY_LOAD.py",
    "CATEGORY_LOAD.py",
    "SUBCATEGORY_LOAD.py",
    "PRODUCT_LOAD.py",
    "SEGMENT_LOAD.py",
    "CUSTOMER_LOAD.py",
    # "DATE_LOAD.py",
    "SHIP_MODE_LOAD.py",
    "FACT_SALES_LOAD.py",   # always last
]

for script in load_order:
    print(f"Running {script}...")
    subprocess.run(["python", script], check=True)
    print(f"{script} completed.")