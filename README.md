# FoodOrders ETL Project

This folder contains the original CSV, your Tasks 1–8 notebook, and the Task 9 ETL pipeline. The notebook is kept as provided. Task 9 scripts are at the project root to match your friend's project layout; a small launcher in `Task_9_Pipeline/` preserves the notebook's Task 9 command and report path.

## Project contents

- `food_orders.csv`: source dataset.
- `FoodOrders_Analysis.ipynb`: your Tasks 1–8 notebook, with each question and sub-question in its own section/cell.
- `config.py`, `extract.py`, `load_raw.py`, `retrieve_raw.py`, `transform.py`, `validate.py`, `load_transformed.py`, `elt_batch_pipeline.py`, and `db_utils.py`: Task 9 pipeline.
- `Task_9_Pipeline/elt_batch_pipeline.py`: compatibility launcher used by the notebook.
- `Task_9_Pipeline/reports/`: Task 9 execution, JSON validation, CSV validation, and rejected-record reports.
- `reports/`: notebook outputs, cleaned dataset, and charts.
- `sql_queries.sql`: Task 9 SQL questions.
- `requirements.txt`: Python dependencies.

The Task 9 configuration accepts either `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` (as in the reference project) or `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`. `MYSQL_*` takes precedence if both sets are present. No database password is stored in this project.

## Run Tasks 1–8

Extract the project folder, open Terminal, and change to the extracted `FoodOrders_ETL` folder. For example, if you extracted it into Documents:

```bash
cd ~/Documents/FoodOrders_ETL
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
jupyter notebook FoodOrders_Analysis.ipynb
```

In the Jupyter browser tab, choose **Kernel → Restart & Run All**. Wait for all cells to finish, then save the notebook. The CSV needs to remain beside the notebook.

## Run Task 9

Keep MySQL running. Check whether it is already running before starting it, so you do not launch a second server:

```bash
ps -axo pid,command | grep '[m]ysqld'
/usr/local/mysql/bin/mysqladmin --no-defaults --protocol=TCP -h 127.0.0.1 -P 3306 -u root -p ping
```

A successful `mysqladmin` check prints `mysqld is alive`. If the server is not running, start it using the MySQL installation's service manager or startup script; do not use `sudo` unless the MySQL installation specifically requires it.

In a second Terminal tab, run these commands from the project folder. They use the reference project's `DB_*` variable names and prompt for the current MySQL password without displaying it:

```bash
cd ~/Documents/FoodOrders_ETL
source .venv/bin/activate
unset MYSQL_HOST MYSQL_PORT MYSQL_USER MYSQL_PASSWORD
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=root
read -rs 'DB_PASSWORD?Enter your current MySQL root password: '
export DB_PASSWORD
printf '\n'
python3 Task_9_Pipeline/elt_batch_pipeline.py
```

Enter the password currently set for the MySQL `root` account. The `read` command only puts the password into the current Terminal environment; it does not change the MySQL account password. If MySQL prints `Access denied`, confirm the password using a direct MySQL login before rerunning the pipeline. Do not paste the password into chat or save it in the project.

The pipeline creates `food_orders_raw_db.raw_food_orders` and `food_orders_analytics_db.cleaned_food_orders`. It loads source rows to the raw database, retrieves them, transforms and validates the data, then loads accepted rows to analytics in batches of 100. The pipeline assigns a run ID; rerunning it creates a new run. Exact duplicate source rows are rejected and recorded with reasons.

## Reports and SQL

After Task 9, inspect:

```bash
python3 -m json.tool Task_9_Pipeline/reports/pipeline_execution_report.json
open Task_9_Pipeline/reports/validation_report.csv
```

The packaged historical successful Task 9 run is recorded as PASS: 1,010 raw rows loaded and retrieved, 1,000 transformed rows loaded to analytics, 10 exact duplicates rejected, and 21 successful batches with zero failures. A later run will replace the latest execution report. A failed MySQL login is a credential/configuration failure and does not invalidate the local CSV validation.

The SQL statements in `sql_queries.sql` use `:run_id` as a placeholder. Replace it with `pipeline_run_id` from the execution report before running the statements in MySQL Workbench.
