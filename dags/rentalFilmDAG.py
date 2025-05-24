from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from airflow.providers.mysql.operators.mysql import MySqlOperator


conn = BaseHook.get_connection("mysql_conn")
password_encoded = quote_plus(str(conn.password))

engine = create_engine(
    f"mysql+pymysql://{conn.login}:{password_encoded}@{conn.host}:{conn.port}/{conn.schema}"
)


# ----------- Python Functions ---------------- #

import traceback

def extract_payment(**kwargs):
    try:
        print("🔄 Connecting to database for payment extraction...")
        query = """
        SELECT 
            p.payment_id,
            p.staff_id,
            p.rental_id,
            r.inventory_id,          
            DATE(p.payment_date) AS payment_date,
            p.amount,
            s.first_name,
            s.last_name,
            r.rental_date,
            r.customer_id,
            s.store_id
        FROM payment p
        JOIN staff s ON p.staff_id = s.staff_id
        JOIN rental r ON p.rental_id = r.rental_id
        """
        df = pd.read_sql(query, engine)
        print(f"✅ Retrieved {len(df)} payment records.")
        
        output_path = '/tmp/extracted_pay.csv'
        df.to_csv(output_path, index=False)
        print(f"📁 Payment data written to {output_path}")
        
    except Exception as e:
        print("❌ Error in extract_payment:")
        traceback.print_exc()
        raise AirflowFailException(f"extract_payment failed due to: {str(e)}")


def extract_inventory(**kwargs):
    try:
        print("🔄 Connecting to database for inventory extraction...")
        query = """
        SELECT 
            i.inventory_id,
            i.film_id,
            f.title,
            i.store_id,
            s.address_id,
            s.manager_staff_id,
            a.address,
            f.release_year,
            f.language_id,
            i.last_update
        FROM inventory i
        JOIN film f ON i.film_id = f.film_id
        JOIN store s ON i.store_id = s.store_id
        JOIN address a ON s.address_id = a.address_id
        """
        df = pd.read_sql(query, engine)
        df = df.drop_duplicates().dropna()
        print(f"Retrieved and cleaned {len(df)} inventory records.")
        
        output_path = '/tmp/extracted_inventory.csv'
        df.to_csv(output_path, index=False)
        print(f"Inventory data written to {output_path}")
        
    except Exception as e:
        print("Error in extract_inventory:", e)
        raise AirflowFailException(f"extract_inventory failed: {str(e)}")


def transform_pay(**kwargs):
    df = pd.read_csv('/tmp/extracted_pay.csv').dropna()
    df['payment_date'] = pd.to_datetime(df['payment_date'])
    df['year'] = df['payment_date'].dt.year
    df['month'] = df['payment_date'].dt.month
    df['day'] = df['payment_date'].dt.day
    df.to_csv('/tmp/transformed_pay.csv', index=False)

def transform_inv(**kwargs):
    df = pd.read_csv('/tmp/extracted_inventory.csv')
    df.drop_duplicates().dropna().to_csv('/tmp/transformed_inv.csv', index=False)

def load_dim_staff(**kwargs):
    df = pd.read_csv('/tmp/transformed_pay.csv')
    df[["staff_id", "first_name", "last_name", "store_id"]].drop_duplicates()\
        .to_sql('dim_staff', con=engine, if_exists='append', index=False)

def load_dim_date(**kwargs):
    df = pd.read_csv('/tmp/transformed_pay.csv')
    df = df.dropna(subset=['payment_date'])
    df['date_id'] = pd.to_datetime(df['payment_date']).dt.strftime('%Y%m%d').astype(int)
    dim_date = df[['date_id', 'payment_date', 'month', 'year']]\
        .drop_duplicates().rename(columns={'payment_date': 'full_date'})
    dim_date.to_sql('dim_date', con=engine, if_exists='append', index=False)

def load_dim_film(**kwargs):
    df = pd.read_csv('/tmp/transformed_inv.csv')
    df[['film_id', 'title', 'release_year', 'language_id']].drop_duplicates()\
        .to_sql('dim_film', con=engine, if_exists='append', index=False)

def load_dim_store(**kwargs):
    df = pd.read_csv('/tmp/transformed_inv.csv')
    df[['store_id', 'manager_staff_id', 'address_id']].drop_duplicates()\
        .to_sql('dim_store', con=engine, if_exists='append', index=False)

def load_dim_rental(**kwargs):
    df = pd.read_csv('/tmp/transformed_pay.csv')
    df[["rental_id", "rental_date", "inventory_id", "customer_id"]].drop_duplicates()\
        .to_sql('dim_rental', con=engine, if_exists='append', index=False)
    
def load_fact_inventory(**kwargs):
    df_inventory = pd.read_csv('/tmp/transformed_inv.csv')
    df_inventory['last_update'] = pd.to_datetime(df_inventory['last_update']).dt.date

    dim_date = pd.read_sql("SELECT date_id, full_date FROM dim_date", engine)
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date

    df_inventory = df_inventory.merge(dim_date, left_on='last_update', right_on='full_date', how='left')
    fact_inventory = df_inventory[['date_id', 'film_id', 'store_id']]
    fact_inventory.to_sql('fact_daily_inventory', con=engine, if_exists='append', index=False)


def load_fact_monthly_payment(**kwargs):
    df = pd.read_csv('/tmp/transformed_pay.csv')
    df['payment_date'] = pd.to_datetime(df['payment_date'])
    df['year'] = df['payment_date'].dt.year
    df['month'] = df['payment_date'].dt.month

    monthly_agg = df.groupby(['staff_id', 'rental_id', 'year', 'month'], as_index=False)['amount'].sum()
    monthly_agg.rename(columns={'amount': 'monthly_payment_total'}, inplace=True)

    dim_date = pd.read_sql("SELECT date_id, year, month FROM dim_date", engine)
    fact = pd.merge(monthly_agg, dim_date, on=['year', 'month'], how='left')
    fact = fact[['staff_id', 'rental_id', 'date_id', 'monthly_payment_total']]
    fact.to_sql('fact_monthly_payment', con=engine, if_exists='append', index=False)
    

# ----------- DAG Definition ---------------- #
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_dimensional_model',
    default_args=default_args,
    description='ETL to build dimensional model for rental film',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start_task = BashOperator(
        task_id='first_task',
        bash_command="echo hello!!!!!"
    )

    extract_payment_task = PythonOperator(
        task_id='extract_payment',
        python_callable=extract_payment,
    )

    extract_inventory_task = PythonOperator(
        task_id='extract_inventory',
        python_callable=extract_inventory,
    )

    transform_pay_task = PythonOperator(
        task_id='transform_pay',
        python_callable=transform_pay,
    )

    transform_inv_task = PythonOperator(
        task_id='transform_inventory',
        python_callable=transform_inv,
    )

    load_dim_staff_task = PythonOperator(
        task_id='load_dim_staff',
        python_callable=load_dim_staff,
    )

    load_dim_date_task = PythonOperator(
        task_id='load_dim_date',
        python_callable=load_dim_date,
    )

    load_dim_film_task = PythonOperator(
        task_id='load_dim_film',
        python_callable=load_dim_film,
    )

    load_dim_store_task = PythonOperator(
        task_id='load_dim_store',
        python_callable=load_dim_store,
    )

    load_dim_rental_task = PythonOperator(
        task_id='load_dim_rental',
        python_callable=load_dim_rental,
    )
    load_fact_inventory_task = PythonOperator(
        task_id='load_fact_inventory',
        python_callable=load_fact_inventory,
    )

    load_fact_monthly_payment_task = PythonOperator(
        task_id='load_fact_monthly_payment',
        python_callable=load_fact_monthly_payment,
    )


with DAG(
    dag_id='test_mysql_connection',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_query = MySqlOperator(
        task_id='run_test_query',
        mysql_conn_id='mysql_conn',  # Your connection ID here
        sql='SELECT 1;',  # Simple query to test connection
    )
    # ----------- Dependencies ---------------- #
    start_task >> [extract_payment_task, extract_inventory_task]

extract_payment_task >> transform_pay_task
extract_inventory_task >> transform_inv_task

transform_pay_task >> [load_dim_staff_task, load_dim_date_task, load_dim_rental_task]
transform_inv_task >> [load_dim_film_task, load_dim_store_task]

# Fact table dependencies
[load_dim_date_task, load_dim_film_task, load_dim_store_task] >> load_fact_inventory_task
[load_dim_date_task, load_dim_rental_task, load_dim_staff_task] >> load_fact_monthly_payment_task