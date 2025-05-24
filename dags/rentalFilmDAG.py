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

def clean_dataframe(df, table_name=None):
    original_shape = df.shape
    df = df.dropna().drop_duplicates()
    cleaned_shape = df.shape
    if table_name:
        print(f"[{table_name}] Cleaned: {original_shape[0] - cleaned_shape[0]} rows removed")
    return df

def load_dim_staff(**kwargs):
    query = "SELECT staff_id, first_name, last_name, store_id FROM staff"
    df = pd.read_sql(query, engine)
    df = clean_dataframe(df, 'dim_staff')
    df.to_sql('dim_staff', engine, if_exists='append', index=False)


def load_dim_film(**kwargs):
    query = "SELECT film_id, title, release_year, language_id FROM film"
    df = pd.read_sql(query, engine)
    df = clean_dataframe(df, 'dim_film')
    df.to_sql('dim_film', engine, if_exists='append', index=False)


def load_dim_store(**kwargs):
    query = "SELECT store_id, manager_staff_id, address_id FROM store"
    df = pd.read_sql(query, engine)
    df = clean_dataframe(df, 'dim_store')
    df.to_sql('dim_store', engine, if_exists='append', index=False)


def load_dim_date(**kwargs):
    date_range = pd.date_range(start='2005-01-01', end='2006-12-31', freq='D')
    df = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d').astype(int),
        'full_date': date_range,
        'month': date_range.month,
        'year': date_range.year,
    })
    df = clean_dataframe(df, 'dim_date')
    df.to_sql('dim_date', engine, if_exists='append', index=False)


def load_dim_rental(**kwargs):
    query = "SELECT rental_id, rental_date, inventory_id, customer_id FROM rental"
    df = pd.read_sql(query, engine)
    df = clean_dataframe(df, 'dim_rental')
    df.to_sql('dim_rental', engine, if_exists='append', index=False)


def load_fact_daily_inventory(**kwargs):
    rental_df = pd.read_sql("SELECT rental_id, rental_date, inventory_id FROM rental", engine)
    inventory_df = pd.read_sql("SELECT inventory_id, film_id, store_id FROM inventory", engine)

    merged = rental_df.merge(inventory_df, on='inventory_id', how='inner')
    merged['date_id'] = pd.to_datetime(merged['rental_date']).dt.strftime('%Y%m%d').astype(int)

    grouped = merged.groupby(['date_id', 'film_id', 'store_id']).size().reset_index(name='inventory_count')
    grouped = clean_dataframe(grouped, 'fact_daily_inventory')
    grouped.to_sql('fact_daily_inventory', engine, if_exists='append', index=False)


def load_fact_monthly_payment(**kwargs):
    payments = pd.read_sql("SELECT staff_id, rental_id, payment_date, amount FROM payment", engine)
    payments['payment_date'] = pd.to_datetime(payments['payment_date'])
    payments['year'] = payments['payment_date'].dt.year
    payments['month'] = payments['payment_date'].dt.month
    payments['date_id'] = (payments['year'] * 10000 + payments['month'] * 100 + 1).astype(int)

    grouped = payments.groupby(['staff_id', 'rental_id', 'date_id'])['amount'].sum().reset_index()
    grouped.rename(columns={'amount': 'monthly_payment_total'}, inplace=True)
    grouped = clean_dataframe(grouped, 'fact_monthly_payment')
    grouped.to_sql('fact_monthly_payment', engine, if_exists='append', index=False)
# ----------- DAG Definition ---------------- #
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Updated DAG structure remains the same
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
        python_callable=load_fact_daily_inventory,
    )

    load_fact_monthly_payment_task = PythonOperator(
        task_id='load_fact_monthly_payment',
        python_callable=load_fact_monthly_payment,
    )

    # Task dependencies
    
 
    # Load dimensions first
   
    
    # Load facts after dimensions are loaded
    [load_dim_staff_task, load_dim_date_task, load_dim_rental_task] >> load_fact_monthly_payment_task
    [load_dim_film_task, load_dim_store_task, load_dim_date_task] >> load_fact_inventory_task






