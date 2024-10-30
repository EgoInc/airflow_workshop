import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def get_wp_detail():
    data_new = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select 
    (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as id,
    (json_array_elements(wp_in_academic_plan::json)->>'discipline_code')::varchar(20) as discipline_code,
    (json_array_elements(wp_in_academic_plan::json)->>'title')::text as title,
    (json_array_elements(wp_in_academic_plan::json)->>'description')::text as description,
    (json_array_elements(wp_in_academic_plan::json)->>'status')::varchar(20) as status,
    update_ts
    from stg.work_programs 
    """
    )

    target_fields = ["id", "discipline_code", "title", "description", "status", "update_ts"]

    df_new = pd.DataFrame(data_new, columns=target_fields)
    df_new_data = df_new.sort_values(by=["id", "update_ts"], ascending=[True, False])
    df_new_data_last = df_new_data.drop_duplicates(subset="id", keep="first")

    data_hist = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
        """
    select id, discipline_code, title, description, status, update_ts
    from stg.wp_detail
    """
    )

    df_hist = pd.DataFrame(data_hist, columns=target_fields)
    df_hist_data = df_hist.sort_values(by=["id", "update_ts"], ascending=[True, False])
    df_hist_data_last = df_hist_data.drop_duplicates(subset="id", keep="first").drop("update_ts", axis=1)

    merged = df_hist_data_last.merge(df_new_data_last, how="outer", indicator=True)
    df = merged[merged["_merge"] == "right_only"].drop(columns=["_merge"])
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").insert_rows(
        "stg.wp_detail", df.values, target_fields=target_fields
    )


with DAG(
    dag_id="get_wp_detail_withoutDB",
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_wp_detail_withoutDB", python_callable=get_wp_detail)

t1
