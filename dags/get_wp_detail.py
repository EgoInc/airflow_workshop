import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get ("username")
password = Variable.get ("password")
auth_data = {"username": username, "password": password}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

def get_wp_detail():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.wp_detail  restart identity cascade;
    """)
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select id from stg.work_programs order by 1
    """)

    print(f"IDS -> {ids}")

    url_down = 'https://op.itmo.ru/api/workprogram/detail/'
    target_fields = ['id', 'discipline_code', 'title', 'description', 'structural_unit', 'prerequisites', 'discipline_sections', 'bibliographic_reference', 'outcomes', 'certification_evaluation_tools', 'expertise_status']
    for wp_id in ids:
        print(f"wp_id -> {wp_id}")
        wp_id = str(wp_id[0])
        print (wp_id)
        url = url_down + wp_id + '?format=json'
        page = requests.get(url, headers=headers)

        # skip if page is error status or have no data or { "detail": "Not found." }
        if page.status_code != 200 or not page.json() or ("detail" in page.json() and page.json()["detail"] == "Not found."):
            print(f"Error with {wp_id} -> {page.status_code}")
            continue

        # df = pd.DataFrame.from_dict(page.json(), orient='index')
        # df = df.T
        # df['certification_evaluation_tools'] = df[~df['certification_evaluation_tools'].isna()]["certification_evaluation_tools"].apply(lambda st_dict: json.dumps(st_dict))
        # df['discipline_sections'] = df[~df['discipline_sections'].isna()]["discipline_sections"].apply(lambda st_dict: json.dumps(st_dict))
        # df['outcomes'] = df[~df['outcomes'].isna()]["outcomes"].apply(lambda st_dict: json.dumps(st_dict))
        # df['bibliographic_reference'] = df[~df['bibliographic_reference'].isna()]["bibliographic_reference"].apply(lambda st_dict: json.dumps(st_dict))
        # df['prerequisites'] = df[~df['prerequisites'].isna()]["prerequisites"].apply(lambda st_dict: json.dumps(st_dict))
        # df['structural_unit'] = df[~df['structural_unit'].isna()]["structural_unit"].apply(lambda st_dict: json.dumps(st_dict))
        # df = df[target_fields]
        # PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_detail', df.values, target_fields = target_fields)

        # Преобразуем данные в DataFrame
        df = pd.DataFrame.from_dict(page.json(), orient='index').T
        df['certification_evaluation_tools'] = df[~df['certification_evaluation_tools'].isna()]["certification_evaluation_tools"].apply(lambda st_dict: json.dumps(st_dict))
        df['discipline_sections'] = df[~df['discipline_sections'].isna()]["discipline_sections"].apply(lambda st_dict: json.dumps(st_dict))
        df['outcomes'] = df[~df['outcomes'].isna()]["outcomes"].apply(lambda st_dict: json.dumps(st_dict))
        df['bibliographic_reference'] = df[~df['bibliographic_reference'].isna()]["bibliographic_reference"].apply(lambda st_dict: json.dumps(st_dict))
        df['prerequisites'] = df[~df['prerequisites'].isna()]["prerequisites"].apply(lambda st_dict: json.dumps(st_dict))
        df['structural_unit'] = df[~df['structural_unit'].isna()]["structural_unit"].apply(lambda st_dict: json.dumps(st_dict))
        df = df[target_fields]


        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_detail', df.values, target_fields=target_fields)
        # # Обрабатываем каждую строку
        # for row in df.itertuples(index=False):
        #     query = """
        #     WITH updated AS (
        #         UPDATE stg.wp_detail
        #         SET effective_to = current_timestamp,
        #             is_current = FALSE
        #         WHERE id = %s AND is_current = TRUE
        #         RETURNING *
        #     )
        #     INSERT INTO stg.wp_detail (id, discipline_code, title, description, structural_unit, prerequisites, discipline_sections, bibliographic_reference, outcomes, certification_evaluation_tools, expertise_status, effective_from, is_current)
        #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp, TRUE)
        #     ON CONFLICT (id, effective_from) DO NOTHING;
        #     """
        #     PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(query, parameters=(
        #         row.id,  # Для обновления старой записи
        #         row.id, row.discipline_code, row.title, row.description, row.structural_unit, row.prerequisites, row.discipline_sections, row.bibliographic_reference, row.outcomes, row.certification_evaluation_tools, row.expertise_status  # Для вставки новой записи
        #     ))


with DAG(dag_id='get_wp_detail', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_wp_detail',
    python_callable=get_wp_detail
    )

t1
