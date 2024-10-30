import os
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

# Путь к папке с файлами
data_path = '../data/'

def get_up(up_id):
    up_id = str(up_id)
    
    # Формируем возможные имена файлов
    file_name_old = f"{up_id}_old.json"
    file_name_new = f"{up_id}.json"
    
    # Проверяем, существует ли файл с таким id
    if os.path.exists(os.path.join(data_path, file_name_old)):
        file_path = os.path.join(data_path, file_name_old)
    elif os.path.exists(os.path.join(data_path, file_name_new)):
        file_path = os.path.join(data_path, file_name_new)
    else:
        print(f"File for id {up_id} not found, skipping.")
        return

    # Чтение файла и обработка данных
    with open(file_path, 'r', encoding='utf-8') as file:
        file_data = json.load(file)
        df = pd.DataFrame(file_data['result'])
        
        # Преобразуем 'disciplines_blocks' в строку JSON, если это необходимо
        if 'disciplines_blocks' in df.columns:
            df['disciplines_blocks'] = df[~df['disciplines_blocks'].isna()]["disciplines_blocks"].apply(lambda st_dict: json.dumps(st_dict))
        
        # Если данные есть, вставляем в таблицу
        if len(df) > 0:
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows(
                'stg.up_isu', 
                df.values, 
                target_fields=df.columns.tolist()
            )

def get_up_isu():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    with t as (
    select (json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id')::integer as ap_isu_id 
    from stg.work_programs wp 
    order by 1)
    select ap_isu_id from t
    where ap_isu_id = 18339
    """
    )
    
    # Пакетная обработка id
    start = 0
    finish = start + 100
    while start < len(ids):
        if finish > len(ids):
            finish = len(ids)
        
        for up_id in ids[start:finish]:
            up_id = str(up_id[0])
            print(f"Processing id: {up_id}")
            get_up(up_id)

        start += 100
        finish = start + 100

with DAG(dag_id='get_up_isu_from_files', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 5 * * 0', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='get_up_isu',
        python_callable=get_up_isu
    )

t1
