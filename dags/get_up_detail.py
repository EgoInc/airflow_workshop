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

def get_up_detail():
    target_fields = ['id', 'ap_isu_id', 'on_check', 'laboriousness', 'academic_plan_in_field_of_study', 'valid_from', 'is_current']
    
    # Получаем все идентификаторы программ для обновления
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
        """
        select id as op_id
        from stg.work_programs wp
        where id > 7290
        order by 1
        """
    )
    
    url_down = 'https://op.itmo.ru/api/academicplan/detail/'
    dt = pendulum.now().to_iso8601_string()

    for op_id in ids:
        op_id = str(op_id[0])
        print(op_id)
        url = url_down + op_id + '?format=json'
        
        try:
            page = requests.get(url, headers=headers)
            df = pd.DataFrame.from_dict(page.json(), orient='index').T
            df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
            df['valid_from'] = dt
            df['is_current'] = True
            
            pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
            
            # Проверка текущей версии записи
            query = f"""
            SELECT * FROM stg.up_detail
            WHERE id = {op_id} AND is_current = TRUE;
            """
            current_record = pg_hook.get_records(query)
            
            if current_record:
                # Проверка, изменились ли данные
                if (
                    current_record[0][1] != df['ap_isu_id'].values[0] or
                    current_record[0][2] != df['on_check'].values[0] or
                    current_record[0][3] != df['laboriousness'].values[0] or
                    current_record[0][4] != df['academic_plan_in_field_of_study'].values[0]
                ):
                    # Обновление текущей записи, делая её неактуальной
                    update_query = f"""
                    UPDATE stg.up_detail
                    SET is_current = FALSE
                    WHERE id = {op_id} AND is_current = TRUE;
                    """
                    pg_hook.run(update_query)
                    
                    # Вставка новой версии записи
                    pg_hook.insert_rows('stg.up_detail', df.values.tolist(), target_fields=target_fields)
            else:
                # Вставка новой записи, если такой записи ещё нет
                pg_hook.insert_rows('stg.up_detail', df.values.tolist(), target_fields=target_fields)
        
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке ID {op_id}: {e}")
            continue


with DAG(dag_id='get_up_detail', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_up_detail',
    python_callable=get_up_detail
    )

t1
