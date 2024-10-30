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


def get_wp_descriptions():
    target_fields = ['id', 'academic_plan_in_field_of_study', 'wp_in_academic_plan', 'update_ts', 'valid_from', 'is_current']
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    
    for p in range(1, c // 10 + 2):
        url_down = f'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page={p}'
        dt = pendulum.now().to_iso8601_string()

        try:
            page = requests.get(url_down, headers=headers)
            res = json.loads(page.text)['results']
            for r in res:
                df = pd.DataFrame([r], columns=r.keys())
                df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
                df['wp_in_academic_plan'] = df[~df['wp_in_academic_plan'].isna()]["wp_in_academic_plan"].apply(lambda st_dict: json.dumps(st_dict))
                df.loc[:, 'update_ts'] = dt
                df.loc[:, 'valid_from'] = dt
                df.loc[:, 'is_current'] = True
                
                # Проверка наличия текущей версии записи
                pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
                query = f"""
                SELECT * FROM stg.work_programs 
                WHERE id = {r['id']} AND is_current = TRUE;
                """
                current_record = pg_hook.get_records(query)
                
                if current_record:
                    # Сравниваем данные, чтобы понять, нужно ли обновление
                    if current_record[0][1] != df['academic_plan_in_field_of_study'].values[0] or current_record[0][2] != df['wp_in_academic_plan'].values[0]:
                        # Делаем старую запись неактуальной
                        update_query = f"""
                        UPDATE stg.work_programs
                        SET is_current = FALSE
                        WHERE id = {r['id']} AND is_current = TRUE;
                        """
                        pg_hook.run(update_query)
                        # Вставка новой записи
                        pg_hook.insert_rows('stg.work_programs', df.values.tolist(), target_fields=target_fields)
                else:
                    # Вставка новой записи, если такой записи ещё нет
                    pg_hook.insert_rows('stg.work_programs', df.values.tolist(), target_fields=target_fields)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке страницы {p}: {e}")
            continue
        
def get_practice():
    target_fields = ['id', 'title', 'description', 'valid_from', 'is_current']
    url_down = 'https://op.itmo.ru/api/practice/?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    
    for p in range(1, c // 10 + 2):
        url_down = f'https://op.itmo.ru/api/practice/?format=json&page={p}'
        dt = pendulum.now().to_iso8601_string()

        try:
            page = requests.get(url_down, headers=headers)
            res = json.loads(page.text)['results']
            for r in res:
                df = pd.DataFrame([r], columns=r.keys())
                df['valid_from'] = dt
                df['is_current'] = True
                
                pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
                query = f"""
                SELECT * FROM stg.practice 
                WHERE id = {r['id']} AND is_current = TRUE;
                """
                current_record = pg_hook.get_records(query)
                
                if current_record:
                    if current_record[0][1] != df['title'].values[0] or current_record[0][2] != df['description'].values[0]:
                        update_query = f"""
                        UPDATE stg.practice
                        SET is_current = FALSE
                        WHERE id = {r['id']} AND is_current = TRUE;
                        """
                        pg_hook.run(update_query)
                        pg_hook.insert_rows('stg.practice', df.values.tolist(), target_fields=target_fields)
                else:
                    pg_hook.insert_rows('stg.practice', df.values.tolist(), target_fields=target_fields)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке страницы {p}: {e}")
            continue

def get_structural_units():
    target_fields = ['fak_id', 'fak_title', 'work_programs', 'valid_from', 'is_current']
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    page = requests.get(url_down, headers=headers)
    res = list(json.loads(page.text))
    dt = pendulum.now().to_iso8601_string()

    for su in res:
        df = pd.DataFrame.from_dict(su)
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        df['valid_from'] = dt
        df['is_current'] = True
        
        pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        query = f"""
        SELECT * FROM stg.su_wp 
        WHERE fak_id = {su['fak_id']} AND is_current = TRUE;
        """
        current_record = pg_hook.get_records(query)
        
        if current_record:
            if current_record[0][1] != df['fak_title'].values[0] or current_record[0][2] != df['work_programs'].values[0]:
                update_query = f"""
                UPDATE stg.su_wp
                SET is_current = FALSE
                WHERE fak_id = {su['fak_id']} AND is_current = TRUE;
                """
                pg_hook.run(update_query)
                pg_hook.insert_rows('stg.su_wp', df.values.tolist(), target_fields=target_fields)
        else:
            pg_hook.insert_rows('stg.su_wp', df.values.tolist(), target_fields=target_fields)


def get_online_courses():
    target_fields = ['id', 'title', 'institution', 'topic_with_online_course', 'valid_from', 'is_current']
    url_down = 'https://op.itmo.ru/api/course/onlinecourse/?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    
    for p in range(1, c // 10 + 2):
        url_down = f'https://op.itmo.ru/api/course/onlinecourse/?format=json&page={p}'
        dt = pendulum.now().to_iso8601_string()

        try:
            page = requests.get(url_down, headers=headers)
            res = json.loads(page.text)['results']
            for r in res:
                df = pd.DataFrame([r], columns=r.keys())
                df['institution'] = df[~df['institution'].isna()]["institution"].apply(lambda st_dict: json.dumps(st_dict))
                df['topic_with_online_course'] = df[~df['topic_with_online_course'].isna()]["topic_with_online_course"].apply(lambda st_dict: json.dumps(st_dict))
                df['valid_from'] = dt
                df['is_current'] = True
                
                pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
                query = f"""
                SELECT * FROM stg.online_courses 
                WHERE id = {r['id']} AND is_current = TRUE;
                """
                current_record = pg_hook.get_records(query)
                
                if current_record:
                    if current_record[0][1] != df['title'].values[0] or current_record[0][2] != df['institution'].values[0] or current_record[0][3] != df['topic_with_online_course'].values[0]:
                        update_query = f"""
                        UPDATE stg.online_courses
                        SET is_current = FALSE
                        WHERE id = {r['id']} AND is_current = TRUE;
                        """
                        pg_hook.run(update_query)
                        pg_hook.insert_rows('stg.online_courses', df.values.tolist(), target_fields=target_fields)
                else:
                    pg_hook.insert_rows('stg.online_courses', df.values.tolist(), target_fields=target_fields)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка при обработке страницы {p}: {e}")
            continue

with DAG(dag_id='get_data', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_practice',
    python_callable=get_practice
    )
    t2 = PythonOperator(
    task_id='get_wp_descriptions',
    python_callable=get_wp_descriptions
    )
    t3 = PythonOperator(
    task_id='get_structural_units',
    python_callable=get_structural_units
    )
    t4 = PythonOperator(
    task_id='get_online_courses',
    python_callable=get_online_courses
    )

t1 >> t2 >> t3 >> t4