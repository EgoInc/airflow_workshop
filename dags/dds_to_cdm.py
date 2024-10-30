import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

# Функция для вставки данных в таблицу CDM.up_programs
def up_programs():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate cdm.up_programs restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.up_programs (app_isu_id, on_check, laboriousness, year, qualification)
    SELECT 
        app_isu_id,
        on_check,
        laboriousness,
        year,
        qualification
    FROM dds.up
    WHERE NOT EXISTS (
        SELECT 1 
        FROM cdm.up_programs 
        WHERE cdm.up_programs.app_isu_id = dds.up.app_isu_id
        AND cdm.up_programs.year = dds.up.year
    );
    """
    )


# Функция для вставки данных в таблицу CDM.wp_details
def wp_details():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate cdm.wp_details restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.wp_details (wp_id, discipline_code, wp_title, wp_status, unit_id, unit_title)
    SELECT 
        wp.wp_id,
        wp.discipline_code::text,
        wp.wp_title,
        states.state_name AS wp_status,
        wp.unit_id,
        units.unit_title
    FROM dds.wp AS wp
    LEFT JOIN dds.units ON wp.unit_id = units.id
    LEFT JOIN dds.states ON wp.wp_status = states.id
    ON CONFLICT (wp_id) DO NOTHING;
    """
    )

# Функция для вставки данных в таблицу CDM.wp_editors
def wp_editors():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate cdm.wp_editors restart identity cascade;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.wp_editors (wp_id, editor_id, editor_name)
    SELECT 
        wp_editor.wp_id,
        wp_editor.editor_id,
        CONCAT(editors.first_name, ' ', editors.last_name) AS editor_name
    FROM dds.wp_editor AS wp_editor
    LEFT JOIN dds.editors ON wp_editor.editor_id = editors.id
    ON CONFLICT (wp_id, editor_id) DO NOTHING;
    """
    )


# Определение DAG
with DAG(dag_id='dds_to_cdm', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='@daily', catchup=False) as dag:
    
    # Задачи для переноса данных из DDS в CDM
    t1 = PythonOperator(
        task_id='up_programs',
        python_callable=up_programs
    )

    t2 = PythonOperator(
        task_id='wp_details',
        python_callable=wp_details
    )

    t3 = PythonOperator(
        task_id='wp_editors',
        python_callable=wp_editors
    )

    # Определение порядка выполнения задач
    t1 >> t2 >> t3
