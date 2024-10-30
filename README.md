* Создайте виртуальное окружение и активируйте его:
```shell script
python -m venv ./venv
```

* Активируйте его:
```shell script
 .\venv\Scripts\activate.bat
```

* Обновите pip до последней версии:
```shell script
pip install --upgrade pip
```
* Установите зависимости:
```shell script
pip install -r requirements.txt
```

Для выполнения заданий выполните:

`docker compose up -d`

Если у Вас не установлен python 3.8 то самое время сделать это. 

- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
	- в Airflow порт 5432
- Metabase
    - `http://localhost:3333/` 

Подключение к БД:
- docker exec -it  hw_2-de-pg-cr-af-1 bash
- psql -U jovyan -d de -h localhost


## Начало работы

Подключитесь в dockere к Postrges и создайте модель для stg-слоя (пример скрипта sql/stg_ddl.sql)

Через Airflow UI (AirflowAdmin : airflow_pass) создайте подключение к БД (admin - connections), назовите его PG_WAREHOUSE_CONNECTION (как в скрипте dags/get_data.py).

Запустите dag get_data и убедитесь в том, что dag завершен успешно.

![image-20241020204829623](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241020204829623.png)

*Чтобы запустить успешно потребовалось добавить обработчик ошибок в get_wp_descriptions иначе там все ломалось при пустых ответах, проверка того что все успешно загрузилось ниже*

![image-20241020205153586](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241020205153586.png)

## Задание 2 оп курсу "Прикладные пакеты для анализа данных" 

Задание делается в группах по 3-5 человек

1) Построить пайплайн средствами Airflow для подготовки данных для дэшбордов. Данные для дэшборда должны находиться в слое cdm и формироваться только по данным из слоя dds.

Требования к дэшборду:
- доля учебных планов в статусе "одобрено" (только для 2023 и 2024 года),
- доля/количество учебных планов с некорректной трудоемкостью (только для 2023 и 2024 года, корректная трудоемкость у бакалавров - 240, а магистров - 120),
- доля заполненных аннотаций в динамике,
- долю дисциплин в статусе "одобрено" в динамике,

Данные необходимо отображать в разрезах:
- - ОП (с учетом года), 
- - структурных подразделений для дисциплин, 
- - уровней образования (бакалавр, специалист, магистр),
- - структурных подразделений для ОП, 
- - редактор (для РПД),
- - учебного года (текущий и следующий).

2) Построить дэшборд средствами Metabase (есть в сборке). Возможна выгрузка таблиц из слоя cdm и дальнейшее построение дэшборда другими средствами


Здесь описан один из возможных вариантов выполнения: 
- доработайте модель на слое stg с учетом эндпойнтов для получения информации о структурных подразделениях и статусах, а также с учетом json'ов c учебными планами;
- доработайте модель на stg с учетом SCD2 (https://en.wikipedia.org/wiki/Slowly_changing_dimension);
- доработайте скрипт из dag'а get_data для сбора данных по всем эндпойнтам;
- доработайте модель данных для слоя dds, учивающую все необходимые поля для построения дэшборда;
- создайте скрипт для переноса данных из stg на dds;
- спроектируйте модель для cdm (слой витрин);
- подключите Metabase к модели на слое cdm (при первом подключении нужно зарегистрироваться, хост:порт de-pg-cr-af:5432);
- сформируйте дэшборд в соответствии с требованиями.


## Эндпойнты
- Структные подразделения: https://op.itmo.ru/api/record/structural/workprogram (см. dags/get_data.py)

![image-20241020205335176](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241020205335176.png)

- Рабочие программы и статусы: https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all (см. dag/get_data.py)

  ![image-20241020205507388](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241020205507388.png)

- Рабочие программы ипо годам https://op.itmo.ru/api/record/academicplan/get_wp_by_year/ - (см. dag/get_disc_by_year.py)

  *Бралось из папки data, однако содержало рабочие программы только до 2022 года*

- Учебные планы из конструктора ОП: https://op.itmo.ru/api/academicplan/detail/ - (см. dag/get_up_detail.py)

  *C браузера выдавало 500тку, данные отсутствовали для многих ID*

# Ход работы

## Выбираем SCD2 - подход

![image-20241029105524849](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241029105524849.png)

```python
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
```

В таблицы добавляется:
```sql
...
valid_from TIMESTAMP,
is_current BOOLEAN DEFAULT TRUE
...
```

А также меняем слой миграции на DDS, добавляя условие:
```sql
WHERE sw.is_current = TRUE 
```

## Проектирование CDM

Схема:

![image-20241029114706745](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241029114706745.png)

Схема миграции с DDS => CDM

![image-20241029114853293](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241029114853293.png)

Итоговый дашборд:

![image-20241029122745687](C:\Users\Vasya\AppData\Roaming\Typora\typora-user-images\image-20241029122745687.png)

### Учебные планы verified

```sql
SELECT
    up.year,
    up.qualification AS education_level,
    up.app_isu_id AS edu_program_id,
    wd.unit_title AS discipline_unit,
    wp.unit_title AS edu_program_unit,
    CASE WHEN up.on_check = 'verified' THEN 1 ELSE 0 END AS approved, -- Флаг одобрения
    STRING_AGG(we.editor_name, ', ') AS editor_names -- Объединяем имена редакторов
FROM 
    cdm.up_programs AS up
LEFT JOIN 
    (SELECT DISTINCT wp_id, unit_id, unit_title FROM cdm.wp_details) AS wd ON wd.wp_id = up.app_isu_id -- связываем дисциплину с ОП
LEFT JOIN 
    (SELECT DISTINCT unit_id, unit_title FROM cdm.wp_details) AS wp ON wp.unit_id = wd.unit_id -- связываем ОП с дисциплиной
LEFT JOIN 
    cdm.wp_editors AS we ON we.wp_id = wd.wp_id
WHERE  up.app_isu_id IS NOT NULL
GROUP BY 
    up.year, up.qualification, up.app_isu_id, wd.unit_title, wp.unit_title, up.on_check
ORDER BY 
    up.year;
```

### Учебные планы с некорректной трудоемкостью

```sql
SELECT
    up.year,
    up.qualification AS education_level,
    up.app_isu_id AS edu_program_id,
    up.laboriousness,
    CASE
        WHEN (up.qualification = 'bachelor' AND up.laboriousness = 240) OR 
             (up.qualification = 'master' AND up.laboriousness = 120) THEN 0
        ELSE 1
    END AS incorrect_laboriousness, -- Флаг для некорректной трудоемкости
    STRING_AGG(we.editor_name, ', ') AS editor_names -- Объединяем имена редакторов
FROM 
    cdm.up_programs AS up
LEFT JOIN 
    cdm.wp_editors AS we ON we.wp_id = up.app_isu_id -- Подключаем редакторов к учебным программам
WHERE 
    up.app_isu_id IS NOT NULL
GROUP BY 
    up.year, up.qualification, up.app_isu_id, up.laboriousness
ORDER BY 
    up.year;
```

