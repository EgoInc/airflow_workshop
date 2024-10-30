-- Таблица для хранения информации о учебных планах
CREATE TABLE cdm.up_programs (
    app_isu_id INTEGER,             -- ОП
    on_check VARCHAR(20),           -- Статус проверки
    laboriousness INTEGER,          -- Трудоемкость
    year INTEGER,                   -- Год
    qualification TEXT              -- Уровень образования (бакалавр, специалист, магистр)
);


-- Таблица для хранения информации о дисциплинах
CREATE TABLE cdm.wp_details (
    wp_id INTEGER UNIQUE, -- ID дисциплины
    discipline_code TEXT, -- Код дисциплины
    wp_title TEXT, -- Название дисциплины
    unit_id INTEGER, -- ID структурного подразделения, связанного с дисциплиной
    unit_title TEXT, -- Название структурного подразделения
    wp_status TEXT -- Статус дисциплины (например, "одобрено" или другое)
);

-- Таблица для хранения информации о редакторах
CREATE TABLE cdm.wp_editors (
    wp_id INTEGER, -- ID дисциплины
    editor_id INTEGER, -- ID редактора
    editor_name TEXT, -- Полное имя редактора
    UNIQUE (wp_id, editor_id) -- Уникальная комбинация дисциплины и редактора
);