''' Модуль для создания и заполнения данными таблиц БД '''

'''Получить данные о работодателях и их вакансиях с сайта hh.ru. 
   Для этого используйте публичный API hh.ru и библиотеку requests.
  
  Выбрать не менее 10 интересных вам компаний, от которых вы будете получать данные о вакансиях по API.
Спроектировать таблицы в БД PostgreSQL для хранения полученных данных о работодателях
 и их вакансиях.  Для работы с БД используйте библиотеку psycopg2.
Реализовать код, который заполняет созданные в БД PostgreSQL таблицы данными о работодателях
 и их вакансиях.
Создать класс DBManager для работы с данными в БД.'''
import func
from configparser import ConfigParser
import psycopg2
import requests

companies_count=0
company_vac_count=0

def config(filename="database.ini", section="postgresql"):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db


def update_exchange_rate(database_name: str, params: dict):
   """ Обновление курса валют по данным hh.
   Отношение к рублю скачивается в локальную базу vacancies, в таблицу exchange_rate """

   func.parsing_delay(29)
   func.get_exchange_rate()
   func.parsing_delay(29)

   conn = psycopg2.connect(dbname=database_name, **params)
   conn.autocommit = True
   with conn.cursor() as cur:
      cur.execute("CREATE TABLE if not exists exchange_rate(cur_code varchar PRIMARY KEY, cur_rate float)")
      cur.execute("alter table vacancies drop constraint if exists vacancies_sal_currency_fkey;"
                  "TRUNCATE exchange_rate;"
                  #"ALTER TABLE exchange_rate DISABLE TRIGGER ALL;"
                  )
      cur.executemany("INSERT INTO exchange_rate VALUES (%s,%s)",
                    list(func.exchange_rate.items()))  # курсы валют с HH
      cur.execute("""alter table vacancies
                  ADD CONSTRAINT vacancies_sal_currency_fkey FOREIGN KEY(sal_currency) REFERENCES exchange_rate(cur_code);""")
   conn.commit

def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных о ВАКАНСИЯХ."""


    conn = psycopg2.connect(dbname='postgres', **params)  # **params ?
    conn.autocommit = True
    cur = conn.cursor()

    try:
      cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
      #psycopg2.errors.ObjectInUse: ОШИБКА:  база данных "vacancies" занята другими пользователями
      #DETAIL:  Эта база данных используется ещё в 1 сеансе.

      cur.execute(f"CREATE DATABASE {database_name}")
      conn.close()

      conn = psycopg2.connect(dbname=database_name, **params)
      with conn.cursor() as cur:
        # создаем таблицу курса валют, обновляемую с Hh
        cur.execute("CREATE TABLE if not exists exchange_rate(cur_code varchar PRIMARY KEY, cur_rate float)")
        # создаем таблицу работодателей и вакансий
        cur.execute("""CREATE TABLE if not exists employers (
                employer_id SERIAL PRIMARY KEY,
                employer_outid bigint NOT NULL,
                empl_name varchar,
                empl_url varchar);

              CREATE TABLE if not exists vacancies (
                  vac_id SERIAL PRIMARY KEY,
                  vac_outid bigint not null,
                  vac_name VARCHAR(255) NOT NULL,
                  employer_id bigINT REFERENCES employers(employer_id),
                  vac_url VARCHAR(255) UNIQUE,
                  sal_from float,
                  sal_to float,
                  sal_currency varCHAR DEFAULT 'RUR' REFERENCES exchange_rate(cur_code),
                  sal_gross boolean,
                  income float,
                  area_name VARCHAR(200),
                  address  VARCHAR(255),
                  vac_requirements varchar);
                  
              TRUNCATE employers CASCADE ;    
          """)  #использую каскадное очищение таблиц, если вдруг база была занята для удаления
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        print("Закрою базу в сеансах Postgresql. Имеющиеся в ней объекты пока не обновлены !")
        #cur.execute("select * from pg_stat_activity where datname = 'template1';")
        #Принудительно завершим запросом открытые сеансы:
        cur.execute("select pg_terminate_backend(pid) from pg_stat_activity where datname = 'template1';")
        conn = psycopg2.connect(dbname='vacancies', **params)  # **params ?
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE employers СASCADE")


    conn.commit()

    income_updating(database_name, params)


def filling_table(from_url:str, api_params:dict, from_json_part:str, database_name: str, db_params:dict,
                  to_table:str, to_fields:list[str], from_fields:list,additional_values:list, objects_count,object_name, page=None):
    api_params['page']=page
    func.parsing_delay(29) #func.time_check
    response = requests.get(from_url, api_params)
    if "капч" in response.json():
        print("Притормаживаемся...")
        func.parsing_delay(29)  #datetime.now()
        filling_table(from_url, api_params, from_json_part,
                      database_name, db_params, to_table,
                      to_fields,
                      from_fields, additional_values,
                      objects_count, object_name, p)  # рекурсивный вызов функции для перебора всех страниц


    if from_json_part in response.json():
       conn = psycopg2.connect(dbname=database_name, **db_params)

       with conn.cursor() as cur:
           for data_row in response.json()[from_json_part]:
               get_fields=[func.get_value(data_row, x) for x in from_fields]
               get_fields.extend(additional_values)

               cur.execute(
                   f"""
                   INSERT INTO {to_table} ({','.join(to_fields)})
                   VALUES ({('%s,'*(len(from_fields)+len(additional_values)))[:-1]});
                   COMMIT""",
                   get_fields )
               objects_count+=1

       #conn.commit()
       conn.close()
       if page is None:
       #  первый не рекурсивный вызов, вернувший по API число страниц, позволяет просмотреть рекурсивно остальные страницы со второй в цикле

           for p in range(1, response.json()["pages"]):
               filling_table(from_url, api_params, from_json_part,
                             database_name, db_params, to_table,
                             to_fields,
                             from_fields,additional_values,
                             objects_count, object_name, p)  # рекурсивный вызов функции для перебора всех страниц

       print(f"C Hh скачано с учётом ограничений: {objects_count} {object_name} из {response.json()['pages']} страниц.")
    else:
       print(f"Нет такого раздела '{from_json_part}' в json-ответе сервера")

def filling_employers(empl_list: list[str], database_name: str, params: dict):  #data: list[dict[str, any]]
    """ Парсинг работодателей по заданному предпочтительному списку примерных названий """
    companies_count = 0

    filling_table('https://api.hh.ru/employers/', {'text':' or '.join(empl_list),
                            'only_with_vacancies':True,
                            'per_page': 100}, 'items', 'vacancies', params,
                  'employers',
                  ('employer_outid', 'empl_name', 'empl_url'),  #  в какие поля  таблицы
                  ['id','name', 'alternate_url'],[],            # из каких полей json
                  companies_count,'работодателей')

def filling_vacancies(database_name: str, params: dict):  #data: list[dict[str, any]]
    """ Парсинг вакансий работодателей, УЖЕ ИМЕЮЩИХСЯ в базе данных PGSQL """
    #https://api.hh.ru/vacancies?employer_id=2188344
    company_vac_count = 0


    conn = psycopg2.connect(dbname=database_name, **params)
    #print(conn.get_dsn_parameters())


    with conn.cursor() as cur:
        cur.execute("select employer_id, employer_outid, empl_name from employers where empl_url like '%hh.ru%';")
        #организуем цикл по запрашиванию вакансий с сайта по всем уже скачанным работодателям
        for empl in cur.fetchall() :
            print(f'{empl[0]}. Скачиваю вакансии {empl[2]}')
            filling_table(f'https://api.hh.ru/vacancies?employer_id={empl[1]}', {
                    'count': 1000,
                    'is_archive': False, # неархивные вакансии
                    'order_field':'payment',   #сразу с сортировкой по зарплате - по убыванию по умолчанию
                                                           'per_page': 100}, 'items', 'vacancies', params,
                          'vacancies',
                          ('vac_outid', 'vac_name', 'vac_url',
                           'sal_from', 'sal_to', 'sal_currency','sal_gross',
                           'area_name','address', 'vac_requirements',
                           'employer_id'),  # в какие поля  таблицы
                          ['id', 'name',  'alternate_url',
                           'salary|from','salary|to','salary|currency','salary|gross',
                           'area|name','address|raw','snippet|requirement'], # из каких полей json
                          [empl[0]                           ],
                          company_vac_count, f'вакансий {empl[2]}')

    conn.commit()
    income_updating(database_name, params)
    conn.close()

def income_updating(database_name: str, params: dict):
   """ Обновляем поле income дохода на руки после налогообложения """

   update_exchange_rate(database_name, params)
   conn = psycopg2.connect(dbname=database_name, **params)
   conn.autocommit
   with conn.cursor() as cur:
       cur.execute("UPDATE vacancies "
       # странно, но привычная мне конструкция, ниже приведенная в #, не работает верно - pgs везде одно значение ставит
#          SET income=coalesce(vac.sal_to, coalesce(vac.sal_from,0))*(1-case 
#          when coalesce(vac.sal_gross,true) then 0.13 else 0 end)/cur_rate
#          FROM vacancies vac LEFT JOIN exchange_rate on vac.sal_currency=cur_code;
         
         "SET income=coalesce(sal_to, coalesce(sal_from,0))*(1-case " 
          "when coalesce(sal_gross,true) then 0.13 else 0 end)/"
                   "coalesce((select cur_rate from exchange_rate where cur_code=sal_currency limit 1),1);"
       "COMMIT")

   conn.commit()
   conn.close()