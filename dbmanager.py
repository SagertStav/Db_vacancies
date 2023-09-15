class DBManager:
   ''' класс DBManager для подключения к БД PostgreSQL'''
import psycopg2
import db_making
database_name='vacancies'

def get_companies_and_vacancies_count():
    ''' — получает список всех компаний и количество вакансий у каждой компании.'''
    params = db_making.config()
    conn = psycopg2.connect(dbname=database_name, **params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("select row_number() OVER(ORDER BY count(vac_id) DESC, empl_name) N,"
                    "empl_name, count(vac_id) вакансий from employers join vacancies using (employer_id)"
                    "group by empl_name order by вакансий desc, empl_name") #using vacancies.employer_id

        r=cur.fetchall()
        print(f'Всего скачаны вакансии {len(r)} работодателей:')
        print('\n'.join([f'{x[0]:5.0f}. {x[1]} - {x[2]} вакансий' for x in r]),'\n')
        return r




def get_all_vacancies():
  '''— получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.'''
  params = db_making.config()
  conn = psycopg2.connect(dbname=database_name, **params)
  conn.autocommit = True
  with conn.cursor() as cur:
      cur.execute("select row_number() OVER(ORDER BY income DESC, empl_name) N,"
                  "empl_name, vac_name, income, vac_url, sal_currency from employers join vacancies using (employer_id)"
                  "order by income desc, empl_name")
      r=cur.fetchall()
      print(f'Всего скачано {len(r)} вакансий:')
      print('\n'.join([f'{x[0]:5.0f}. {x[1]} - {x[2]}: {0 if x[3] is None else x[3]: 7.0f} руб., {x[4]}{"" if x[5] is None else ", "}{"" if x[5] is None else x[5]}' for x in r]),'\n')
      return r


def get_avg_salary():
 ''' — получает среднюю зарплату по вакансиям. '''

 params = db_making.config()
 conn = psycopg2.connect(dbname=database_name, **params)
 conn.autocommit = True
 with conn.cursor() as cur:
    cur.execute('select AVG(income) "средняя_ЗП" from vacancies where income>0')
    r=cur.fetchone()
    print(r)
    if r is None or r[0] is None:
      print('Записи отсутствуют, определить среднюю зарплату невозможно\n')
    else:
      print(f"Средняя зарплата (на руки): {r[0]:4.0f} руб.\n")
    return r


def get_vacancies_with_higher_salary():
 '''— получает список всех вакансий, у которых зарплата выше средней по всем вакансиям. '''
 params = db_making.config()
 conn = psycopg2.connect(dbname=database_name, **params)
 conn.autocommit = True
 with conn.cursor() as cur:
    cur.execute('select row_number() OVER(ORDER BY income DESC, empl_name, vac_outid) N, income, '
                'vac_name, empl_name, area_name, vac_url, sal_currency '
                'from vacancies join employers using (employer_id) '
                'where income>(select AVG(income) from vacancies vac2 where income>0)'
                'order by income DESC, empl_name, vac_outid')
    print(f'Вакансии с доходом (на руки) выше среднего:'  if cur.rowcount>0 else 'Записи с зарплатой выше среднего отсутствуют')
    print('\n'.join([f'{x[0]:5.0f}. {x[1]:6.0f} руб. - {x[2]} - в {x[3]} ({x[4]}): {x[5]}{"" if x[6] is None else ", "}{"" if x[6] is None else x[6]}' for x in cur.fetchall()]))
    return cur.fetchall()

def get_vacancies_with_keyword(string_of_words:str):
 '''— получает список всех вакансий, в названии которых содержатся переданные в метод слова,
 например python.'''
 # сгенерим строку (для WHERE в select-запросе) для поиска по двум полям в НАШЕЙ базе данных:
 # по вхождению слов в название вакансии vac_name или в требования vac_requirements:
 sql_key='(' + " OR ".join(["LOWER(vac_name) like '%" + word + "%' or LOWER(vac_requirements) like '%" + word + "%'" for word in string_of_words.lower().split('+')])+')'
 params = db_making.config()
 conn = psycopg2.connect(dbname=database_name, **params)
 conn.autocommit = True
 with conn.cursor() as cur:
    cur.execute('select row_number() OVER(ORDER BY income DESC) N, income, '
                'vac_name, empl_name, area_name, vac_url, sal_currency '
                'from vacancies join employers using (employer_id) '
                f'where {sql_key}'
                'order by income DESC, empl_name' )
    print(f"Вакансии со словами '{string_of_words}' (вне зависимости от регистра):"  if cur.rowcount>0 else f"вакансии с упоминанием таких слов '{string_of_words}' отсутствуют.")
    print('\n'.join([f'{x[0]:4.0f}. {x[1]:6.0f} руб. - {x[2]} - в {x[3]} ({x[4]}): {x[5]}{"" if x[6] is None else ", "}{"" if x[6] is None else x[6]}' for x in cur.fetchall()]))
    return cur.fetchall()
