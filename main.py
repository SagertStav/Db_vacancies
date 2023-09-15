""" Основной модуль взаимодействия с пользователем для получения ключевых слов
    для поиска вакансий """
import db_making
import dbmanager

def user_interaction():
  global   companies_count, company_vac_count

  companies_list = ['газпром межрегионгаз',  # 'газпром' много дает
                      'автомакон', 'спортмастер', 'ставрополь', 'медиафарм', 'медбизнесконсалтинг']


  params = db_making.config()

  db_making.create_database('vacancies', params)  

  db_making.filling_employers(companies_list,'vacancies', params) # Получение работодателей с hh по примерному списку companies_list
  db_making.filling_vacancies( 'vacancies', params) # Получение их вакансий

  print('База сформирована по списку работодателей ',companies_list)
  dbmanager.get_companies_and_vacancies_count()
  db_making.income_updating('vacancies', db_making.config())
  dbmanager.get_all_vacancies()
  dbmanager.get_avg_salary()
  dbmanager.get_vacancies_with_higher_salary()

  filter_words = input('Введите ключевые слова для фильтрации вакансий (через +, например, SQL+VBA). По умолчанию [программист+разработчик+Python]: ')
  filter_words = 'программист+разработчик+Python' if filter_words=='' else filter_words
  dbmanager.get_vacancies_with_keyword(filter_words)
  while True:
    q=input('Хотите снова повторить? Введите\n'
              ' 1-получить список скачанных компаний и количество вакансий у каждой компании;\n'
              ' 2-получить список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию;\n'
              ' 3-получить среднюю зарплату по скачанным вакансиям;\n'
              ' 4-получить список всех вакансий, у которых зарплата выше средней по всем скачанным вакансиям;\n'
              ' 5-получить список вакансий (из скачанных) по ключевым словам;\n'
              ' 6-изменить перечень работодателей по названию и скачать вакансии заново;\n'
              ' 7-обновить курс валют/зарплат;\n'
              ' 8-выход ? ')
    print("\n")
    match q:
      case '1': # 1-получить список скачанных компаний и количество вакансий у каждой компании
        dbmanager.get_companies_and_vacancies_count()
      case '2': # 2-получить список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию
        dbmanager.get_all_vacancies()
      case '3': # 3-получить среднюю зарплату по скачанным вакансиям
        dbmanager.get_avg_salary()
      case '4': # 4-получить список всех вакансий, у которых зарплата выше средней по всем скачанным вакансиям
        dbmanager.get_vacancies_with_higher_salary()
      case '5': # 5-получить список вакансий (из скачанных) по ключевым словам
        filter_words = input('Введите ключевые слова для фильтрации вакансий (через +, например, SQL+VBA).'
                             ' По умолчанию [программист+разработчик+Python]: ')
        filter_words = 'программист+разработчик+Python' if filter_words == '' else filter_words
        dbmanager.get_vacancies_with_keyword(filter_words)

      case '6': # 6-изменить перечень работодателей по названию и скачать вакансии заново
        companies_list = ['газораспределение','медбизнес','медиафарм']
        companies = input('Введите ключевые слова в названии работодателей (через +, например, Спортмастер+Газпром+газораспределение).\n'
                          ' По умолчанию [газораспределение+медбизнес+медиафарм]: ')
        companies_list = companies_list if companies == '' else companies.split('+')

        db_making.create_database('vacancies', params)
        db_making.filling_employers(companies_list, 'vacancies',
                                  params)  # Получение работодателей с hh по примерному списку companies_list
        db_making.filling_vacancies('vacancies', params)  # Получение их вакансий
        print('База сформирована по списку работодателей ', companies_list)
        dbmanager.get_companies_and_vacancies_count()  #+выведу список скачанных компаний и количество вакансий у каждой компании

      case '7': #' 7-обновить курс валют/зарплат;'
        db_making.income_updating('vacancies', db_making.config())
        dbmanager.get_avg_salary()   # + выведу среднюю зарплату, которая могла измениться от курсов валют
      case '8': break       #выход,    case _ не нужен

if __name__ == "__main__":
  user_interaction()
