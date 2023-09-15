from bs4 import BeautifulSoup
import re
import requests
from datetime import datetime, timedelta
import pandas as pd

request_count = 0
time_check = datetime.now() + timedelta(microseconds=10)
def includes(words_list, phrase):
 ''' Функция includes возвращает Истину, если любое из слов в любом порядке входит в проверяемый текст
 При этом если искомое словосочетание разделено *-звездочками,  требуется одновременное вхождение всех частей (AND-наличие),
 а любой отдельный элемент списка проверяется как OR-вхождение (хотя бы один) '''

 incl=True
 for i in words_list:
        if phrase is None:
            pass
        else:
            for j in i.split("*"):
                if not(j in phrase):
                   incl = False
                   break
 return incl


def income(salary_from, salary_to, income_tax):
  ''' Функция по 3-м аргументам (диапазон ЗП минимум-максимум и доля подоходного налога) возвращает доход, получаемый на руки.
    Используется для корректной сортировки вакансий, которые могут быть с выдачей ЗП 'на руки' без подоходного.'''

  money_to_get =  (0 if salary_from is None or salary_from==0 else salary_from) if salary_to is None or salary_to==0 else salary_to
  money_to_get-= (money_to_get * income_tax)
  return int(money_to_get)


def RemoveHTMLTags(html_str:str)->str:  #очистка от html-tэгов в длинных текстах
#для вывода на экран предъявляемых требований к кандидату на вакансию, НЕ УСЕЧЕННЫХ API-запросом, но очищенных от html-тэгов
    return BeautifulSoup(html_str, features="html.parser").get_text()

def narrow_down_print(big_str:str):   # col_count=120): # , col_count:int):   # не пойму, почему не работает при вызове по умолчанию col_count=120
 """ Функция переноса длинной строки текста на последующие строки при выводе на экран
  big_str when printed in the command console, may exceed 80.
Characters in length and wrap around, looking ugly.
Сузим для печати на экран  границы выводимого текста """

 print('\n'.join(line.strip() for line in re.findall(r'.{1,134}(?:\s+|$)', big_str)))

def get_value(data_row:dict,x:str):
    ''' get_value() возвращает значение поля из словаря (json) с глубиной вложений до трех.
    Адресация при этом задается через | по аналогии со слэшем в пути, например,
    при x= salary|currency воспримется как get_value=data_row[salary][currency] '''

    down_fields=x.split("|",2)
    l=len(down_fields)
    try:
      if l==1:
        return data_row[down_fields[0]]
      elif l==2:
         return data_row[down_fields[0]][down_fields[1]]
      else:
         return data_row[down_fields[0]][down_fields[1]][down_fields[2]]  #глубину до трех допускаю
    except:
         return  #при отсутствии значений поля json такой вложенности

#class ApiClient(ABC):
    """Класс для получения вакансий с помощью API"""
exchange_rate = {} #курс валют возьму с hh

def parsing_delay(req_per_sec:int):
        ''' Функция притормаживания для API-запросов'''
        global request_count, time_check        #request_count - число запросов в секунду, для hh=30: https://github.com/hhru/api/issues/74
                #time_check -  последнее ограничение - время начала "будущей" задержки в случае исчерпания req_per_sec запросов

        request_count+=1
        if request_count == req_per_sec:
            request_count=0

            if pd.to_datetime(time_check)>pd.to_datetime(datetime.now()):
                time.sleep(((time_check-datetime.now()).total_microseconds())/10)
            time_check =datetime.now()+timedelta(microseconds=10*req_per_sec)


def get_exchange_rate():
        #получение курса валют с Hh для использования сортировок по зарплате вне зависимости от валюты "оклада"

        print('Получим курсы валют с HeadHunter (ждите):')


        dictionaries = requests.get('https://api.hh.ru/dictionaries').json()
        for currency in dictionaries['currency']:
           #ApiClient.exchange_rate[currency['code']] = currency['rate']
           exchange_rate[currency['code']] = currency['rate']
        narrow_down_print(str(exchange_rate))
        return  exchange_rate
