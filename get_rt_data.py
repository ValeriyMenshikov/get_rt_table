import datetime
from SQLs import SQL_REPS_CR_RES_RT, \
    SQL_REPS_DEBT_RT, \
    SQL_REPS_RES_INFO_RT, \
    SQL_RT_NAVIGATOR, \
    Parameter, \
    get_data_from_table
from concurrent.futures import ThreadPoolExecutor
import logging

db = 'IBS/IBS_EXPOBANK1908@BARKDB.BARK_RPT'
# db = 'IBS/IBS@57_TEST_REP_OMSK'

print("""
1. REPS_CR_RES_RT
2. REPS_DEBT_RT
3. REPS_RES_INFO_RT
4. RT_NAVIGATOR
""")

inp = str(input('УКАЖИ НОМЕРА ВЫГРУЖАЕМЫХ ТАБЛИЦ В ФОРМАТЕ (12 или 1234):'))
realisation_1 = str(input("ВВЕДИ id 1 РЕАЛИЗАЦИИ:"))
realisation_2 = str(input("Введи id 2 РЕАЛИЗАЦИИ:"))

startTime = datetime.datetime.today()
print(startTime)


Params = []
for i in inp:
    if realisation_1 and realisation_2:
        if i == '1':
            logging.debug('Формируем параметры для таблицы REPS_CR_RES_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_CR_RES_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_CR_RES_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_CR_RES_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_CR_RES_RT, db=db))
        elif i == '2':
            logging.debug('Формируем параметры для таблицы REPS_DEBT_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_DEBT_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_DEBT_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_DEBT_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_DEBT_RT, db=db))
        elif i == '3':
            logging.debug('Формируем параметры для таблицы REPS_RES_INFO_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_RES_INFO_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_RES_INFO_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_RES_INFO_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_RES_INFO_RT, db=db))
        elif i == '4':
            logging.debug('Формируем параметры для получения данных из представления навигатора')
            Params.append(
                Parameter(table_name='SQL_RT_NAVIGATOR_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_RT_NAVIGATOR, db=db))
            Params.append(
                Parameter(table_name='SQL_RT_NAVIGATOR_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_RT_NAVIGATOR, db=db))


print(f'Максимальное возможное количество потоков {len(Params)}')
threads = int(input('Укажи количество потоков:\n'))

with ThreadPoolExecutor(max_workers=threads) as executor:
    logging.debug('Формируем потоки')
    start_threads = {executor.submit(get_data_from_table, param): param for param in Params}

endTime = datetime.datetime.today()
print(f"ЗАВЕРШЕНО {endTime - startTime}")
