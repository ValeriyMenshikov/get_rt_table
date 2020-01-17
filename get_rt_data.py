import datetime
from SQLs import SQL_REPS_CR_RES_RT, \
    SQL_REPS_DEBT_RT, \
    SQL_REPS_RES_INFO_RT, \
    SQL_RT_NAVIGATOR, \
    Parameter, \
    get_data_from_table
from concurrent.futures import ThreadPoolExecutor
import logging

# db = 'IBS/IBS_EXPOBANK1908@BARKDB.BARK_RPT'
db = 'IBS/IBS@57_TEST_REP_OMSK'
logging.disable(logging.DEBUG)

print("""
1. REPS_CR_RES_RT
2. REPS_DEBT_RT
3. REPS_RES_INFO_RT
4. RT_NAVIGATOR
""")

different = str(input('Сравнение или все?:'))
inp = str(input('УКАЖИ НОМЕРА ВЫГРУЖАЕМЫХ ТАБЛИЦ В ФОРМАТЕ (12 или 1234):'))
if not different:
    realisation_1 = str(input("ВВЕДИ id 1 РЕАЛИЗАЦИИ:"))
    realisation_2 = str(input("ВВЕДИ id 2 РЕАЛИЗАЦИИ:"))
else:
    realisation_1 = str(input("ВВЕДИ id РЕАЛИЗАЦИИ:"))
    realisation_2 = ''

startTime = datetime.datetime.today()
print(startTime)

logging.debug(f"Realisation_1 = {realisation_1}, type = {bool(realisation_1)} ")
logging.debug(f"Realisation_2 = {realisation_2}, type = {bool(realisation_1)} ")

Params = []
for i in inp:
    if not different:
        logging.info('Формируем параметры для выгрузки расхождений')
        if i == '1':
            logging.info('Формируем параметры для таблицы REPS_CR_RES_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_CR_RES_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_CR_RES_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_CR_RES_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_CR_RES_RT, db=db))
        elif i == '2':
            logging.info('Формируем параметры для таблицы REPS_DEBT_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_DEBT_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_DEBT_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_DEBT_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_DEBT_RT, db=db))
        elif i == '3':
            logging.info('Формируем параметры для таблицы REPS_RES_INFO_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_RES_INFO_RT_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_REPS_RES_INFO_RT, db=db))
            Params.append(
                Parameter(table_name='SQL_REPS_RES_INFO_RT_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_REPS_RES_INFO_RT, db=db))
        elif i == '4':
            logging.info('Формируем параметры для получения данных из представления навигатора')
            Params.append(
                Parameter(table_name='SQL_RT_NAVIGATOR_1', id_realization_1=realisation_1,
                          id_realization_2=realisation_2, sql_query=SQL_RT_NAVIGATOR, db=db))
            Params.append(
                Parameter(table_name='SQL_RT_NAVIGATOR_2', id_realization_1=realisation_2,
                          id_realization_2=realisation_1, sql_query=SQL_RT_NAVIGATOR, db=db))
    else:
        logging.info('Формируем параметры для выгрузки всей таблицы')
        if i == '1':
            logging.info('Формируем параметры для таблицы REPS_CR_RES_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_CR_RES_RT_1', id_realization_1=realisation_1,
                          sql_query=SQL_REPS_CR_RES_RT, db=db, different=True))
        elif i == '2':
            logging.info('Формируем параметры для таблицы REPS_DEBT_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_DEBT_RT_1', id_realization_1=realisation_1,
                          sql_query=SQL_REPS_DEBT_RT, db=db, different=True))
        elif i == '3':
            logging.info('Формируем параметры для таблицы REPS_RES_INFO_RT')
            Params.append(
                Parameter(table_name='SQL_REPS_RES_INFO_RT_1', id_realization_1=realisation_1,
                          sql_query=SQL_REPS_RES_INFO_RT, db=db, different=True))
        elif i == '4':
            logging.info('Формируем параметры для получения данных из представления навигатора')
            Params.append(
                Parameter(table_name='SQL_RT_NAVIGATOR_1', id_realization_1=realisation_1,
                          sql_query=SQL_RT_NAVIGATOR, db=db, different=True))

print(f'Максимальное возможное количество потоков {len(Params)}')
threads = int(input('Укажи количество потоков:\n'))

with ThreadPoolExecutor(max_workers=threads) as executor:
    logging.debug('Формируем потоки')
    start_threads = {executor.submit(get_data_from_table, param): param for param in Params}

endTime = datetime.datetime.today()
print(f"ЗАВЕРШЕНО {endTime - startTime}")
