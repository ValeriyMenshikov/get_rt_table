from concurrent.futures import ThreadPoolExecutor
from work_with_db import get_data_from_table
from sql import SQL
import datetime
import logging

while True:
    db = input(
        '''
УКАЖИ СХЕМУ ДЛЯ ПОДКЛЮЧЕНИЯ НАПРИМЕР:

IBS/IBS_EXPOBANK1908@BARKDB.BARK_RPT
IBS/IBS@57_TEST_REP_OMSK
IBS/IBS@57_TEST_REP_TOMSK
IBS/IBS@57_TEST_REP_NSK1
IBS/IBS@57_TEST_REP_NSK2

СХЕМА:
''')
    logging.disable(logging.DEBUG)

    num = 0
    table_names = list(SQL.keys())
    for table_name in table_names: print(f'{num}.', table_name); num += 1

    tables = input('\nУКАЖИ НОМЕРА ВЫГРУЖАЕМЫХ ТАБЛИЦ В ФОРМАТЕ (12 или 0123):')
    id_realization_1 = str(input("ВВЕДИ id 1 РЕАЛИЗАЦИИ:"))
    id_realization_2 = str(input("ВВЕДИ id 2 РЕАЛИЗАЦИИ:"))

    startTime = datetime.datetime.today()
    print(startTime)
    Params = []
    for table in tables:
        table_name = table_names[int(table)]
        sql_query = SQL[table_name]
        if id_realization_2 != '':
            Params += [(table_name + '1', id_realization_1, id_realization_2, sql_query, db)]
            Params += [(table_name + '2', id_realization_2, id_realization_1, sql_query, db)]
        else:
            Params += [(table_name, id_realization_1, None, sql_query, db)]

    with ThreadPoolExecutor(max_workers=len(Params)) as executor:
        logging.debug('Запускаем потоки')
        list(map(lambda param: executor.submit(get_data_from_table, param), Params))

    endTime = datetime.datetime.today()
    print(f"ЗАВЕРШЕНО {endTime - startTime}")
