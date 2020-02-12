import cx_Oracle
import logging
import pandas as pd


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.INFO)


def get_data_from_table(Parameter):
    logging.debug(f'Устанавливаем соединение c {Parameter.db} установлено')
    connection = cx_Oracle.connect(Parameter.db)
    logging.debug(f'Соединение c {Parameter.db} установлено')
    if Parameter.id_realization_2 is not None:
        logging.debug('Формируем запрос для выгрузки результата сравнения реализации')
        sql = Parameter.sql_query + Parameter.id_realization_1 + '\nminus\n' + Parameter.sql_query + Parameter.id_realization_2
    else:
        logging.debug('Формируем запрос для выгрузки реализации')
        sql = Parameter.sql_query + Parameter.id_realization_1

    logging.debug(sql)

    with connection.cursor() as cur:
        logging.debug('Открываем курсор')
        cur.execute(sql)
        # Получаем именования колонок
        columns = [desc[0] for desc in cur.description]

        # Устанавливаем количество строк для fetch
        numRows = 10000

        # Счетчик выгруженных строк
        sum_rows = 0
        switch = True
        while True:
            rows = cur.fetchmany(numRows)
            logging.debug('Запрос отправлен формируем датафрейм')

            df = pd.DataFrame(list(rows), columns=columns)
            # Сначала записываем в файл с заголовком
            if len(df) != 0:
                switch = False
                if sum_rows == 0:
                    df.to_csv(f'{Parameter.table_name}.csv', mode='a', header=True, index=False)
                # Потом дописываем в файл без заголовка
                else:
                    df.to_csv(f'{Parameter.table_name}.csv', mode='a', header=False, index=False)
            if switch:
                print(f'В таблице {Parameter.table_name} расхождений нет')

            sum_rows += len(rows)
            logging.info(f'В {Parameter.table_name}.csv выгружено {sum_rows}  строк..')
            if not rows:
                # print(f'ТАБЛИЦА ВЫГРУЖЕНА В {Parameter.table_name}.csv')
                break
