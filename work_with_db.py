import cx_Oracle
import logging
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.INFO)


def get_data_from_table(parameter):
    table_name = parameter[0]
    id_realization_1 = parameter[1]
    id_realization_2 = parameter[2]
    sql_query = parameter[3]
    db = parameter[4]
    logging.debug(f'Устанавливаем соединение c {db} установлено')
    connection = cx_Oracle.connect(db)
    logging.debug(f'Соединение c {db} установлено')
    if id_realization_2 is not None:
        logging.debug('Формируем запрос для выгрузки результата сравнения реализации')
        sql = sql_query + id_realization_1 + '\nminus\n' + sql_query + id_realization_2
    else:
        logging.debug('Формируем запрос для выгрузки реализации')
        sql = sql_query + id_realization_1

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
        rows = True
        while rows:
            rows = cur.fetchmany(numRows)
            logging.debug('Запрос отправлен формируем датафрейм')
            df = pd.DataFrame(list(rows), columns=columns)
            if rows:
                # Сначала записываем в файл с заголовком
                if sum_rows == 0:
                    df.to_csv(f'{table_name}.csv', mode='a', header=True, index=False)
                # Потом дописываем в файл без заголовка
                else:
                    df.to_csv(f'{table_name}.csv', mode='a', header=False, index=False)
            else:
                print(f'В таблице {table_name} расхождений нет')
            sum_rows += len(rows)
            logging.info(f'В {table_name}.csv выгружено {sum_rows}  строк..')
