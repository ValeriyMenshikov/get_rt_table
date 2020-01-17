import cx_Oracle
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


logging.disable(logging.INFO)

class Parameter:
    def __init__(self, table_name, id_realization_1, sql_query, db, different=False, id_realization_2='0'):
        self.table_name = table_name
        self.id_realization_1 = id_realization_1
        self.id_realization_2 = id_realization_2
        self.sql_query = sql_query
        self.db = db
        self.different = different


def get_data_from_table(Parameter):
    logging.debug(f'Устанавливаем соединение c {Parameter.db} установлено')
    connection = cx_Oracle.connect(Parameter.db)
    logging.debug(f'Соединение c {Parameter.db} установлено')

    if Parameter.different:
        logging.debug('Формируем запрос для выгрузки реализации')
        sql = Parameter.sql_query + Parameter.id_realization_1
    else:
        logging.debug('Формируем запрос для выгрузки результата сравнения реализации')
        sql = Parameter.sql_query + Parameter.id_realization_1 + '\nminus\n' + Parameter.sql_query + Parameter.id_realization_2

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

        while True:
            rows = cur.fetchmany(numRows)
            logging.debug('Запрос отправлен формируем датафрейм')

            df = pd.DataFrame(list(rows), columns=columns)
            # Сначала записываем в файл с заголовком
            if sum_rows == 0:
                df.to_csv(f'{Parameter.table_name}.csv', mode='a', header=True, index=False)
            # Потом дописываем в файл без заголовка
            else:
                df.to_csv(f'{Parameter.table_name}.csv', mode='a', header=False, index=False)

            sum_rows += len(rows)
            logging.info(f'В {Parameter.table_name}.csv выгружено {sum_rows}  строк..')
            if not rows:
                print(f'ТАБЛИЦА ВЫГРУЖЕНА В {Parameter.table_name}.csv')
                break


SQL_REPS_CR_RES_RT = f"""select c_thrd_num, c_dog_index, c_dog_ref, c_debt_code,
                                c_acc_num, c_acc_ref, c_res_prc, c_summ_res,
                                c_base_acc_num, c_base_acc_ref, c_summ_debt#summ_rub,
                                c_summ_debt#summ_val, c_line_ref, c_res_acc_saldo,
                                c_res_prod, c_gr_risk, c_kind_res, c_summ_rasch_res,
                                c_uch_debt_code, c_pos_num, c_pot_num, c_pot_ref, c_dog_class
                           from z#REPS_CR_RES_RT
                          where c_pn = """

SQL_REPS_RES_INFO_RT = """select t.c_prod_reserv, t.c_kind, t.c_summ_res, t.c_res_acc_ref, t.c_res_acc_num, t.c_debt_code,
                                 t.c_res_prc, t.c_gr_risk, t.c_res_acc_saldo, t.c_pot_ref, t.c_port_num, t.o$acc_type,
                                 t.c_acc_corr_type, t.c_summ_corr, t.c_acc_positiv_ref, t.c_acc_negativ_ref, t.c_acc_negativ_num,
                                 t.c_acc_type_positiv, t.c_acc_type_negativ, t.c_dog_ref, t.c_corr_type, t.c_acc_type, t.c_acc_type, t.c_cr_rez_ln
                            from z#REPS_RES_INFO_RT t
                           where t.c_pn = """

SQL_REPS_DEBT_RT = """select t.c_dog_ref, t.c_acc_num, t.c_debt_code, t.c_line_ref, t.c_summ_debt#summ_rub, t.c_summ_debt#summ_val, t.c_date_close,
                             t.c_acc_ref, t.c_acc_saldo#summ_rub, t.c_acc_saldo#summ_val, t.c_main_acc_num, t.c_currency_code, t.c_currency_ref,
                             t.c_summ_gash#summ_rub, t.c_summ_gash#summ_val, t.c_summ_zlg#summ_val, t.c_summ_zlg#summ_rub, t.c_thrd_num, t.c_dog_index,
                             t.c_summ_plus#summ_rub, t.c_summ_plus#summ_val, t.c_summ_minus#summ_rub, t.c_summ_minus#summ_val, t.c_date_appearence,
                             t.c_uch_debt_code, t.c_line_debt_summ#summ_rub, t.c_line_debt_summ#summ_val, t.c_prod_res, t.c_type_prod_res, t.c_acc_active_type,
                             t.c_port_res, t.c_wtype, t.c_acc_name, t.c_wtype_res, t.c_last_res_summ#summ_rub, t.c_last_res_summ#summ_val, c.class_id, k.c_name
                        from z#reps_debt_rt t, z#pr_cred c, z#kind_credits k
                        where c.id = t.c_dog_ref
                        and c.c_kind_credit = k.id
                        and t.c_pn = """

SQL_RT_NAVIGATOR = """SELECT C_1 Дата_расчета,
                             C_2 Филиал,
                             C_3 Заемщик,
                             C_4 Класс_клиента,
                             C_5 Тип_клиента,
                             C_6 Резидентность,
                             C_7 Номер_договора,
                             TO_CHAR(C_8) ID_договора,
                             C_9 Номер_договора_верхнего_уровня,
                             C_10 Вид_договора,
                             C_11 Дата_выдачи_кредита,
                             C_12 Дата_окончания_договора,
                             C_13 Дата_закрытия_договора,
                             C_14 Дата_предельной_выдачи_ссуды,
                             C_15 Счет_основного_долга,
                             C_16 Счет_учета_задолженности,
                             C_17 Наименование_счета_задолженности,
                             C_18 Валюта_счета,
                             TO_CHAR(C_19) Остаток_врублях,
                             C_20 Вид_задолженности,
                             C_21 Учетный_вид_задолженности,
                             TO_CHAR(C_22) Сумма_задолженности_в_валюте,
                             TO_CHAR(C_23) Сумма_задолженности_в_рублях,
                             C_24 Категория_качества,
                             TO_CHAR(C_25) Процент_резервирования,
                             TO_CHAR(C_26) Минимальный_процент_резервирования,
                             C_27 Продукт_резервирования,
                             C_28 Счет_резерва,
                             TO_CHAR(C_29) Фактический_резерв,
                             TO_CHAR(C_30) Расчетный_резерв,
                             C_31 Резерв_Счет_положительной_корректировки,
                             TO_CHAR(C_32) Резерв_Сумма_положительной_корректировки,
                             C_33 Резерв_Счет_отрицательной_корректировки,
                             TO_CHAR(C_34) Резерв_Сумма_отрицательной_корректировки,
                             C_35 Номер_ПОС_Номер_ПОТ,
                             C_36 Вид_ПОС,
                             C_37 Урегулирование_резерва_в_ПОС,
                             TO_CHAR(C_38) Минимальный_срок_просрочки_по_ПОС,
                             TO_CHAR(C_39) Максимальный_срок_просрочки_по_ПОС,
                             TO_CHAR(C_40) Просроска_1_30,
                             TO_CHAR(C_41) Просроска_31_90,
                             TO_CHAR(C_42) Просроска_91_180,
                             TO_CHAR(C_43) Просрочка_свыше_180,
                             TO_CHAR(C_44) Срок_до_востребования_и_1_день,
                             TO_CHAR(C_45) Срок_2_5,
                             TO_CHAR(C_46) Срок_6_10,
                             TO_CHAR(C_47) Срок_11_20,
                             TO_CHAR(C_48) Срок_21_30,
                             TO_CHAR(C_49) Срок_31_90,
                             TO_CHAR(C_50) Срок_91_180,
                             TO_CHAR(C_51) Срок_181_270,
                             TO_CHAR(C_52) Срок_270_1год,
                             TO_CHAR(C_53) Срок_свыше_1года,
                             TO_CHAR(C_54) Сумма_операций_к_погашению_в_отчетном_периоде,
                             TO_CHAR(C_55) Сумма_операций_не_погашенных_в_срок_в_отчетном_периоде,
                             C_56 Стоимость_ФИ_счет_положительной_корректировки,
                             TO_CHAR(C_57) Стоимость_ФИ_Сумма_положительной_корректировки,
                             C_58 Стоимость_ФИ_счет_отрицательной_корректировки,
                             TO_CHAR(C_59) Стоимость_ФИ_Сумма_отрицательной_корректировки,
                             C_60 Стоимость_ФИ_счет_положительной_переоценки,
                             TO_CHAR(C_61) Стоимость_ФИ_Сумма_положительной_переоценки,
                             C_62 Стоимость_ФИ_счет_отрицательной_переоценки,
                             TO_CHAR(C_63) Стоимость_ФИ_Сумма_отрицательной_переоценки,
                             C_64 Категория_учета_ФИ,
                             C_69 Признак_карточного_кредита,
                             C_70 Признак_плавающей_процентной_ставки,
                             C_71 Признак_нахождения_договора_на_балансе,
                             C_72 Корректировать_размер_резерва_до_нуля,
                             C_73 Качество_обслуживания_долга,
                             C_74 Признак_чувствительности_к_изменению_процентной_ставки
                        FROM IBS.VW_CRIT_CRED
                       WHERE C_76 = """
