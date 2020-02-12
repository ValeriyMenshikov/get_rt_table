class Parameter:
    def __init__(self, table_name=None, id_realization_1=None, sql_query=None, db=None, id_realization_2=None):
        self.table_name = table_name
        self.id_realization_1 = id_realization_1
        self.id_realization_2 = id_realization_2
        self.sql_query = sql_query
        self.db = db

    def __repr__(self):
        return f"table_name={self.table_name}, id_realization_1={self.id_realization_1}, " \
               f"sql_query={self.sql_query}, db={self.db}, id_realization_2={self.id_realization_2}"
