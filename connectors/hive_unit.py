import pandas as pd
from sqlalchemy.engine import create_engine

from configs.db import HIVE_CONFIG


class HiveUnit:
    def __init__(self, host, port, username, password, database='default', auth='LDAP'):
        self.__engine = create_engine(f"hive://{username}:{password}@{host}:{port}",
                                      connect_args={'auth': auth},
                                      echo=True
                                      )
        self.__conn = self.__engine.connect()

    def execute(self, query):
        """execute query"""
        self.__execute_before()
        queries = query.split(';')
        for q in queries:
            if q.strip():
                self.__execute_one(q)

    def get_df_from_db(self, query):
        """get dataframe from database"""
        self.__execute_before()
        return pd.read_sql(query, self.__conn)

    def df2db(self, df: pd.DataFrame, tab_name, if_exists='replace'):
        self.__execute_before()
        df.to_sql(tab_name, self.__conn, if_exists=if_exists)

    def release(self):
        try:
            self.__conn.close()
            self.__engine.dispose()
        except:
            pass

    def __execute_before(self):
        self.__conn.execute("set role all")
        self.__conn.execute("set hive.execution.engine = tez")
        self.__conn.execute("set tez.queue.name = xxxx")

    def __execute_one(self, query):
        self.__conn.execute(query)


def hiveunit_test():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    hive_unit.execute('show databases')
    df = hive_unit.get_df_from_db("select * from default.test limit 10")
    print(df)
    hive_unit.release()


if __name__ == '__main__':
    hiveunit_test()
