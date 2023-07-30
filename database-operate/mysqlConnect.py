import pandas as pd
from sqlalchemy import create_engine
import mysql.connector as mql


class mysqlObject:
    def __init__(self, host="47.102.126.153", port="3306", username="aidata", password="aidata") -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def mysqlEngine(self, db):
        engine = create_engine(
            "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
                host=self.host,
                user=self.username,
                pw=self.password,
                port=self.port,
                db=db,
            )
        )
        return engine

    def mysqlConnect(self, db):
        connection = mql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=db,
        )
        return connection

    def writeData(
        self,
        data: pd.DataFrame,
        table_name,
        db="aidata",
        if_exists="replace",
        dataType=None,
    ):
        engine = self.mysqlEngine(db=db)
        engine_connection = engine.connect()
        if not dataType:
            data.to_sql(
                table_name,
                engine_connection,
                index_label="ID",
                if_exists=if_exists,
                dtype=dataType,
            )
        else:
            data.to_sql(
                table_name, engine_connection, index_label="ID", if_exists=if_exists
            )
        engine_connection.close()

    def queryData(self, sql, db="aidata"):
        engine = self.mysqlEngine(db=db)
        engine_connection = engine.connect()
        data = pd.read_sql(sql, engine_connection)
        engine_connection.close()
        return data

    def deleteData(self, sql, db="aidata"):
        connection = self.mysqlConnect(db=db)
        mycursor = connection.cursor()
        mycursor.execute(sql)
        connection.commit()
        print(mycursor.rowcount, "record(s) deleted")
        mycursor.close()
        connection.close()

    def writeDataAfterDelete(self, data, table_name, deleteSql, if_exists='append', db='aidata'):
        try:
            self.deleteData(f"""delete from {table_name} where {deleteSql}""")
        except Exception as e:
            print(str(e))
        finally:
            self.writeData(data=data, table_name=table_name, if_exists=if_exists)




if __name__ == "__main__":
    product_mysql = mysqlObject()
    df = product_mysql.queryData("""
    select * from noResultKey_main limit 10
    """)
    print(df)
