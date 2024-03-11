import psycopg2
import psycopg2.extras as extras
from .configreader import read_config
import pandas as pd
import math


class PostgresObject:
    def __init__(
        self,
        host=None,
        port=None,
        user=None,
        password=None,
        database=None,
        config_path=None,
        config_section=None,
    ):
        if config_path:
            if not config_section:
                raise Exception("config_section is None")
            config = read_config(config_path, config_section)
            self.conn = psycopg2.connect(
                database=config["dbname"],
                user=config["user"],
                password=config["password"],
                host=config["host"],
                port=config["port"],
            )
        else:
            if not host or not port or not user or not password or not database:
                raise Exception("Put in complete database info")
            self.conn = psycopg2.connect(
                database=database, user=user, password=password, host=host, port=port,
            )
        self.cursor = self.conn.cursor()

    @staticmethod
    def replace_nan_with_none(data):
        new_data = []
        for item in data:
            new_item = []
            for sub_item in item:
                if isinstance(sub_item, float) and math.isnan(sub_item):
                    new_item.append(None)
                else:
                    new_item.append(sub_item)
            new_data.append(tuple(new_item))
        return new_data

    def queryColumn(self, table):
        sql = f"select * from {table} limit 0"
        self.cursor.execute(sql)
        cols = [desc[0] for desc in self.cursor.description]
        return cols

    def query(self, sql):
        self.cursor.execute(sql)
        cols = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        self.conn.commit()
        if data:
            data = pd.DataFrame(data, columns=cols)
            return data
        return None

    def delete(self, table=None, condition=None, sql=None):
        if sql:
            self.cursor.execute(sql)
            self.conn.commit()
        else:
            if not table or not condition:
                raise Exception("table or condition is None")
            sql = f"delete from {table} where {condition}"
            self.cursor.execute(sql)
            self.conn.commit()

    def insert(self, insert_df: pd.DataFrame, table=None):
        if not table:
            raise Exception("table is None")
        field_names = self.queryColumn(table)
        try:
            insert_df = insert_df[field_names]
        except Exception as e:
            print(e)
            raise Exception(">> 插入数据表的字段与目标表字段无法保持一致")
        cols = ",".join(list(insert_df.columns))
        tuples = [tuple(x) for x in insert_df.to_numpy()]
        tuples = self.replace_nan_with_none(tuples)
        try:
            insert_query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
            extras.execute_values(self.cursor, insert_query, tuples)
            self.conn.commit()
        except psycopg2.Error as e:
            print(e)
            self.conn.rollback()

    def insertAfterDelete(self, insert_df: pd.DataFrame, table, delete_condition):
        self.delete(table=table, condition=delete_condition)
        self.insert(insert_df=insert_df, table=table)

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    po = PostgresObject(config_path=r"database.ini", config_section="holo_prod_offline")
    import numpy as np

    df = pd.DataFrame(
        {
            "sn_code": ["a", "b", "c", "b"],
            "sub_product_type": ["a", "b", np.nan, "b"],
            "future_n": [1, np.nan, 3, 4],
        }
    )
    df["pdate"] = "2024-01-02"
    po.insertAfterDelete(df, table="iot.zy_test", delete_condition="pdate='2024-01-01'")
