from pyhive import hive
import pandas as pd


class HiveObject:
    def __init__(self, host="101.133.208.140", port="10000", username="bdpro", password="wkjszdhd111") -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth = "LDAP"

    def hiveConIns(self):
        connection = hive.Connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            auth=self.auth,
            configuration={
                "hive.exec.dynamic.partition.mode": "nonstrict"},  # 插入分区表配置
        )
        return connection

    def queryData(self, sql, args=None):
        """
        查询
        """
        conn = self.hiveConIns()
        cur = conn.cursor()
        cur.execute(sql, args)
        alldata = cur.fetchall()
        cols = [i[0].split(".")[-1] for i in cur.description]
        cur.close()
        conn.close()
        data = pd.DataFrame(data=alldata, columns=cols)
        return data

    def alterData(self, sql, args=None):
        """
        更改：插入分区表，删除操作等
        """
        conn = self.hiveConIns()
        cur = conn.cursor()
        try:
            # 做一个粗暴的判断当args是list时就进行批量插入
            if isinstance(args, list):
                # executemany(sql,args)方法args支持tuple或者list类型
                cur.executemany(sql, args)
            else:
                # execute(sql,args)方法args支持string,tuple,list,dict
                cur.execute(sql, args)
            conn.commit()
        except Exception as e:
            # 因为hive不支持事务，因此虽然提供了rollback()但是是没用的
            # conn.rollback()
            print(e)
        finally:
            cur.close()
            conn.close()


if __name__ == "__main__":
    product_hive = HiveObject()
    df = product_hive.queryData("""
    select * from dwd.dwd_menu_event limit 10
    """)
    print(df)
