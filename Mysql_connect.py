import MySQLdb
# pip install mysql
import pandas as pd 

class sql_operate:
    def __init__(self, host="localhost", user="root", passwd="930428zy"):
        self.host = host
        self.user = user
        self.passwd = passwd

    def generate(self, sql):
        db = MySQLdb.connect(
            host=self.host,  # 主机名
            user=self.user,     # 用户名
            passwd=self.passwd,  # 密码
            db="")    # 数据库名称
        # 查询前，必须先获取游标
        cur = db.cursor()
        # 执行的都是原生SQL语句
        cur.execute(sql)
        result = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(result, columns=columns)
        db.close() 
        return df, result, columns 

sql1 = 'select * from world.city limit 100'

connect_sql = sql_operate()

connect_sql.generate(sql1)[0] 
