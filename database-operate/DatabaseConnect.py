import pandas as pd  
import numpy as np 
from pyhive import hive
import MySQLdb
import matplotlib.pyplot as plt  
import math 


class hive_operate:
    def __init__(
        self,
        host="101.133.208.140",
        port="10001",  # JBDC连接
        username="bdpro",
        password="wkjszdhd111",
        auth="LDAP",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth = auth

    def consor_(self):
        conn = hive.Connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            auth=self.auth,
        )
        consor = conn.cursor()
        return consor

    def generate_sql(self, sql):
        consor = self.consor_()
        consor.execute(sql)
        result = consor.fetchall()
        df = pd.DataFrame(list(result))
        columns = [i[0].split(".")[-1] for i in consor.description] 
        try:
            df.columns = columns 
        except:
            print("查询列表为空") 
        return df

    def close(self):
        self.consor_().close() 

class mysql_operate:
    def __init__(self, host="localhost", user="root", passwd="930428zy", db=''):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db

    def generate(self, sql):
        db = MySQLdb.connect(
            host=self.host,  # 主机名
            user=self.user,  # 用户名
            passwd=self.passwd,  # 密码
            db=self.db,
            charset='utf8'
        )  # 数据库名称
        # 查询前，必须先获取游标
        cur = db.cursor()
        # 执行的都是原生SQL语句
        cur.execute(sql)
        result = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(result, columns=columns)
        db.close()
        return df, result, columns 

class ssh_operate:
    """
    执行效率比较低，数据量比较少的适用
    暂时不用此方式匹配
    """

    def __init__(
        self,
        ip="101.132.152.113",
        port=52118,
        username="bdpro",
        passward="HMSYxL68s9NI5xPNeJGZ",
    ):
        self.ip = ip
        self.port = port
        self.username = username
        self.passward = passward

    def ssh_(self):
        trans = paramiko.Transport((self.ip, self.port))
        trans.connect(username=self.username, password=self.passward)
        ssh = paramiko.SSHClient()
        ssh._transport = trans
        return ssh

    def command_generate(self, command):
        ssh = self.ssh_()
        stdin, stdout1, stderr = ssh.exec_command(command)
        result1 = stdout1.read().decode().split("\n")[:-1]
        try:
            result2 = [eval("{" + re.split("{|}", i)[-2] + "}") for i in result1]
        except:
            result2 = result1
        ssh.close()
        return (stdin, stdout1, stderr, result2)

    def close(self):
        self.ssh_().close() 

class Visualization:

    @staticmethod 
    def multiline_plot(
        df,
        x_col:str,
        y_cols:list,
        title:str,
        y_cols_label=False,
        text = False,
        figsize=(20, 10),
        colors=["#1f78b4", "#2ba02d", "#9568bc", "#fd7e0c", "#d42727"],
        stack=False,
        xticks_step=2,
        draw_avg=False,
        alpha=0.8,
        y_grid=False,
        save_fig=False, 
        dpi=1000,
        save_fig_name_mark = '', 
        legend_loc = 'best'
    ):
        df.index = range(len(df))
        if not y_cols_label:
            y_cols_label = y_cols
        fig, ax = plt.subplots(figsize=figsize)
        if len(y_cols) <= len(colors):
            colors = colors[: len(y_cols)] 
        else:
            colors = [None] * len(y_cols)

        for col in df[y_cols]:
            if any([i in col for i in ['rate', 'lv', '占比', '率']]): 
                df[col] = df[col] * 100 
        for col, color in zip(y_cols, colors):
            ax.plot(df[col], marker="o", color=color, linewidth=2.5, label=col)  
            if draw_avg:
                ax.hlines(
                    df[col].mean(),
                    0,
                    len(df)-1,
                    colors=color,
                    alpha=alpha,
                    linestyle="--",
                    linewidth=3,
                ) 
        if text:
            for col in y_cols:  
                for i in range(len(df[col])):
                    ax.text(
                        i,
                        df[col].iloc[i] * 1.02 if any([j in col for j in ['rate', 'lv', '占比', '率']]) else df[col].iloc[i] * 1.09, 
                        str(round(df[col].iloc[i], 2)) + "%" if any([j in col for j in ['rate', 'lv', '占比', '率']]) else str(round(df[col].iloc[i], 2)),
                        ha="center",
                        fontsize=14,
                    )

  
        xticks = [i for i in range(df.shape[0]) if i % xticks_step == 0]
        ax.set_xticks(ticks=xticks)
        ax.set_xticklabels(
            labels=[
                str(i[5:]) if x_col == "pdate" else str(i)
                for i in df[x_col].iloc[xticks]
            ],
            rotation=40,
            font={"family": "Cambria", "size": "16"},
        )
        y_max = math.ceil(df[y_cols].max().max())  
        yticks = np.arange(0, y_max + 0.001, 0.5 if y_max <=8 else 5)
        ax.set_yticks(ticks=yticks)
        ax.set_yticklabels(
            labels=[str(i)[:3] for i in yticks],
            font={"family": "Cambria", "size": "16"},
        )

        ax.legend(
            frameon=True,
            prop={"family": "STKaiTi", "size": "17"},
            edgecolor="white",
            ncol=1,
            loc=legend_loc,
        )
        if y_grid:
            ax.grid(axis="y")
        ax.set_title(title, fontdict={"family": "STKaiTi", "size": "22"}, y=1.015)  

        if save_fig: 
            plt.savefig(f"{str(save_fig_name_mark)}.jpg", dpi=dpi, bbox_inches="tight", pad_inches=0.0)   
            print(f"{title}{str(save_fig_name_mark)}.jpg Have Been Saved!")  
        

