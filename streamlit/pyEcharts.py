import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyhive import hive
from scipy.interpolate import make_interp_spline
import numpy as np
import paramiko
import re
import os
from IPython.core.interactiveshell import InteractiveShell
from tqdm import tqdm
import warnings
from scipy import stats
import datetime as dt
from sklearn.preprocessing import MinMaxScaler

mm = MinMaxScaler()
InteractiveShell.ast_node_interactivity = "all"
warnings.filterwarnings("ignore")
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False


# In[3]:


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
        return df

    def close(self):
        self.consor_().close()


product_hive = hive_operate()


# In[4]:


now = dt.datetime.now()
last_week_start = str(now - dt.timedelta(days=now.weekday() + 14))[:10]
last_week_end = str(now - dt.timedelta(days=now.weekday() + 1))[:10]
print(f"开始时间为{last_week_start}， 结束时间为{last_week_end}。")


# In[5]:


df1 = product_hive.generate_sql(
    f"""
SELECT a.* 
FROM dwd.dwd_swan3_show as a
join dim.dim_tineco_user b 
    ON a.user_id = b.user_id  AND b.user_type = 2 and a.pdate between '{last_week_start}' and '{last_week_end}'
"""
)


# In[6]:


df1.columns = [
    "id",
    "app_version",
    "manufacturer",
    "os",
    "screen_title",
    "model",
    "os_version",
    "lib_version",
    "phone_type",
    "screen_name",
    "device_id",
    "event",
    "time",
    "user_id",
    "params",
    "lib",
    "device_version",
    "pdate",
]


# In[7]:


screen_name_dict = {
    "TinecoWindowTabBarViewController": "app首页",
    "TinecoLifeHomeActivity": "app首页",
    "MainActivity": "食万首页",
    "TKTHHomeViewController": "食万首页",
    "ActivityFoodProcessor": "食万首页",
}


# In[8]:


df1 = df1.assign(
    screen_name=lambda x: [
        i.split(".")[-1] if isinstance(i, str) else i for i in x["screen_name"]
    ],
    screen_rename=lambda x: [screen_name_dict.get(i, "--") for i in x["screen_name"]],
    weekday=lambda x: [
        {0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四", 4: "星期五", 5: "星期六", 6: "星期天",}.get(
            dt.datetime.strptime(i, "%Y-%m-%d").weekday()
        )
        for i in x["pdate"]
    ],
    week=lambda x: [
        "上周" if i < str(now - dt.timedelta(days=now.weekday() + 7))[:10] else "本周"
        for i in x["pdate"]
    ],
)


# ## data_a1  食万首页UV

# In[9]:


data_a1 = (
    df1.loc[lambda x: x["screen_rename"] == "食万首页"]
    .groupby(["pdate", "week", "weekday"], as_index=False)
    .agg({"user_id": pd.Series.nunique})
    .sort_values(by=["pdate"])
    .pivot(index="weekday", columns="week", values="user_id")
    .reset_index()
    .assign(
        weekday=lambda x: x["weekday"]
        .astype("category")
        .cat.reorder_categories(["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期天"])
    )
    .sort_values("weekday")
)


# ## data_a2  首页机型

# In[10]:


phones = [
    "OnePlus",
    "HONOR",
    "samsung",
    "OPPO",
    "iOS",
    "vivo",
    "Xiaomi",
    "HUAWEI",
    "Apple",
    "alps",
]


def get_phone(x):
    if x == "iOS":
        return "Apple"
    elif x == "alps":
        return "料理机"
    elif x in phones:
        return x
    else:
        return "other"


# In[11]:


data_a2 = (
    df1.loc[lambda x: x["screen_rename"].isin(["食万首页"])][
        ["manufacturer", "pdate", "user_id"]
    ]
    .drop_duplicates()
    .assign(
        week=lambda x: [
            "上周" if i < str(now - dt.timedelta(days=now.weekday() + 7))[:10] else "本周"
            for i in x["pdate"]
        ]
    )
    .groupby(["week", "manufacturer",], as_index=False)
    .agg({"user_id": pd.Series.nunique})
    .assign(phone_type=lambda x: [get_phone(i) for i in x["manufacturer"]])
    .groupby(["week", "phone_type"], as_index=False)
    .sum()
    .pivot(index="phone_type", columns="week", values="user_id")
    .reset_index()
    .sort_values(["本周"], ascending=False)
)


# In[12]:


cook_detail = {
    i.split("`")[1]: i.split("COMMENT ")[1].split("'")[1]
    for i in product_hive.generate_sql(
        """
show create table dwd.dwd_swan3_menu_cook_detils 
"""
    )
    .iloc[1:18, 0]
    .tolist()
}
cook_detail["pdate"] = ""


# ## data_a3 菜品烹饪数据

# In[13]:


df2 = product_hive.generate_sql(
    f"""
select *
from dwd.dwd_swan3_menu_cook_detils 
where user_type = 2 and pdate between '{last_week_start}' and '{last_week_end}'
"""
)


# In[14]:


df2.columns = cook_detail.keys()
df2 = df2[
    [
        "user_id",
        "menu_id",
        "menu_name",
        "start_time",
        "end_time",
        "is_end_cook",
        "is_fill_cook",
        "fill_time",
        "is_add_magazine",
        "is_app",
        "pdate",
    ]
]


# In[15]:


df2 = df2.assign(
    start_time=lambda x: pd.to_datetime(x["start_time"]),
    end_time=lambda x: pd.to_datetime(x["end_time"]),
    fill_time=lambda x: pd.to_datetime(x["fill_time"]),
    cook_time=lambda x: (x["end_time"] - x["start_time"]).dt.components.minutes.fillna(
        0
    ),
    fillcook_time=lambda x: (
        x["fill_time"] - x["end_time"]
    ).dt.components.minutes.fillna(0),
)


# In[16]:


data_a3 = df2.groupby("pdate", as_index=False).agg(
    {
        "user_id": [("烹饪用户数", pd.Series.nunique),],
        "menu_id": [("烹饪菜品数", "count"),],
        "is_end_cook": [("烹饪完成菜品数", lambda x: (x == 1).sum())],
        "is_fill_cook": [("补炊菜品数", lambda x: (x == 1).sum())],
        "is_add_magazine": [("使用料盒烹饪菜品数", lambda x: (x == 1).sum())],
    }
)

data_a3.columns = [i[1] if i[1] != "" else "pdate" for i in data_a3.columns]


# In[17]:


data_a3 = data_a3.assign(
    人均烹饪菜品数=lambda x: np.round(x["烹饪菜品数"] / x["烹饪用户数"], 2),
    菜品烹饪完成率=lambda x: np.round(x["烹饪完成菜品数"] / x["烹饪菜品数"], 3),
    菜品烹饪补炊率=lambda x: np.round(x["补炊菜品数"] / x["烹饪完成菜品数"], 3),
    菜品烹饪使用料盒率=lambda x: np.round(x["使用料盒烹饪菜品数"] / x["烹饪菜品数"], 3),
)


# In[18]:


data_a4 = df2.groupby("menu_name",).agg(
    {
        "user_id": [("烹饪过的人数", pd.Series.nunique)],
        "menu_id": [("烹饪过的次数", "count")],
        "is_end_cook": [
            ("完成烹饪的次数", lambda x: (x == 1).sum()),
            ("未完成烹饪的次数", lambda x: (x == 0).sum()),
        ],
        "is_fill_cook": [("补炊次数", lambda x: (x == 1).sum())],
        "is_add_magazine": [("使用料盒次数", lambda x: (x == 1.0).sum())],
        "cook_time": [("平均烹饪时间（不含补炊时间）", "mean")],
        "fillcook_time": [("平均补炊时间", "mean")],
    }
)
data_a4.columns = data_a4.columns.droplevel(0)
data_a4 = data_a4.reset_index().sort_values("烹饪过的人数", ascending=False)
data_a4 = data_a4.assign(
    补炊率=lambda x: np.round(x["补炊次数"] / x["烹饪过的次数"], 3),
    烹饪完成率=lambda x: np.round(x["完成烹饪的次数"] / x["烹饪过的次数"], 3),
).fillna(0)


# In[19]:


def round_up_5(x):
    t_ = np.round(x, decimals=-1)
    if t_ >= x:
        return t_
    else:
        return t_ + 5


def round_down_5(x):
    t_ = np.round(x, decimals=-1)
    if t_ < x:
        return t_
    elif t_ - 5 <= 0:
        return 0
    else:
        return min(t_ - 5, 5)


# In[20]:


data_a4 = data_a4.assign(
    平均烹饪时间_cut=lambda x: pd.cut(
        x["平均烹饪时间（不含补炊时间）"],
        bins=list(
            np.arange(
                round_down_5(x["平均烹饪时间（不含补炊时间）"].min()),
                round_up_5(x["平均烹饪时间（不含补炊时间）"].max()) + 1,
                5,
            )
        ),
        #         include_lowest=True,
    ),
    平均补炊时间_cut=lambda x: pd.cut(
        x["平均补炊时间"],
        bins=list(
            np.arange(
                round_down_5(x["平均补炊时间"].min()), round_up_5(x["平均补炊时间"].max()) + 1, 5,
            )
        ),
    ),
)


# # Draw

# In[21]:


import pyecharts.options as opts
from pyecharts.charts import (
    Line,
    Bar,
    Grid,
    WordCloud,
    Page,
    Liquid,
    EffectScatter,
    Scatter,
)
from pyecharts.faker import Faker
from pyecharts.globals import SymbolType
from pyecharts.commons.utils import JsCode
import math


# In[22]:


a1 = (
    Line()
    .add_xaxis(list(data_a1.weekday))
    .add_yaxis("上周", list(data_a1["上周"]), is_smooth=True)
    .add_yaxis("本周", list(data_a1["本周"]), is_smooth=True)
    .set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.1),
        label_opts=opts.LabelOpts(is_show=True),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="食万首页UV近两周变化",
            subtitle=f"{last_week_start} ~ {last_week_end}",
            #             pos_left=100,
        ),
        xaxis_opts=opts.AxisOpts(
            axistick_opts=opts.AxisTickOpts(is_align_with_label=True),
            is_scale=False,
            boundary_gap=False,
        ),
        legend_opts=opts.LegendOpts(is_show=True,),
    )
)

a2 = (
    Bar()
    .add_xaxis(list(data_a2.phone_type))
    .add_yaxis("上周 ", list(data_a2["上周"]),)
    .add_yaxis("本周 ", list(data_a2["本周"]),)
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="进入食万首页用户机型",
            subtitle=f"{last_week_start} ~ {last_week_end}",
            #             pos_top=235,
            #             pos_left=100,
        ),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(is_show=True,),
    )
)


# In[23]:


barline_overloap_dict = {
    "整体烹饪情况": ("烹饪用户数", "烹饪菜品数", "人均烹饪菜品数",),
    "烹饪完成情况": ("烹饪菜品数", "烹饪完成菜品数", "菜品烹饪完成率",),
    "烹饪补炊情况": ("烹饪完成菜品数", "补炊菜品数", "菜品烹饪补炊率",),
    "使用料盒烹饪情况": ("烹饪菜品数", "使用料盒烹饪菜品数", "菜品烹饪使用料盒率",),
}
barline_ls = []
for i, j in barline_overloap_dict.items():
    bar_ = (
        Bar()
        .add_xaxis(list(data_a3.pdate))
        .add_yaxis(j[0], list(data_a3[j[0]]))
        .add_yaxis(j[1], list(data_a3[j[1]]))
        .extend_axis(yaxis=opts.AxisOpts(axislabel_opts=opts.LabelOpts(), interval=20))
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title=i, subtitle=f"{last_week_start} ~ {last_week_end}"
            ),
        )
    )
    line_ = (
        Line()
        .add_xaxis(list(data_a3.pdate))
        .add_yaxis(j[2], list(data_a3[j[2]]), yaxis_index=1)
    )
    bar_.overlap(line_)
    barline_ls.append(bar_)


# In[24]:


wc_dict = {
    "未完成烹饪菜品词云": lambda x: x["is_end_cook"] == 0,
    "完成烹饪未补炊菜品词云": lambda x: (x["is_end_cook"] == 1) & (x["is_fill_cook"] == 0),
    "补炊菜品词云": lambda x: (x["is_fill_cook"] == 1),
}
wc_ls = []
for i, j in wc_dict.items():
    words_ = [
        (i.strip(), j)
        for i, j in df2.loc[j]
        .groupby("menu_name")["menu_id"]
        .count()
        .sort_values()
        .to_dict()
        .items()
    ]
    wc_ = (
        WordCloud()
        .add("", words_, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
        .set_global_opts(title_opts=opts.TitleOpts(title=i))
    )
    wc_ls.append(wc_)


# In[25]:


a4_t1 = np.round(
    data_a4.groupby(["平均烹饪时间_cut",])[["补炊率", "烹饪完成率"]]
    .mean()
    .reset_index()
    .assign(平均烹饪时间_cut=lambda x: x["平均烹饪时间_cut"].astype(str)),
    2,
)

a4_1 = (
    Line()
    .add_xaxis(list(a4_t1.平均烹饪时间_cut))
    .add_yaxis("补炊率", list(a4_t1.补炊率), is_smooth=True)
    .add_yaxis("烹饪完成率", list(a4_t1.烹饪完成率), is_smooth=True)
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="平均烹饪时间与补炊率、烹饪完成率的关系", subtitle=f"{last_week_start} ~ {last_week_end}"
        )
    )
)


# In[26]:


a4_t2 = np.round(
    data_a4.groupby(["平均补炊时间_cut",])[["补炊率", "烹饪完成率"]]
    .mean()
    .reset_index()
    .assign(平均补炊时间_cut=lambda x: x["平均补炊时间_cut"].astype(str)),
    2,
)

a4_2 = (
    Line()
    .add_xaxis(list(a4_t2.平均补炊时间_cut))
    .add_yaxis("补炊率", list(a4_t2.补炊率), is_smooth=True)
    .add_yaxis("烹饪完成率", list(a4_t2.烹饪完成率), is_smooth=True)
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="平均补炊时间与补炊率、烹饪完成率的关系", subtitle=f"{last_week_start} ~ {last_week_end}"
        )
    )
)


# In[60]:


# data_a4.sample(3)
cook_complate_rate = data_a4["完成烹饪的次数"].sum() / data_a4["烹饪过的次数"].sum()
fillcook_rate = data_a4["补炊次数"].sum() / data_a4["完成烹饪的次数"].sum()
magzine_rate = data_a4["使用料盒次数"].sum() / data_a4["烹饪过的次数"].sum()
cook_complate_rate, fillcook_rate, magzine_rate
lq1 = (
    Liquid()
    .add(r"完成烹饪的次数/烹饪总次数", [cook_complate_rate, 0.1], center=["15%", "50%"])
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="烹饪完成率",
            subtitle=f"{last_week_start} ~ {last_week_end}",
            pos_left="9%",
        )
    )
)
lq2 = (
    Liquid()
    .add(
        r"补炊次数/完成烹饪的次数",
        [fillcook_rate, 0.1],
        center=["45%", "50%"],
        #         is_outline_show=False,
        shape=SymbolType.DIAMOND,
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="补炊率",
            subtitle=f"{last_week_start} ~ {last_week_end}",
            pos_left="37%",
        )
    )
)
lq3 = (
    Liquid()
    .add(r"使用料盒次数/烹饪过的次数", [magzine_rate, 0.1], center=["75%", "50%"])
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="料盒使用率",
            subtitle=f"{last_week_start} ~ {last_week_end}",
            pos_left="65%",
        )
    )
)
grid1 = (
    Grid()
    .add(lq1, grid_opts=opts.GridOpts())
    .add(lq2, grid_opts=opts.GridOpts())
    .add(lq3, grid_opts=opts.GridOpts())
)
grid1.render_notebook()


# In[62]:


page = Page(layout=Page.DraggablePageLayout)
page.add(*([a1, a2] + wc_ls + barline_ls + [a4_1, a4_2, grid1]))
page.render("./output/demo0407.html")


# In[69]:


Page.save_resize_html(
    "./output/demo0407.html",
    cfg_file=r"C:\Users\yan.zy\Desktop\WorkStation\2022-04-01 三代可视化报告\output\chart_config.json",
    dest="./output/demo0407_2.html",
)
