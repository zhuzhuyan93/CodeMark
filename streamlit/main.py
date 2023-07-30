# coding:utf-8
import streamlit as st
import openai
import pandas as pd

openai.api_key = "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"


st.header('1、数据校验SQL工具')

st.subheader("1.1 HiveSql 正则表达式")

c1, c2 = st.columns(2)
with c1:
    t1 = st.text_area("源文本", value=r"https:\\/\\/qas-gl-CN-api.tineco.com\\/v1\\/private\\/CN\\/ZH_CN\\/B9C916E6-6030-495A-B9FD-79EAE3F75345\\/global_a\\/1.2.25\\/appStore\\/2\\/food\\/rookie\\/collect\\/question")
with c2:
    t2 = st.text_area("需求", "获取B9C916E6-6030-495A-B9FD-79EAE3F75345")
m1 = [
    {"role": "user",
     "content": f"在HiveSql中，如何从{t1}获取{t2}"
     }
]
if st.button("RUN", key=894732):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=m1
    )
    st.write(completion.choices[0]['message']['content'])


st.subheader("1.2 HiveSql 优化")

t3 = st.text_area("输入SQL", """
    SELECT substr(a.pdate, 1, 7) m_month,
COUNT(*),COUNT(DISTINCT get_json_object(a.json,"$.device_id") ),CAST(substr(last_day(a.pdate), 9, 2) as int)
FROM ods.ods_swan3_original_point_flat a
inner join dim.dim_user b
on get_json_object(a.json,"$.distinct_id") = b.user_id and b.user_type =2 and a.pdate = b.pdate
WHERE a.pdate>='2022-11-01' and a.pdate<='2023-03-06'
and get_json_object(a.json,"$.event") = 'AppStart' and get_json_object(a.json,"$.properties.os") = 'ChiereOne'
GROUP BY substr(a.pdate, 1, 7) ;
    """)
m2 = [
    {"role": "user",
     "content": f"优化以下的SQL，提升运行速度： {t3}"
     }
]
if st.button("RUN", key=2):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=m2
    )
    st.write(completion.choices[0]['message']['content'])


# c1, c2 = st.columns(2)
st.subheader("2、数据探索和可视化脚本生成")
data1 = st.file_uploader("上传数据文件")
if data1:
    df1 = pd.read_excel(data1)
    # st.write(df1)

    messages2 = [
        {"role": "user",
            "content": f"现在有数据集 {df1}. 请详细分析这批数据，并给出分析代码"}
    ]

c1, c2, c3 = st.columns(3)
if c1.button("Show Data", key=222):
    st.dataframe(df1)


if c2.button("分析及可视化", key=43243):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages2
    )
    result = completion.choices[0]['message']
    st.write(result['content'])
    messages2.append({
        "role": "assistant",
        "content": result['content']
    })
    attach_info = st.text_area("改善建议", "")
    
    if st.button("ReRun", key=563455):
        completion2 = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=messages2
        )
        st.write(completion2.choices[0]['message']['content'])

if c3.button("时序预测"):
    messages4 = [
        {"role": "user",
            "content": f"现在有数据集 {df1}. 预测未来七天的值"}
    ]
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages4
    )
    result = completion.choices[0]['message']
    st.write(result['content'])