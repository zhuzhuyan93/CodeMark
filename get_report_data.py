import os, requests
import pandas as pd
from time import sleep
from datetime import datetime, date, timedelta
from openpyxl import load_workbook

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 100)
pd.set_option("display.width", 1000)

time = int(date.strftime(datetime.now(), "%H"))
if time >= 12:
    report_date = date.strftime(date.today() + timedelta(days=0), "%Y-%m-%d")
else:
    report_date = date.strftime(date.today() + timedelta(days=-1), "%Y-%m-%d")

SAVE_DIR = os.path.split(os.path.realpath(__file__))[0].replace("codes", "reports\\")
print("SAVE_DIR:", SAVE_DIR)


cols = ["日期", "产品", "运营商", "高龄", "标签名", "发送量", "成功量", "实际成功", "发送失败", "IP", 
        "总注册量", "总下单量", "注册均价", "下单均价", "总成本", "总收入", "总毛利", "短信类型",] 

cols_wh = ["日期", "产品", "运营商", "高龄", "标签名", "外呼量", "呼通量", "发送量", "成功量", 
           "实际成功", "发送失败", "IP", "总注册量", "总下单量", "总成本", "总收入", "总毛利", "短信类型",]

cols_wh_xd = ["日期", "产品", "运营商", "标签名", "外呼量", "呼通量", "发送量", "成功量", "实际成功", "发送失败", "IP", 
              "总注册量", "授信申请量", "授信成功量", "总成本", "总收入", "总毛利", "短信类型", "渠道号",]

cols_pd_zaxd = ["日期", "产品", "运营商", "标签名", "发送量", "成功量", "实际成功", "发送失败", "IP", 
                "总注册量", "授信申请量", "授信成功量", "总成本", "总收入", "总毛利", "短信类型",]

#保险普短
products = [
    "暖哇_众安保险赠险_保泰", 
    "i云保_京东健康赠险_保泰",
    "泰康魔方_直投_保泰" ,
    "暖哇_众安保险赠险_泰康魔方_保泰",
    "暖哇_众安保险赠险_泰康魔方_超信_保泰", 
    "众安账户安全险_众安优保_保泰" ,
    "i云保众安赠险_iyb众安魔方_保泰",
    "暖哇众安赠险_众安优保_保泰",
    "暖哇众安赠险_门诊_均分_保泰",
    "众安优保重疾赠险_转众安优保魔方_保泰",
    "众安优保交通赠险_转众安优保魔方_保泰",
]
#保险外呼
products_wh = ["京东健康_保通魔方", "暖哇_众安保险赠险_保通魔方","暖哇_众安保险赠险_暖哇魔方_外呼","现代魔方_直投_外呼"] 
#小贷外呼
products_wh_xd = ["众安小贷", "众安小贷24"] 
#众安小贷普短
products_pd_zaxd = ["众安小贷_保泰"] 


dates = [report_date]
# dates = ['2021-12-01','2021-12-02']
dates = [date.strftime(datetime(2022, 2, 1) + timedelta(days=i), '%Y-%m-%d') for i in range(10)]   
print("dates:", dates) 

token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4dXFpYW4iLCJ1c2VySWQiOiIyMyIsIm5hbWUiOiJ4dXFpYW4iLCJyb2xlIjoibWFya2V0aW5nLXJlcG9ydCIsImV4cCI6MTY0Njk1ODg5MX0.VBkDELtf58-mQeR-rB9JYWN8Tpj_u5j8qv1tVcjZF4ZPakcz9GSR9IYyyjAH6lXlKlGdiQ7613TlCKouGDQfc4WJC9hqUxLfNnpxFmpeSEFF6x-7VLsmjEoeNxkmL0rWGYDW0O4Vp84pVWInz8SrtsGaKt583pRqSuvWZZjeV_o"
headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
    'token': token,
    'Origin': 'http://prod.bountech.com',
    'Referer': ' ',
    'Accept-Language': 'zh,zh-CN;q=0.9',
}  

def get_operator(x):
    if "cucc" in x:
        return "联通"
    elif "cmcc" in x:
        return "移动"
    else:
        return "电信" 

def get_report_data(dates, products=products, cols=cols):
    Referer = 'http://prod.bountech.com/marketfront/channelreport/insurance/labelStatistics'
    headers['Referer'] = Referer

    config_dicts = [{"product": product, "beginDate": date,"endDate": date, "high_age": high_age}
                    for product in products
                    for date in dates
                    for high_age in [0]] 
    
    report_data_list = []
    for config_dict in config_dicts:
        data = '{{"product":"{product}","beginDate":"{beginDate}","endDate":"{endDate}","operator":"","label":"","isContainHighAge": {high_age}}}'.format(
            **config_dict
        ).encode("utf-8")
        response = requests.post(
            "http://prod.bountech.com/marketprod/InsuranceStatistics/report/rules-report",
            headers=headers,
            data=data,
            verify=False,
        )

        if response.json().get("data", -1) != -1:
            report_data_temp = pd.DataFrame(
                response.json().get("data").get("list"))
            report_data_temp = (
                report_data_temp.rename(
                    columns={"label": "标签名", "sendAmount": "发送量", "successAmount": "成功量", "realSuccessAmount": "实际成功", 
                             "ip": "IP", "totalRegisters": "总注册量", "totalOrders": "总下单量", "totalCost": "总成本", "totalIncome": "总收入", 
                             "totalMargin": "总毛利", "averageRegisterIncome": "注册均价", "averageOrderIncome": "下单均价", "operator": "运营商", 
                             "product": "产品",}
                )
                .assign(
                    日期=config_dict["beginDate"],
                    高龄={1: "含高龄", 0: "不含高龄"}[config_dict["high_age"]],
                    产品=config_dict["product"],
                    运营商=lambda x: x["标签名"].apply(get_operator),
                )
                .assign(发送失败=lambda x: x["发送量"] - x["成功量"])
                .assign(
                    短信类型=report_data_temp.apply(
                        lambda x: "普短" if "CX" not in x["label"] else "彩信", axis=1
                    )
                )
                .loc[lambda x: x["标签名"] != "合计", cols]
            )

            report_data_list.append(report_data_temp)
            sleep(0.1)
    try:
        report_data = pd.concat(report_data_list) 
    except: 
        report_data = pd.DataFrame(columns = cols)
    return report_data  

def get_outbound_report_data(dates, products=products_wh, cols=cols_wh):
    Referer = 'http://prod.bountech.com/marketfront/channelreport/insurance/outboundLabelStatistics'
    headers['Referer'] = Referer

    config_dicts = [
        {"product": product, "beginDate": date,
            "endDate": date, "high_age": high_age}
        for product in products
        for date in dates
        for high_age in [0] 
    ]
    report_data_list = []

    for config_dict in config_dicts:
        data = '{{"product":"{product}","beginDate":"{beginDate}","endDate":"{endDate}","operator":"","label":"","isContainHighAge":{high_age}}}'.format(
            **config_dict
        ).encode("utf-8")

        response = requests.post(
            'http://prod.bountech.com/marketprod/InsuranceStatistics/report-outbound/rules-report',
            headers=headers,
            data=data,
            verify=False,
        )
        if response.json().get("data", -1) != -1:
            report_data_temp = pd.DataFrame(
                response.json().get("data").get("list")) 
            report_data_temp = (
                report_data_temp.rename(
                    columns={"label": "标签名", "sendAmount": "发送量", "successAmount": "成功量", "realSuccessAmount": "实际成功",
                             "ip": "IP", "totalRegisters": "总注册量", "totalOrders": "总下单量", "totalCost": "总成本", "totalIncome": "总收入",
                             "totalMargin": "总毛利", "operator": "运营商", "product": "产品", "outboundCount": "外呼量", "outboundCalledCount": "呼通量", }
                ) 
                .assign(产品=config_dict.get("product"))
                .assign(
                    日期=config_dict["beginDate"],
                    高龄={1: "含高龄", 0: "不含高龄"}[config_dict["high_age"]],
                )
                .assign(发送失败=lambda x: x["发送量"] - x["成功量"])
                .assign(
                    短信类型=report_data_temp.apply(
                        lambda x: "普短" if "CX" not in x["label"] else "彩信", axis=1
                    )
                )
                .loc[lambda x: x["标签名"] != "合计", cols]
                .assign(
                    运营商=lambda x: x["标签名"].apply(get_operator),
                    标签名=lambda x: ["_".join(ele.split("_")[:-1])
                                   for ele in x["标签名"]],
                )
            )

            report_data_temp = report_data_temp.groupby(
                ["日期", "产品", "运营商", "高龄", "短信类型", "标签名"], as_index=False
            ).sum()[cols]
            report_data_list.append(report_data_temp)
            sleep(0.1)
    try:
        report_data = pd.concat(report_data_list) 
    except: 
        report_data = pd.DataFrame(columns = cols) 
    return report_data 

def get_xdoutbound_report_data(dates, products=products_wh_xd, cols=cols_wh_xd):
    no_channels = [str(int(i)) for i in range(1635154548027, 1635154548077)] + [str(int(i)) for i in range(1634178285651, 1634178285661)]
        
    Referer = "http://prod.bountech.com/marketfront/channelreport/other/outboundXDLabelStatistics"
    headers['Referer'] = Referer
    
    config_dicts = [
        {"product": product, "beginDate": date, "endDate": date}
        for product in products
        for date in dates
    ]

    report_data_list = []

    for config_dict in config_dicts:
        data = '{{"product":"{product}","beginDate":"{beginDate}","endDate":"{endDate}","operator":"","label":""}}'.format(**config_dict).encode("utf-8")

        response = requests.post(
            "http://prod.bountech.com/marketprod/finance-statistics/report-other/zaxd-rules-outbound-report",
            headers=headers,
            data=data,
            verify=False,
        )

        if response.json().get("data", -1) != -1:
            report_data_temp = pd.DataFrame(
                response.json().get("data").get("list"))
            report_data_temp = (
                report_data_temp.rename(
                    columns={"label": "标签名", "sendCount": "发送量", "successCount": "成功量", "realSuccess": "实际成功", "ip": "IP", 
                             "totalRegisterAmount": "总注册量", "totalOrderApplyAmount": "授信申请量", "totalOrderAmount": "授信成功量", 
                             "totalCost": "总成本", "totalIncome": "总收入", "totalMargin": "总毛利", "operator": "运营商", "product": "产品", 
                             "outboundCount": "外呼量", "outboundCalledCount": "呼通量",}
                )
                .assign(产品=config_dict.get("product"))
                .assign(日期=config_dict["beginDate"])
                .assign(发送失败=lambda x: x["发送量"] - x["成功量"])
                .assign(
                    短信类型=report_data_temp.apply(
                        lambda x: "普短" if "CX" not in x["label"] else "彩信", axis=1
                    )
                )
                .loc[lambda x: x["标签名"] != "合计"]
                .loc[lambda x: x["标签名"].str.len() != 13]
                .assign(渠道号=lambda x: [ele.split('_')[-1] for ele in x['标签名']])
                .loc[lambda x:~(x['渠道号'].isin(no_channels))]
                .assign(
                    标签名=lambda x: ["_".join(ele.split("_")[:-1])
                                   for ele in x["标签名"]]
                )
            )

            report_data_temp = report_data_temp.groupby(
                ["日期", "产品", "运营商", "短信类型", "标签名", '渠道号'], as_index=False
            ).sum()[cols]
            report_data_list.append(report_data_temp)
            sleep(0.1)

    report_data = pd.concat(report_data_list)
    return report_data 

def get_report_data_zaxd_pd(dates, products=products_pd_zaxd, cols=cols_pd_zaxd):
    Referer = "http://prod.bountech.com/marketfront/channelreport/other/XDLabelStatistics"
    headers['Referer'] = Referer
    
    config_dicts = [
        {"product": product, "beginDate": date, "endDate": date}
        for product in products
        for date in dates
    ]

    report_data_list = []

    for config_dict in config_dicts:
        data = '{{"product":"{product}","beginDate":"{beginDate}","endDate":"{endDate}","operator":"","label":""}}'.format(
            **config_dict
        ).encode("utf-8")

        response = requests.post(
            "http://prod.bountech.com/marketprod/finance-statistics/report-other/zaxd-rules-report",
            headers=headers,
            data=data,
            verify=False,
        )

        if response.json().get("data", -1) != -1:
            report_data_temp = pd.DataFrame(
                response.json().get("data").get("list"))
            report_data_temp = (
                report_data_temp.rename(
                    columns={ "label": "标签名", "sendCount": "发送量", "successCount": "成功量", "realSuccess": "实际成功", "ip": "IP", 
                             "totalRegisterAmount": "总注册量", "totalOrderApplyAmount": "授信申请量", "totalOrderAmount": "授信成功量", 
                             "totalCost": "总成本", "totalIncome": "总收入", "totalMargin": "总毛利", "operator": "运营商", "product": "产品",}
                )
                .assign(产品=config_dict.get("product"))
                .assign(日期=config_dict["beginDate"])
                .assign(发送失败=lambda x: x["发送量"] - x["成功量"])
                .assign(
                    短信类型=report_data_temp.apply(
                        lambda x: "普短" if "CX" not in x["label"] else "彩信", axis=1
                    )
                )
                .loc[lambda x: x["标签名"] != "合计", cols]
                .loc[lambda x: x["标签名"].str.len() != 13]
            )

            report_data_temp = report_data_temp.groupby(
                ["日期", "产品", "运营商", "短信类型", "标签名"], as_index=False
            ).sum()[cols]
            report_data_list.append(report_data_temp)
            sleep(0.1)

    report_data = pd.concat(report_data_list)
    return report_data 

def report_save(get_report_data, file_name='summary_report_bt.xlsx', cols=cols):
    def update_excel(data, excel_file=SAVE_DIR + file_name, sheet_name='summary'):
        book = load_workbook(excel_file)
        writer = pd.ExcelWriter(excel_file, engine='openpyxl')
        writer.book = book

        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        data.to_excel(writer, sheet_name, index=False)
        writer.save()

    if file_name not in os.listdir(SAVE_DIR):
        old_report_data = pd.DataFrame(columns=cols)
        old_report_data.to_excel(SAVE_DIR + file_name,
                                 index=False, sheet_name='summary')

    old_report_data = pd.read_excel(SAVE_DIR + file_name)\
        .loc[lambda x: [all(report_date != date for date in dates) for report_date in x['日期']]]
    new_report_data = get_report_data(dates)
    report_data = old_report_data.append(new_report_data)
    update_excel(report_data) 

if __name__ == '__main__':
    report_save(get_outbound_report_data,file_name='summary_report_outbound.xlsx', cols=cols_wh) 
    report_save(get_xdoutbound_report_data,file_name='summary_report_outbound_xd.xlsx', cols=cols_wh_xd) 
    report_save(get_report_data, file_name='summary_report_bt.xlsx', cols=cols)
    report_save(get_report_data_zaxd_pd, file_name='summary_report_zaxd_pd.xlsx', cols=cols_pd_zaxd) 
    
