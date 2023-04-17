import sys
sys.path.append("../../")
from dataBaseConnect import *


def GetTurnPage(StartDate, EndDate):
    Sql = f"""
    select pdate, shunt_name, cast(pageindex as bigint) pageindex, count(1) request_num
    from (select user_id,
                 request_id,
                 pageindex,
                 event_time,
                 lead(pageindex, 1, 1) over (partition by user_id order by event_time) next_pageindex,
                 pdate,
                 shunt_name
          from (select user_id, request_id, pageindex, event_time, pdate, shunt_name
                from ads.ads_target_middle_table_guess_you_like_recommendation
                where pdate >= '{StartDate}'
                and pdate <= '{EndDate}'
                group by user_id, request_id, pageindex, event_time, pdate, shunt_name) as a) as a
    where pageindex >= next_pageindex
    group by pdate, shunt_name, pageindex
    """
    return Sql


def GetTargetData(StartDate, EndDate):
    Sql = f"""
    select pdate,
           shunt_name,
           cast(pageindex as bigint) as pageindex,
           count(menu_id)                                           exp_pv,
           count(distinct user_id)                                  exp_uv,
           count(clk_menu_id)                                       clk_pv,
           count(distinct clk_user_id)                              clk_uv,
           nvl(sum(clk_num), 0)                                  as clk_num
    from ads.ads_target_middle_table_guess_you_like_recommendation
    where pdate >= '{StartDate}'
                and pdate <= '{EndDate}'
    group by pdate, shunt_name, pageindex
    """
    return Sql


def GetTargetData2(StartDate, EndDate):
    Sql = f"""
    select pdate,
           shunt_name,
           count(menu_id)                                           exp_pv,
           count(distinct user_id)                                  exp_uv,
           count(clk_menu_id)                                       clk_pv,
           count(distinct clk_user_id)                              clk_uv,
           nvl(sum(clk_num), 0)                                  as clk_num
    from ads.ads_target_middle_table_guess_you_like_recommendation
    where pdate >= '{StartDate}'
                and pdate <= '{EndDate}'
    group by pdate, shunt_name
    """
    return Sql


if __name__ == "__main__":
    environment = sys.argv[1]
    startDate = sys.argv[2]
    endDate = sys.argv[3]

    # 数据库连接
    productHiveConfig = configReader("../../connection/config.ini").readDBInfo(
        "-".join(["hive-product", environment])
    )
    productHive = HiveObject(*productHiveConfig)
    mysqlAnalysisConfig = configReader("../../connection/config.ini").readDBInfo(
        "-".join(["mysql-analysis", environment])
    )
    mysqlAnalysis = mysqlObject(*mysqlAnalysisConfig)
    mysqlProductConfig = configReader("../../connection/config.ini").readDBInfo(
        "-".join(["mysql-product", environment])
    )
    mysqlProduct = mysqlObject(*mysqlProductConfig)

    # Sql执行
    Sql1 = GetTurnPage(StartDate=startDate, EndDate=endDate)
    Data1 = productHive.queryData(Sql1)
    mysqlAnalysis.writeDataAfterDelete(
        Data1,
        "guess_turn_page",
        deleteSql=f"pdate >= '{startDate}' and pdate <= '{endDate}'",
    )

    Sql2 = GetTargetData(StartDate=startDate, EndDate=endDate)
    Data2 = productHive.queryData(Sql2)
    mysqlAnalysis.writeDataAfterDelete(
        Data2,
        "guess_target_data",
        deleteSql=f"pdate >= '{startDate}' and pdate <= '{endDate}'",
    )

    Sql3 = GetTargetData2(StartDate=startDate, EndDate=endDate)
    Data3 = productHive.queryData(Sql3)
    mysqlAnalysis.writeDataAfterDelete(
        Data3,
        "guess_target_data2",
        deleteSql=f"pdate >= '{startDate}' and pdate <= '{endDate}'",
    )
