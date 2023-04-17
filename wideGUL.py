import sys
sys.path.append("../../")
from dataBaseConnect import *




def GuessULike(startDate, endDate, env):
    def userTypeSql(environment, tableS=None):
        if environment.lower().endswith('st'):
            return "" 
        elif tableS:
            return f'and {tableS}.user_type = 2' 
        else:
            return 'and user_type = 2'
    Sql = f"""
    insert overwrite table ads.ads_target_middle_table_guess_you_like_recommendation partition (pdate)
    select case when a.shunt_name is null or a.shunt_name = '' then 'ALL' else a.shunt_name end as shunt_name,
           a.user_id,
           a.request_id,
           a.menu_id,
           a.position,
           a.pageindex,
           c.menu_name,
           c.cuisine,
           c.taste,
           c.menu_food,
           c.dishes,
           c.easy_degree,
           c.people,
           c.scenario,
           d.user_id                                                                            as clk_user_id,
           d.menu_id                                                                            as clk_menu_id,
           d.clk_num,
           substr(a.event_time, 1,19) as event_time,
           a.pdate
    from dwd.dwd_recommend_menu_expose as a
             join dim.dim_user as b
                  on a.user_id = b.user_id and a.pdate = b.pdate {userTypeSql(env, 'b')}
             left join dim.dim_menu as c on a.menu_id = c.menu_id and a.pdate = c.pdate and c.is_official = 1
             left join (
        select pdate, request_id, menu_id, distinct_id user_id, count(1) as clk_num
        from dwd.dwd_menu_event
        where pdate >= '{startDate}'
          and pdate <= '{endDate}'
          {userTypeSql(env)}
          and event_code = 'menu_click'
          and page_code = 'swan3_home'
          and module_code = 'rec_for_you_module'
        group by pdate, request_id, menu_id, distinct_id
    ) as d on a.request_id = d.request_id and a.pdate = d.pdate and a.menu_id = d.menu_id
    where a.biz_module = 1
      and a.pdate >= '{startDate}'
      and a.pdate <= '{endDate}'
      and a.item_type = 1
    """
    return Sql


if __name__ == "__main__":
    environment = sys.argv[1]
    startDate = sys.argv[2]
    endDate = sys.argv[3]
    
    # 数据库连接
    productHiveConfig = configReader().readDBInfo('-'.join(['hive-product', environment]))
    productHive = HiveObject(*productHiveConfig)


    # Sql执行
    sql1 = GuessULike(startDate, endDate, environment)
    productHive.alterData(sql1)

