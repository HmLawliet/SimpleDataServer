from database import CON_POOL,QUE_POOL
import asyncio
import pandas as pd


LOOP = asyncio.get_event_loop()

async def query_sql(sql,pool=QUE_POOL):
    '''异步查询sql'''
    async with pool as pl:
        try:
            await pl.execute(sql)
        except Exception:
            return None
        # col = [i[0] for i in pl.description]  # 列名
        result = pl.fetchall()
        return result


async def task(method,**kwargs):
    '''创建任务'''
    tk = LOOP.create_task(method(**kwargs))
    res = await tk
    return res 


async def com_parts_sql(template_sql,area,dealers,product):
    '''拼接sql中公共的部分'''
    if area:
        template_sql += ' and province_code in ({}) '.format(','.join(area.split(','))) 
    if dealers:
        template_sql += ' and distributor_id in ({}) '.format(','.join(dealers.split(',')))
    if product:
        template_sql += ' and product_id in ({}) '.format(','.join(product.split(','))) 
    return template_sql


async def scan_sql(brand,start_time,end_time,provinces=None,dealers=None,productions=None):
    '''生成扫描的sql'''
    sql = 'select device_static_id,create_time,distributor_id,distributor_desc,product_id,product_desc,province_code,province_desc\
         from statistics_antifake where org_id = {} and create_time >= "{}" and create_time <= "{}"'.format(brand,start_time,end_time)
    sql = await com_parts_sql(sql,provinces,dealers,productions)
    return sql


async def scan_query(brand,start_time,end_time,provinces=None,dealers=None,productions=None):
    '''查询扫描'''
    sql = await task(scan_sql,brand=brand,start_time=start_time,end_time=end_time,provinces=provinces,dealers=dealers,productions=productions)
    res = await task(query_sql,sql=sql)
    return {'scan':res}


async def antifake_sql(brand,start_time,end_time,provinces=None,dealers=None,productions=None):
    '''生成验伪的sql'''
    sql = 'select device_static_id,create_time,distributor_id,distributor_desc,product_id,product_desc,province_code,province_desc\
         from statistics_antifake where org_id = {} and create_time >= "{}" and create_time <= "{}"'.format(brand,start_time,end_time)
    sql = await com_parts_sql(sql,provinces,dealers,productions)
    return sql


async def antifake_query(brand,start_time,end_time,provinces=None,dealers=None,productions=None):
    '''查询验伪'''
    sql = await task(antifake_sql,brand=brand,start_time=start_time,end_time=end_time,provinces=provinces,dealers=dealers,productions=productions)
    res = await task(query_sql,sql=sql) 
    return {'antifake':res}


async def cluster_sql(brand,start_time,end_time,provinces=None,dealers=None,productions=None,limit=None,isAsc=False):
    '''生成窜货的sql'''
    sql = 'select distributor_desc,cast(sum(count) as signed) as count from statistics_cluster\
         where org_id = {} and create_time >= "{}" and create_time <="{}"'.format(brand,start_time,end_time)
    sql = await com_parts_sql(sql,provinces,dealers,productions)
    sql += ' group by distributor_desc '
    order = 'asc' if isAsc else 'desc'
    sql += ' order by count {} '.format(order) 
    if limit:
        sql += ' limit {} '.format(limit)
    return sql


async def cluster_query(brand,start_time,end_time,provinces=None,dealers=None,productions=None,limit=None,isAsc=False):
    '''窜货查询'''
    sql = await task(cluster_sql,brand=brand,start_time=start_time,end_time=end_time,provinces=provinces,dealers=dealers,productions=productions,limit=limit,isAsc=isAsc)
    res = await task(query_sql,sql=sql)
    return {'cluster':res}



async def delivery_sql(brand,start_time,end_time,dealers=None,productions=None):
    '''生成发货的sql'''
    sql = ' select {}{}create_time,cast(sum(count) as signed) as count from statistics_delivery where org_id = {}\
         and create_time >= "{}" and create_time < "{}" '.format('distributor_desc,' if dealers else '',
         'product_desc,' if productions else '',brand,start_time,end_time)
    sql = await com_parts_sql(sql,None,dealers,productions)
    sql += ' group by {}{}create_time '.format('distributor_desc,' if dealers else '','product_desc,' if productions else '',)
    return sql 


async def delivery_query(brand,start_time,end_time,dealers=None,productions=None):
    '''查询发货'''
    sql = await task(delivery_sql,brand=brand,start_time=start_time,end_time=end_time,dealers=dealers,productions=productions)
    res = await task(query_sql,sql=sql)
    return {'delivery':res}


async def ordred_sql(brand,start_time,end_time,activity=None,dealers=None,productions=None):
    '''生成红包sql'''
    sql = 'select create_time,distributor_id,distributor_desc,product_id,product_desc,\
        province_desc,city_desc,activity_id,activity_desc,label_code,mn_openid,phone,phone_model,operating_system,gender,\
            send_status,receive_money from statistics_ordred where org_id = {} and create_time >= "{}" and create_time <= "{}"'.format(brand,start_time,end_time)
    if activity:
        sql += ' and activity_id in ({})'.format(','.join(activity.split(',')))
    sql = await com_parts_sql(sql,None,dealers,productions)
    return sql


async def ordred_query(brand,start_time,end_time,activity=None,dealers=None,productions=None):
    '''查询红包'''
    sql = await task(ordred_sql,brand=brand,start_time=start_time,end_time=end_time,activity=activity,dealers=dealers,productions=productions)
    res = await task(query_sql,sql=sql)
    return {'ordred':res} 


async def conditions_sql(brand):
    '''生成下拉框绑定条件的sql'''
    sql_dict = {}
    sql_dict['provinces'] = 'select code,name from ac_district where check_result = 1 and status = 1 and level = "province"'
    sql_dict['dealers'] = 'select distinct id,name from ac_distributor where org_id = {}'.format(brand)
    sql_dict['productions'] ='select distinct id,concat(name,"-",standard) from ac_product where org_id = {}'.format(brand)
    return sql_dict


async def conditions_query(brand):
    '''查询下拉框绑定条件'''
    sqls = await task(conditions_sql,brand=brand)
    res_dict = {}
    for key,sql in sqls.items():
        res_dict[key] = await task(query_sql,sql=sql,pool=CON_POOL)
    return res_dict




if __name__ == '__main__':
    res = scan_query(121,'2019-08-01','2019-08-02')
    print(res) 
    

