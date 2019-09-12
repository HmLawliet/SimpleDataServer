# http get请求 携带参数说明：
# 扫描
示例:
    url = 47.103.36.8:80/scan?brand=121&start_time=2019-08-01&end_time=2019-08-02&provinces=12,13,14&dealers=123,124&productions=236,245

必选参数：
    brand   start_time      end_time
可选参数：
    provinces   dealers     productions

返回结果列表顺序说明:
    device_static_id : 第 1 列 静态id
    create_time : 第 2 列 创建时间
    distributor_id : 第 3 列 渠道商id
    distributor_desc : 第 4 列 渠道商描述
    product_id : 第 5 列 产品id
    product_desc : 第 6 列 产品描述
    province_code : 第 7 列 省份编码
    province_desc : 第 8 列 身份描述


# 验伪
示例:
    url = 47.103.36.8:80/antifake?brand=121&start_time=2019-08-01&end_time=2019-08-02&provinces=12,13,14&dealers=123,124&productions=236,245

必选参数：
    brand   start_time      end_time
可选参数：
    provinces   dealers     productions

返回结果列表顺序说明:
    device_static_id : 第 1 列 静态id
    create_time : 第 2 列 创建时间
    distributor_id : 第 3 列 渠道商id
    distributor_desc : 第 4 列 渠道商描述
    product_id : 第 5 列 产品id
    product_desc : 第 6 列 产品描述
    province_code : 第 7 列 省份编码
    province_desc : 第 8 列 身份描述


# 窜货
示例:
    url = 47.103.36.8:80/cluster?brand=121&start_time=2019-08-01&end_time=2019-08-02&provinces=12,13,14&dealers=123,124&productions=236,245&limit=10&isAsc=True

必选参数：
    brand   start_time      end_time
可选参数：
    provinces   dealers     productions     limit(展示的数量)   isAsc(True或者true为递增,False或者false为递减)

返回结果列表顺序说明:
    distributor_desc : 第 1 列 渠道商描述
    count : 第 2 列 串货数量统计

# 发货_产品维度
示例:
    url = 47.103.36.8:80/delivery_product?brand=121&start_time=2019-08-01&end_time=2019-08-02&dealers=123,124&productions=236,245

必选参数：
    brand   start_time      end_time
可选参数：
    dealers     productions

返回结果列表顺序说明:
    product_desc : 第 1 列 产品描述
    create_time : 第 2 列 创建时间
    count : 第 3 列 统计数量


# 发货_渠道商维度
示例:
    url = 47.103.36.8:80/delivery_distributor?brand=121&start_time=2019-08-01&end_time=2019-08-02&provinces=12,13,14&dealers=123,124&productions=236,245

必选参数：
    brand   start_time      end_time
可选参数：
    provinces   dealers     productions

返回结果列表顺序说明:
    distributor_desc : 第 1 列 渠道商描述
    create_time : 第 2 列 创建时间
    count : 第 3 列 统计数量


# 普通营销红包
示例:
    url = 47.103.36.8:80/ordred?brand=121&start_time=2019-08-01&end_time=2019-08-02&activity=12,13,14&dealers=123,124&productions=236,245

必选参数：
    brand   start_time      end_time
可选参数：
    activity   dealers     productions

返回结果列表顺序说明:
    create_time : 第 1 列 创建时间
    distributor_id : 第 2 列 渠道商id
    distributor_desc : 第 3 列 渠道商描述
    product_id : 第 4 列 产品id 
    product_desc : 第 5 列 产品描述
    province_desc : 第 6 列 省份描述
    city_desc : 第 7 列 城市描述
    activity_id : 第 8 列 活动id
    activity_desc : 第 9 列 活动描述
    label_code : 第 10 列 标签号
    mn_openid : 第 11 列 微信id
    phone : 第 12 列 手机号码
    phone_model : 第 13 列 手机型号
    operating_system : 第 14 列 操作系统
    gender : 第 15 列 性别
    send_status : 第 16 列 红包接受状态
    receive_money : 第 17 列 红包金额



# 下拉框条件
示例:
    url = 47.103.36.8:80/conditions?brand=121
必选参数:
    brand

