import tornado.ioloop
from tornado.web import RequestHandler
from tornado.httpserver import HTTPServer
from query import scan_query,delivery_query,conditions_query,ordred_query,antifake_query,cluster_query
import json
from hashlib import md5
from cache import Cache

ACCESS_IP = ('127.0.0.1','*******','*******') 


async def parse_args(args):
    brand = args['brand'] if 'brand' in args.keys() else None
    start_time = args['start_time'] if 'start_time' in args.keys() else None
    end_time = args['end_time'] if 'end_time' in args.keys() else None
    provinces = args['provinces'] if 'provinces' in args.keys() else None
    dealers = args['dealers'] if 'dealers' in args.keys() else None
    productions = args['productions'] if 'productions' in args.keys() else None
    activity = args['activity'] if 'activity' in args.keys() else None
    limit = args['limit'] if 'limit' in args.keys() else None
    isAsc = True if 'isAsc' in args.keys() and args['isAsc'] in ('true','True') else False
    flag = True
    if not brand or not start_time or not end_time:
        flag = False
    return flag,brand,start_time,end_time,provinces,dealers,productions,activity,limit,isAsc


async def encryption(args):
    args = (str(item) for item in args)
    str_key = ''.join(list(args))
    md5_key = md5(str_key.encode(encoding='utf-8')).hexdigest()
    return md5_key


class ScanHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        flag,brand,start_time,end_time,provinces,dealers,productions,*_ = await parse_args(args)
        if not flag:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('scan',brand, start_time, end_time, provinces, dealers, productions))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return 
        result = await scan_query(brand, start_time, end_time, provinces, dealers, productions)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()


class AntifakeHandler(RequestHandler):
    
    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        flag,brand,start_time,end_time,provinces,dealers,productions,*_ = await parse_args(args)
        if not flag:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('antifake',brand, start_time, end_time, provinces, dealers, productions))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return
        result = await antifake_query(brand, start_time, end_time, provinces, dealers, productions)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()


class ClusterHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        flag,brand,start_time,end_time,provinces,dealers,productions,_,limit,isAsc = await parse_args(args)
        if not flag:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('cluster',brand, start_time, end_time, provinces, dealers, productions,limit,isAsc))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return
        result = await cluster_query(brand, start_time, end_time, provinces, dealers, productions,limit,isAsc)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()



class DeliveryHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        flag,brand,start_time,end_time,_,dealers,productions,*_ = await parse_args(args)
        if not flag:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('delivery',brand, start_time, end_time, dealers, productions))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return
        result = await delivery_query(brand, start_time, end_time, dealers, productions)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()


class OrdRedHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        flag,brand,start_time,end_time,_,dealers,productions,activity,*_ = await parse_args(args)
        if not flag:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('ordred',brand, start_time, end_time, activity, dealers, productions))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return
        result = await ordred_query(brand, start_time, end_time, activity, dealers, productions)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()


class ConditionsHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        args = self.request.arguments
        args = {key: value[0].decode() for key, value in args.items()}
        _,brand,*_ = await parse_args(args)
        if not brand:
            await self.send_error(403,reason='The parameters carried are incorrect!!!')
        md5_key = await encryption(('conditions',brand,))
        cache_obj = Cache()
        result = await cache_obj.get(md5_key)
        if result:
            self.write(json.dumps(json.loads(result)))
            self.flush()
            return
        result = await conditions_query(brand)
        result = json.dumps(result)
        await cache_obj.set(md5_key,result,cache_obj.seconds_to_tomorrow)
        self.write(result)
        self.flush()


class IndexHandler(RequestHandler):

    async def prepare(self):
        client_ip = self.request.remote_ip
        if not client_ip in ACCESS_IP:
            self.send_error(403, reason='Current IP is unauthorized and access is not allowed!!!')

    async def get(self):
        result = {
            '扫描':'/scan',
            '发货-产品维度':'/delivery_product',
            '发货-渠道商维度':'/delivery_distributor',
            '普通红包':'/ordred',
            '下拉框条件':'/conditions'}
        self.write(json.dumps(result))



URL_MAPPING = (
    (r'/', IndexHandler),
    (r'/scan', ScanHandler),
    (r'/antifake',AntifakeHandler),
    (r'/cluster',ClusterHandler),
    (r'/delivery_product', DeliveryHandler),
    (r'/delivery_distributor', DeliveryHandler),
    (r'/ordred', OrdRedHandler),
    (r'/conditions', ConditionsHandler)
)


class Application:

    @staticmethod
    def app(url_mapping):
        return tornado.web.Application(url_mapping)


if __name__ == '__main__':
    app = Application.app(URL_MAPPING)
    http_server = HTTPServer(app)
    http_server.listen(80)
    tornado.ioloop.IOLoop.current().start()
