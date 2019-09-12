import redis
import datetime
from dateutil.relativedelta import relativedelta


class Cache:

    def __init__(self, host='127.0.0.1', port=6379, database=0):
        self.my_redis = redis.StrictRedis(connection_pool=redis.ConnectionPool(host=host, port=port, db=database))

    @property
    def seconds_to_tomorrow(self):
        now = datetime.datetime.now()
        tomorrow = datetime.datetime(now.year,now.month,now.day+1,0,0,0,0)
        disparity = int((tomorrow-now).total_seconds())
        return disparity
        
    async def set(self,key,value,ex=None):
        try:
            await self.my_redis.set(key,value,ex)
            return True
        except Exception:
            return False

    async def get(self,key):
        try:
            result = self.my_redis.get(key)
            return result
        except Exception:
            return None

if __name__ == '__main__':
    pass 