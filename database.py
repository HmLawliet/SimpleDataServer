from collections import namedtuple
import pymysql
from tornado_mysql import connect,MySQLError
import tornado_mysql 


MYSQL = namedtuple('MYSQL', 'host,user,password,database')


class ConnectionPool:

    _connect = None
    def __init__(self,host,user,password,database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database 


    def get_connection(self):
        if not self._connect:
            try:
                self._connect = connect(host=self.host, user=self.user,password=self.password, database=self.database,charset='utf8mb4')
            except MySQLError as e:
                raise e
        return self._connect

    def __enter__(self):
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, type, value, trace):
        self.cursor.close()
    
    async def __aenter__(self):
        self.connection = await self.get_connection()
        self.cursor =  self.connection.cursor()
        return self.cursor

    async def __aexit__(self, type, value, trace):
        self.cursor.close()

    def __del__(self):
        self.connection.close()


mysql_pre_antifake = MYSQL('******', 'test','*******', 'antifake')
mysql_pre_wpwlWebPre = MYSQL('******','root', '******', 'wpwl_web_pre')


CON_POOL = ConnectionPool(mysql_pre_antifake.host,mysql_pre_antifake.user,mysql_pre_antifake.password,mysql_pre_antifake.database)
QUE_POOL = ConnectionPool(mysql_pre_wpwlWebPre.host,mysql_pre_wpwlWebPre.user,mysql_pre_wpwlWebPre.password,mysql_pre_wpwlWebPre.database)
