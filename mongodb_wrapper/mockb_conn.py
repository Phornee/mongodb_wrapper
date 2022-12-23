from .db_conn import DBConn, DBOpenException, DBGetLockException, DBReleaseLockException
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDBConn(DBConn):
    db_tables = {}

    def __init__(self):
        super().__init__()

    def openConn(self, params, autocommit=True):
        token = params['token']
        self.org = params['org']
        self.bucket = params['bucket']

        self.conn = InfluxDBClient(url=params['url'], token=token)

    def closeConn(self):
        self.conn.close()

    def insert(self, table, params):
        write_api = self.conn.write_api(write_options=SYNCHRONOUS)

        point = Point(table)\
                .tag("host", "host1")\
                .field("used_percent", 23.43234543)\
                .time(datetime.utcnow(), WritePrecision.NS)

        write_api.write(self.bucket, self.org, point)

    def getLock(self, lockname):
        raise

    def releaseLock(self, lockname):
        raise

