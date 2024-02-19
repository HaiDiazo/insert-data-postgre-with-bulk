import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import URL


class PostgresConfig(object):

    @staticmethod
    def connect():
        conn = psycopg2.connect(database="territorial",
                                host="10.11.2.45",
                                user="postgres",
                                password="t3rr1t0r!4l@2o24",
                                port=5432)
        return conn

    @staticmethod
    def prod_conn_sql_alchemy():
        url_object = URL.create(
            "postgresql",
            username="postgres",
            password="m4s51v3pr0f1lin9",  # plain (unescaped) text
            host="10.11.2.32",
            database="massiveprofiling",
        )
        password = "p0sTgr3s@d1vTiK"
        conn_string = f'postgresql://postgres:{password}@10.220.66.186/gaiaocean'
        db = create_engine(url_object)
        return db.connect()


client = PostgresConfig()
