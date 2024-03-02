import time
import json

from config.pg_config import PostgresConfig
from elasticsearch import Elasticsearch
from service.percolator import PercolatorData
from elasticsearch.helpers import scan
from shapely.geometry import shape

POINT = [
    'location_coordinate',
    'province_coordinate',
    'country_coordinate',
    'city_coordinate',
    'district_coordinate',
    'sub_district_coordinate',
    'location'
]

INDEX = "global-data*"
TABLE_NAME = 'military_sipri'

MAPPING = {
    'menu': None,
    'value': None,
    'date_time': None,
    'id': None,
    'country': None,
    'location': None
}

ES_CONFIG = {
    'HOST': 'http://192.168.20.21:9200',
    'USER': 'elastic',
    'PASS': '3last!c2o22G0s1p'
}

GEOSHAPE = []

config = {
    'CONFIG': {
        'FIELD_PERCOLATOR_PROVINSI': '',
        'FIELD_PERCOLATOR_CITY': '',
        'FIELD_PERCOLATOR_POLDA': '',
        'FIELD_PERCOLATOR_COUNTRIES': 'country',
        'FIELD_PERCOLATOR_POLRES': ''
    }
}


def query():
    return {
        "_source": [
            "menu", "value", "date_time", "id", "country", "location"
        ],
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "category.keyword": "sipri"
                        }
                    },
                    {
                        "match": {
                            "category.keyword": "sipri arms transfers database"
                        }
                    }
                ]
            }
        }
    }


def query_count():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "category.keyword": "sipri"
                        }
                    }
                ]
            }
        }
    }


def geom_point(key: str):
    if key in POINT and key is not None:
        return "ST_GeomFromText(%s, 4326)"
    return "%s"


def check_anomalies_values(key: str, value):
    if value == '':
        return None
    if isinstance(value, dict):
        if value['lon'] is None or value['lat'] is None:
            return None
    return key


def convert_geoshape_to_wkt(geoshape):
    geometry = shape(geoshape)
    return geometry.wkt


def geom_parser(key: str, value):
    if key in POINT and key is not None:
        if isinstance(value, dict) and key in POINT:
            return f"POINT({value['lon']} {value['lat']})"
        elif isinstance(value, list):
            return f"POINT({value[0]} {value[1]})"
    elif key in GEOSHAPE and key is not None:
        if value is not None:
            return f"{convert_geoshape_to_wkt(value)}"
    elif isinstance(value, dict):
        return json.dumps(value)
    elif isinstance(value, list):
        return json.dumps(value)
    return value


def check_anomalies_parser(key: str, value):
    if value == '':
        return None
    if isinstance(value, dict) and key in POINT:
        if value['lon'] is None or value['lat'] is None:
            return None
    return value


def get_count():
    with Elasticsearch(
            hosts=ES_CONFIG['HOST'],
            http_auth=(ES_CONFIG['USER'], ES_CONFIG['PASS']),
            timeout=3600
    ) as es:
        count = es.count(body=query_count(), index=INDEX)
        return count['count']


def insert_pg(bulk_data: list):
    config = PostgresConfig()
    connect = config.connect()
    cursor = connect.cursor()

    data_key = bulk_data[0]
    query_insert = f"""INSERT INTO {TABLE_NAME} ({','.join([key for key, value in data_key.items()])})
                VALUES"""
    first_values = f"({','.join([geom_point(check_anomalies_values(key, value)) for key, value in data_key.items()])})"
    first_param = tuple([geom_parser(key, check_anomalies_parser(key, value)) for key, value in data_key.items()])
    for data in bulk_data[1:]:
        next_values = f"({','.join([geom_point(check_anomalies_values(key, value)) for key, value in data.items()])})"
        next_params = tuple([geom_parser(key, check_anomalies_parser(key, value)) for key, value in data.items()])

        first_values = f"{first_values},{next_values}"
        first_param = first_param + next_params
    query_insert = f"{query_insert}{first_values}"

    # print(query_insert)
    # print(first_param)
    cursor.execute(query_insert, first_param)
    connect.commit()
    cursor.close()
    connect.close()


count = 0
if __name__ == "__main__":
    with Elasticsearch(
            hosts=ES_CONFIG['HOST'],
            http_auth=(ES_CONFIG['USER'], ES_CONFIG['PASS']),
            timeout=3600
    ) as es:
        bulk_data = []
        start_time = time.time()
        for hit in scan(client=es, index=INDEX, query=query(), size=1000, request_timeout=3600,
                        scroll='10m'):
            source = hit['_source']

            payload = {}
            for key, _ in MAPPING.items():
                payload[key] = source.get(key)

            bulk_data.append(payload)
            # print(payload)
            if len(bulk_data) == 1000:
                count += 1000
                percolate = PercolatorData(datas=bulk_data, context=config)
                results = percolate.standardization_code()
                insert_pg(results)
                bulk_data.clear()

                finish_time = time.time() - start_time
                print("=======================")
                print(f"Already ingest {count}/{get_count()}")
                print(f"Execute time: {finish_time}")
                print("=======================")
                start_time = time.time()
        if len(bulk_data):
            percolate = PercolatorData(datas=bulk_data, context=config)
            results = percolate.standardization_code()
            insert_pg(results)
            count += len(bulk_data)
            bulk_data.clear()
            finish_time = time.time() - start_time
            print("=======================")
            print(f"Already ingest {count}/{get_count()}")
            print(f"Execute time: {finish_time}")
            print("=======================")
            start_time = time.time()
