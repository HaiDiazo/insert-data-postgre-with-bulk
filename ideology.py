import time
import re

from config.pg_config import PostgresConfig
from elasticsearch import Elasticsearch
from service.percolator import PercolatorData
from elasticsearch.helpers import scan

POINT = [
    'location_coordinate',
    'province_coordinate',
    'country_coordinate',
    'city_coordinate',
    'district_coordinate',
    'sub_district_coordinate',
    'ann_content_location.location_coordinate'
]

config = {
    'CONFIG': {
        'FIELD_PERCOLATOR_PROVINSI': 'conflict.detail_location.province_name',
        'FIELD_PERCOLATOR_CITY': 'conflict.detail_location.city_name',
        'FIELD_PERCOLATOR_POLDA': '',
        'FIELD_PERCOLATOR_POLRES': ''
    }
}


def query():
    return {
        "_source": [
            "title",
            "created_at",
            "ann_content_location.location_coordinate",
            "ann_content_location.city_name",
            "ann_organization_all",
            "ann_person",
            "ann_person_all",
            "ann_sentiment",
            "conflict",
            "type"
        ],
        "query": {
            "match": {
                "type": "agama/rasisme"
            }
        }
    }


def query_count():
    return {
        "query": {
            "match": {
                "type": "agama/rasisme"
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


def geom_parser(key: str, value):
    if key in POINT and value is not None:
        if isinstance(value, dict):
            return f"POINT({value['lon']} {value['lat']})"
        if isinstance(value, str):
            pattern = r'-?\d+\.\d+,-?\d+\.\d+'
            if value is not None and re.match(pattern, value):
                point = value.split(',')
                return f"POINT({float(point[1])} {float(point[0])})"
    return value


def check_anomalies_parser(key: str, value):
    if value == '':
        return None
    if isinstance(value, dict):
        if value['lon'] is None or value['lat'] is None:
            return None
    return value


def get_count():
    with Elasticsearch(
            hosts="http://10.12.1.51:5200",
            http_auth=("ingest_ai", "1ngest4i2o23"),
            timeout=3600
    ) as es:
        count = es.count(body=query_count(), index='production-conflict-divtik-*')
        return count['count']


def insert_pg(bulk_data: list):
    config = PostgresConfig()
    connect = config.connect()
    cursor = connect.cursor()

    data_key = bulk_data[0]
    query_insert = f"""INSERT INTO conflict_ideology ({','.join([f'"{key}"' for key, value in data_key.items()])})
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
            hosts="http://10.12.1.51:5200",
            http_auth=("elastic", "rahasia2023"),
            timeout=3600
    ) as es:
        bulk_data = []
        start_time = time.time()
        for hit in scan(client=es, index="production-conflict-divtik-*", query=query(), size=100, request_timeout=3600,
                        scroll='10m'):
            source = hit['_source']
            payload = {
                'id': hit['_id'],
                'title': source.get('title'),
                'created_at': source.get('created_at'),
                'type': source.get('type'),
                'conflict': source.get('conflict'),
                'ann_sentiment': source.get('ann_sentiment'),
                'ann_person_all': source.get('ann_person_all'),
                'ann_person': source.get('ann_person'),
                'ann_organization_all': source.get('ann_organization_all'),
                'ann_content_location': source.get('ann_content_location'),
                'ann_content_location.city_name': None,
                'ann_content_location.location_coordinate': None,
                'conflict.category': None,
                'conflict.sub_category': None,
                'conflict.location': None,
                'conflict.detail_location.province_name': None,
                'conflict.detail_location.city_name': None,
                'conflict.detail_location.country_name': None,
                'conflict.city': None,
                'conflict.country': None,
                'conflict.reason': None,
                'conflict.stakeholders': None,
                'conflict.conflict_loss': None,
                'conflict.conflict_time': None,
                'conflict.conflict_duration': None,
            }

            if payload['conflict'] is not None:
                conflict = payload.get('conflict')
                payload.update(
                    {
                        'conflict.category': conflict.get('category'),
                        'conflict.sub_category': conflict.get('sub_category'),
                        'conflict.city': conflict.get('city'),
                        'conflict.country': conflict.get('country'),
                        'conflict.reason': conflict.get('reason'),
                        'conflict.stakeholders': conflict.get('stakeholders'),
                        'conflict.conflict_loss': conflict.get('conflict_loss'),
                        'conflict.conflict_time': conflict.get('conflict_time'),
                        'conflict.conflict_duration': conflict.get('conflict_duration'),
                    }
                )

                if 'detail_location' in conflict and conflict['detail_location'] is not None:
                    detail_location = conflict['detail_location']
                    payload.update(
                        {
                            'conflict.detail_location.province_name': detail_location.get('province_name'),
                            'conflict.detail_location.city_name': detail_location.get('city_name'),
                            'conflict.detail_location.country_name': detail_location.get('country_name'),
                        }
                    )

            if payload['ann_content_location'] is not None:
                ann_content_location = payload.get('ann_content_location')
                if isinstance(ann_content_location, list):
                    if len(ann_content_location) > 0:
                        payload.update(
                            {
                                'ann_content_location.city_name': ann_content_location[0].get('city_name'),
                                'ann_content_location.location_coordinate': ann_content_location[0].get(
                                    'location_coordinate'),
                            }
                        )
                elif isinstance(ann_content_location, dict):
                    payload.update(
                        {
                            'ann_content_location.city_name': ann_content_location.get('city_name'),
                            'ann_content_location.location_coordinate': ann_content_location.get(
                                'location_coordinate'),
                        }
                    )

            del payload['ann_content_location']
            del payload['conflict']
            print(payload)

            bulk_data.append(payload)
            if len(bulk_data) == 100:
                count += 100
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
