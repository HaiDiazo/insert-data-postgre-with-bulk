import json

import numpy
import pandas as pd
import requests
from pandas import DataFrame


class PercolatorData:
    def __init__(self, datas: list, context):
        self.__dataframe: DataFrame = DataFrame.from_records(datas)
        self.__context = context
        self.__provinsi = context['CONFIG']['FIELD_PERCOLATOR_PROVINSI'].split()
        self.__city = context['CONFIG']['FIELD_PERCOLATOR_CITY'].split()
        self.__polda = context['CONFIG']['FIELD_PERCOLATOR_POLDA'].split()
        self.__polres = context['CONFIG']['FIELD_PERCOLATOR_POLRES'].split()
        self.__countries = context['CONFIG']['FIELD_PERCOLATOR_COUNTRIES'].split()

    def api_percolator(self, list_col: list, dimension: str):
        resp = requests.post(
            url=f'http://10.100.1.119:8919/percolator/batch/{dimension}?mode=code',
            json=list_col
        )
        return json.loads(resp.text)

    def standardization_code(self):
        if len(self.__provinsi):
            for col in self.__provinsi:
                list_col = self.__dataframe[col].tolist()
                response = self.api_percolator(list_col, 'province')
                self.__dataframe['province_code'] = response
                self.__dataframe.drop(col, inplace=True, axis=1)
                self.__dataframe['province_code'].replace({numpy.nan: None}, inplace=True)
        if len(self.__city):
            for col in self.__city:
                list_col = self.__dataframe[col].tolist()
                response = self.api_percolator(list_col, 'city')
                self.__dataframe['city_code'] = response
                self.__dataframe.drop(col, inplace=True, axis=1)
                self.__dataframe['city_code'].replace({numpy.nan: None}, inplace=True)
        if len(self.__polda):
            for col in self.__polda:
                list_col = self.__dataframe[col].tolist()
                response = self.api_percolator(list_col, 'polda')
                self.__dataframe['polda_code'] = response
                self.__dataframe.drop(col, inplace=True, axis=1)
                self.__dataframe['polda_code'].replace({numpy.nan: None}, inplace=True)
        if len(self.__polres):
            for col in self.__polres:
                list_col = self.__dataframe[col].tolist()
                response = self.api_percolator(list_col, 'polres')
                self.__dataframe['polres_code'] = response
                self.__dataframe.drop(col, inplace=True, axis=1)
                self.__dataframe['polres_code'].replace({numpy.nan: None}, inplace=True)
        if len(self.__countries):
            for col in self.__countries:
                list_col = self.__dataframe[col].tolist()
                response = self.api_percolator(list_col, 'countries')
                self.__dataframe['country_code'] = response
                self.__dataframe.drop(col, inplace=True, axis=1)
                self.__dataframe['country_code'].replace({numpy.nan: None}, inplace=True)
        return self.__dataframe.to_dict('records')
