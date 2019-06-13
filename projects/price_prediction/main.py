import pandas as pd
from pandas import Series,DataFrame
import numpy as np
import luigi
import pickle
import datetime
import locale
import requests
import json

class DataPreparingTask(luigi.Task):
    calendar_csv_filename = luigi.Parameter()
    listings_csv_filename = luigi.Parameter()
    using_cols = luigi.Parameter()
    intermediate_data_path = luigi.Parameter()
    google_places_api_url = luigi.Parameter()
    api_key = luigi.Parameter()

    def run(self):
        df_calendar = pd.read_csv(self.calendar_csv_filename)
        df_listing = pd.read_csv(self.listings_csv_filename)

        #########################
        # pregare calendar data #
        #########################
        use_columns_in_calendar = [
            'listing_id',
            'date',
            # 'available',
            'price',
        ]
        df_calendar = df_calendar.loc[0:1000, use_columns_in_calendar]

        # price
        df_calendar['price_amount'] = df_calendar['price'].map(lambda x:float(str(x).replace(',', '').replace('$', '')))
        df_calendar.loc[df_calendar['price_amount'] > 0, :]
        # date
        locale.setlocale(locale.LC_TIME, 'ja_JP')
        df_calendar['datetime'] = df_calendar['date'].map(lambda x:datetime.datetime.strptime(str(x), '%Y-%m-%d'))
        # df_calendar['year'] = df_calendar['datetime'].map(lambda x:x.year)
        df_calendar['month'] = df_calendar['datetime'].map(lambda x:x.month)
        df_calendar['day'] = df_calendar['datetime'].map(lambda x:x.day)
        df_calendar['day_of_week'] = df_calendar['datetime'].map(lambda x:x.weekday())
        df_calendar = pd.get_dummies(df_calendar, columns=['month', 'day_of_week'])
        del df_calendar['date']
        del df_calendar['price']
        del df_calendar['datetime']

        ########################
        # pregare listing data #
        ########################
        use_columns_in_listing = [
            'id',
            'latitude',
            'longitude',
            'property_type',
            'room_type',
            'accommodates',
            'bedrooms',
            'beds',
            'cancellation_policy',
        ]
        df_listing = df_listing.loc[:, use_columns_in_listing]
        df_listing = df_listing.rename(columns={'id': 'listing_id'})

        # latitude, longitude
        # Google Places API
        radius = '300'
        language = 'en'
        for index, row in df_listing.iterrows():
            response = requests.get(self.google_places_api_url + 
                        'key=' + self.api_key + 
                        '&location=' + str(row.latitude) + ',' + str(row.longitude) + 
                        '&radius=' + radius + 
                        '&language=' + language)
            data = response.json()
            for result in data['results']:
                if not result['types'][0] in df_listing.columns:
                    df_listing[result['types'][0]] = 0
                df_listing.loc[index, result['types'][0]] += 1

        # property_type, room_type, cancellation_policy
        df_listing = pd.get_dummies(df_listing, columns=['property_type', 'room_type', 'cancellation_policy'])

        del df_listing['latitude']
        del df_listing['longitude']

        ####################
        # marge and output #
        ####################
        df_intermediate = pd.merge(df_listing, df_calendar, on='listing_id')
        del df_intermediate['listing_id']

        with open(self.output().path, "wb") as target:
            pickle.dump(df_intermediate, target)

    def output(self):
        return luigi.LocalTarget(self.intermediate_data_path)

class CreateModelTask(luigi.Task):
    def requires(self):
        return DataPreparingTask()

    def run(self):
        return

    def output(self):
        return


if __name__ == '__main__':
    luigi.run(['DataPreparingTask', '--workers', '1', '--local-scheduler'])