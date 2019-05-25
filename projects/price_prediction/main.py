import pandas as pd
from pandas import Series,DataFrame
import numpy as np
import luigi
import pickle
import datetime
import locale

class DataPreparingTask(luigi.Task):
    calendar_csv_filename = luigi.Parameter()
    listings_csv_filename = luigi.Parameter()
    using_cols = luigi.Parameter()
    intermediate_data_path = luigi.Parameter()

    def run(self):
        calendar_data_frame = pd.read_csv(self.calendar_csv_filename)
        listing_data_frame = pd.read_csv(self.listings_csv_filename)

        #########################
        # pregare calendar data #
        #########################
        use_columns_in_calendar = [
            'listing_id',
            'date',
            # 'available',
            'price',
        ]
        calendar_data_frame = calendar_data_frame.loc[:, use_columns_in_calendar]

        # price
        calendar_data_frame['price_amount'] = calendar_data_frame['price'].map(lambda x:float(str(x).replace(',', '').replace('$', '')))
        calendar_data_frame.loc[calendar_data_frame['price_amount'] > 0, :]
        # date
        locale.setlocale(locale.LC_TIME, 'ja_JP')
        calendar_data_frame['datetime'] = calendar_data_frame['date'].map(lambda x:datetime.datetime.strptime(str(x), '%Y-%m-%d'))
        # calendar_data_frame['year'] = calendar_data_frame['datetime'].map(lambda x:x.year)
        calendar_data_frame['month'] = calendar_data_frame['datetime'].map(lambda x:x.month)
        calendar_data_frame['day'] = calendar_data_frame['datetime'].map(lambda x:x.day)
        calendar_data_frame['day_of_week'] = calendar_data_frame['datetime'].map(lambda x:x.weekday())
        calendar_data_frame = pd.get_dummies(calendar_data_frame, columns=['month', 'day_of_week'])

        # print(calendar_data_frame.loc[0:3,:].to_csv())

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
        listing_data_frame = listing_data_frame.loc[:, use_columns_in_listing]
        listing_data_frame = listing_data_frame.rename(columns={'id': 'listing_id'})

        # latitude, longitude

        # property_type, room_type, cancellation_policy
        listing_data_frame = pd.get_dummies(listing_data_frame, columns=['property_type', 'room_type', 'cancellation_policy'])

        # print(listing_data_frame.loc[0:1,:].to_csv())

        ####################
        # marge and output #
        ####################
        intermediate_data_frame = pd.merge(listing_data_frame, calendar_data_frame, on='listing_id')

        with open(self.output().path, "wb") as target:
            pickle.dump(intermediate_data_frame, target)

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