from os import environ
from time import sleep
import psycopg2
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Column, Integer, String, JSON, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from math import sin,cos,acos
import asyncio
import json

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

def establish_psql_connection():
    while True:
        try:
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            print('Connection to PostgreSQL successful.')
            return psql_engine
        except OperationalError:
            sleep(0.1)

psql_engine = establish_psql_connection()

def establish_mysql_connection():
    while True:
        try:
            engine = create_engine(environ["MYSQL_CS"])
            print('Connection to MySQL successful.')
            return engine
        except OperationalError:
            print("Connection to MySQL refused. Retrying in 10 seconds...")
            sleep(10)
#
engine = establish_mysql_connection()

async def main():
    while True:
        devices_df = pd.read_sql_query('''SELECT
                                                *
                                          FROM public."devices";''', con=psql_engine)

        print(devices_df)

        devices_df['time'] = pd.to_datetime(devices_df['time'])
        devices_df['round_time'] = devices_df['time'].dt.floor('H')

        print(devices_df)

        # QUESTION1 - MAX TEMPERATURE
        max_temperature = devices_df.groupby(['device_id', 'round_time'])['temperature'].max().reset_index()
        print(max_temperature)

        # QUESTION2 - DATA POINTS
        data_points_count = devices_df.groupby(['device_id', 'round_time']).size().reset_index().rename(columns={0: 'count'})
        print(data_points_count)

        # QUESTION3 - DISTANCE TRAVELLED
        def calculate_dist(device_df):
            dist = 0
            for j in range(1, len(device_df)):
                location_str1 = device_df['location'].iloc[j-1].replace("'", '"')  
                location_str2 = device_df['location'].iloc[j].replace("'", '"')  
                location_dict1 = json.loads(location_str1)
                location_dict2 = json.loads(location_str2)
                lat1 = location_dict1['lat']
                lon1 = location_dict1['lon']
                lat2 = location_dict2['lat']
                lon2 = location_dict2['lon']
                dist += acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371
            return pd.DataFrame({
                'device_id': [device_df['device_id'].unique()[0]],
                'round_time': [device_df['round_time'].unique()[0]],
                'distance': [dist]
            })

        unique_groups = devices_df.groupby(['device_id', 'round_time']).size().reset_index().rename(columns={0: 'count'})

        distance_maintainer = pd.DataFrame()
        for ug in range(len(unique_groups)):
            needed_df = devices_df[(devices_df['device_id'] == unique_groups.iloc[ug]['device_id']) &
                                   (devices_df['round_time'] == unique_groups.iloc[ug]['round_time'])]
            x = calculate_dist(needed_df)
            distance_maintainer = pd.concat([distance_maintainer, x], axis=0)

        print(distance_maintainer)

        # BUILDING RESULT DF
        try:
            result_df = pd.merge(max_temperature, data_points_count, on=['device_id', 'round_time'], how='left')
            result_df = pd.merge(result_df, distance_maintainer, on=['device_id', 'round_time'], how='left')
            result_df.rename(columns={'temperature': 'max_temperature', 'count': 'data_point_count', 'distance': 'distance_travelled'}, inplace=True)
        except Exception as e:
            result_df = pd.DataFrame(columns=["device_id", "round_time", "max_temperature", "data_point_count", "distance_travelled"])

        # AGGREGATED DATA
        print(result_df)
 
        # SENDING TO MYSQL DB
        result_df.to_sql(name='results', con=engine, if_exists='append', index=False)

        await asyncio.sleep(30)

# Create an event loop
loop = asyncio.get_event_loop()

# Run the main coroutine within the event loop
loop.run_until_complete(main())
    



