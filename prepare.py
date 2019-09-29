import pandas as pd
import geopandas as gpd
import random
import sys
import shapefile
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes



def calculate_traveling_time(row):
    diff = row[1] - row[0]
    # by hour
    travle_time = round(diff.seconds/3600,2)
    return travle_time

def random_choose_geohash(row):
    #print(type(row))
    # generate geohash for the location
    geohashs = list(polygon_to_geohashes(row,6,False))
    # assign one geohash for user/driver
    geohash = random.choice(geohashs)
    return geohash

if __name__ == "__main__":

    df = pd.read_csv('./data/yellow_tripdata_2019-01.csv')
    df = df.sample(1000000)


    soa_shape_map_path = "./taxi_zones/taxi_zones.shp"
    soa_shape_map = gpd.read_file(soa_shape_map_path)
    soa_shape_map_geo = soa_shape_map.to_crs(epsg=4326)  # EPSG 4326 = WGS84 = https://epsg.io/4326


    final_df = df.merge(soa_shape_map_geo[["LocationID", "zone",'geometry']], left_on="PULocationID", right_on="LocationID") \
             .drop(['LocationID'], axis=1).rename(columns={"zone":"pickup zone"}) \
             .merge(soa_shape_map_geo[["LocationID", "zone",'geometry']], left_on="DOLocationID", right_on="LocationID") \
             .drop(['LocationID'], axis=1).rename(columns={"zone":"dropoff zone"})

    column = ['trip_distance','tip_amount','tpep_pickup_datetime','tpep_dropoff_datetime','geometry_x','pickup zone','geometry_y','dropoff zone']


    final_df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]] = final_df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]].apply(pd.to_datetime)


    # pickup geometry for user
    final_df['geohash_u'] = final_df.geometry_x.apply(random_choose_geohash)

    # dropoff geometry for driver
    final_df['geohash_d'] = final_df.geometry_y.apply(random_choose_geohash)

    # dropoff geometry for driver
    final_df['trip_time'] = final_df[['tpep_pickup_datetime','tpep_dropoff_datetime']].apply(calculate_traveling_time,axis=1)


    column_u_pick = ['pickup zone','geohash_u']
    column_u_drop = ['dropoff zone','geohash_d']


    user_df_pick = final_df[column_u_pick]
    user_df_pick.columns=['zone','geohash']

    user_df_drop = final_df[column_u_drop]
    user_df_drop.columns=['zone','geohash']

    user_df_pick['status'] = 'pickup'
    user_df_drop['status'] = 'dropoff'


    df_user = pd.concat([user_df_drop,user_df_pick])

    df_user.to_csv('./data/user_data_20190910_v1.csv')

