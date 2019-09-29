from kafka import KafkaProducer
import json
import pandas as pd


#example
payloads=[{'geohash': 'dr5rev', 'status': 'dropoff', 'zone': 'Hudson Sq'},
 {'geohash': 'dr5rev', 'status': 'dropoff', 'zone': 'Hudson Sq'},
 {'geohash': 'dr5rev', 'status': 'dropoff', 'zone': 'Hudson Sq'},
 {'geohash': 'dr5ru7', 'status': 'dropoff', 'zone': 'Clinton East'},
 {'geohash': 'dr5ru5', 'status': 'dropoff', 'zone': 'Clinton East'},
 {'geohash': 'dr5rum', 'status': 'dropoff', 'zone': 'Clinton East'},
 {'geohash': 'dr5rum', 'status': 'dropoff', 'zone': 'Clinton East'},
 {'geohash': 'dr5ru5', 'status': 'pickup', 'zone': 'Clinton East'},
 {'geohash': 'dr5rsn', 'status': 'dropoff', 'zone': 'Hudson Sq'},
 {'geohash': 'dr5rsj', 'status': 'dropoff', 'zone': 'Hudson Sq'}]


if __name__== '__main__':
	
    producer = KafkaProducer(bootstrap_servers=['10.0.0.115:9092'])
	

    df = pd.read_csv('./data/user_data_20190910_v1.csv')
    df = df.drop(['Unnamed: 0'], axis=1)
    #payloads = list(df.T.to_dict().values())
    for pay in payloads:
        producer.send('users', json.dumps(pay).encode('utf-8')).get(timeout=30)
    print('Sent message..')
