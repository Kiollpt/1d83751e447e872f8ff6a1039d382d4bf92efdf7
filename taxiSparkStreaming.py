from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
import pymysql
from datetime import datetime
from ownelastic import sink2elastic
import pickle



CAR_AMOUNT = 10
USER_AMOUNT = 15
TOPIC = ['users']
KAFKA_PARAMS={"bootstrap.servers":"10.0.0.115:9092",
               "group.id" : "test",
              "auto.offset.reset" :"smallest"

             }

BATCH_DURATION = 1
APP_NAME = 'Taxispark'

class TaxiSparkStreaming(object):


   # user = {'Hudson Sq': 10, 'Clinton East': 10, 'Times Sq/Theatre District': 10}
   # car = {'Hudson Sq': 15, 'Clinton East': 15, 'Times Sq/Theatre District': 15}
    zone_geohash  = pickle.load(open('./zone_geohash.pickle', 'rb'))
    user = pickle.load(open('./user.pickle', 'rb'))
    car = pickle.load(open('./car.pickle', 'rb'))

    def __init__(self):
        self.kafkaStream = None
        self.sc = SparkContext()
        self.ssc = StreamingContext(self.sc, BATCH_DURATION)

        print("Created Spark context...")


    @classmethod
    def Init_current_amount(cls,zone):
        cls.car[zone] = CAR_AMOUNT
        cls.user[zone] = USER_AMOUNT

    def connect(self):

        self.kafkaStream = KafkaUtils.createDirectStream(self.ssc,TOPIC,kafkaParams=KAFKA_PARAMS)
        self.kafkaStream.pprint()
        print("Connected Kafka Stream...")

    def check_kafkaStream(self):
        if not self.kafkaStream:
            raise ValueError('please connect first')
        else:
            print('Connected Kafka Stream sucessfully')

    def start(self):
        self.check_kafkaStream()
        self.process()
        self.ssc.start()
        self.ssc.awaitTermination()

    def process(self):

        TransformedDStream = self.kafkaStream.map(lambda x:json.loads(x[1])).map(TaxiSparkStreaming.process_records)
        final_stream = TransformedDStream.reduceByKey(lambda x,y : x+y)
        final_stream.foreachRDD(TaxiSparkStreaming.update_status)
        final_stream.pprint()

    @staticmethod
    def process_records(x):
        zone = x['zone']
        print(zone,type(zone))
        zone = str(zone)
	#if(zone not in TaxiSparkStreaming.user.values()):
	#    TaxiSparkStreaming.Init_current_amount(zone)
        #geohash = x['geohash']

        isPick = (x['status']=='pickup')
        if(isPick):
            tmp= 1
        else:
            tmp = -1

        return (zone,tmp)

    @staticmethod
    def update_status(rdd):
        if rdd.isEmpty():
            print('There is no data now')
            return rdd
        print(TaxiSparkStreaming.user)
        for item in rdd.collect():
            zone = item[0]
            count = item[1]
            TaxiSparkStreaming.user[zone] += count
            TaxiSparkStreaming.car[zone] += count

        TaxiSparkStreaming.show_ration()


    @staticmethod
    def show_ration():
        data = []
        for zone,demand in TaxiSparkStreaming.user.items():
            data_input = {}

            supply = TaxiSparkStreaming.car[zone]
            ratio = round(float(supply)/demand,2)
            data_input["zone"]= zone
            data_input["ratio"]= ratio
            data_input['timestamp'] = datetime.now()
            data_input['location'] = TaxiSparkStreaming.zone_geohash[zone]
            data.append(data_input)
        print(data)
        sink2elastic(data,'citys','city')

    # @staticmethod
    # def insert_db(table,data):
    #     col = list(data[0].keys())
    #     print('Start to insert',' ',data)
    #     PLACEHOLDER =', '.join(['%s']*len(col))
    #     COL =', '.join(col)
    #     sql = 'INSERT INTO data_pool.{tab} ({col}) VALUES ({ph})'.format(tab=table , col=COL, ph=PLACEHOLDER)
    #     with db.cursor() as cur:
    #         for i in data:
    #             print(i)
    #             cur.execute(sql,list(i.values()))
    #             db.commit()
    #
    #     db.close()
