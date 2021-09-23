# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import json
from kafka import KafkaProducer


class HardwarezonePipeline:
    def __init__(self):
        connection = pymongo.MongoClient(
            "localhost",
            27017
        )
        db = connection["hardwarezone"]
        self.collection = db["posts"]
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
            value_serializer=lambda v:json.dumps(v).encode('utf-8'))

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            #self.collection.insert(dict(item))
            self.producer.send('scrapy-output', dict(item))
        return item
