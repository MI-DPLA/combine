from django.conf import settings

# bson and ObjectId
from bson import ObjectId

# import mongoengine and connect
import mongoengine
mongoengine.connect('combine', host=settings.MONGO_HOST, port=27017)

# import pymongo and establish client
import pymongo
mc_handle = pymongo.MongoClient(host=settings.MONGO_HOST, port=27017)
