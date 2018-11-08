
# bson and ObjectId
from bson import ObjectId

# import mongoengine and connect
import mongoengine
mongoengine.connect('combine', host='127.0.0.1', port=27017)

# import pymongo and establish client
import pymongo
mc_handle = pymongo.MongoClient()