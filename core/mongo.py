
# bson and ObjectId
from bson import ObjectId

# import mongoengine and connect
import mongoengine
mongoengine.connect('combine')

# import pymongo and establish client
import pymongo
mc_handle = pymongo.MongoClient()