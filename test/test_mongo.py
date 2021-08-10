import time

import r0mongo as mh

mongo_config = mh.MongoConfig("mongodb://127.0.0.1", 27017, targetdb="test", username="admin", password="123456")
mongo_tools = mh.MongoTools(mongo_config)
# mongo_tools._conn.drop_db()
# print(mongo_client.new_db("test_new_db"))
# db_conn = mongo_client.get_db_conn("test_new_db")
# print(mongo_client.get_database_name())
mongo_tools.insert_one(
    "test_set", {"name": "qwq", "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))}
)
print(mongo_tools.last_obj("test_set"))
