import copy
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import List, Callable, Union

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId

import dt_helper as dh
import input_helper as ih

nest_asyncio.apply()

__SCALE_DICT__ = {
    "bytes": 1,
    "KB": 1024,
    "MB": 1048576,
    "GB": 1073741824
}


def dict_filter(origin_dict: dict, filters: List[str] = None):
    for it in filters:
        """
        ans = ... is required
        If it does not exist in the dictionary, 
        ans can accept the value of none, 
        otherwise an error will be reported
        """
        ans = origin_dict.pop(it, "-1")
    return 1


def dict_list_filter(origin_list: List[dict], filters: dict = None):
    """
    Enter a list of dictionaries
    Remove unqualified items based on filters
    ps: Removing items is means delete the whole items,
        if you just want remove some fields from a/many dict/dicts.
        you should use the dict_filter()
    """
    rest_l = copy.deepcopy(origin_list)
    if not filters:
        return origin_list

    for i in rest_l:
        for k, v in filters.items():
            if isinstance(v, (list, tuple)) and i.get(k) not in v:
                origin_list.remove(i)
                break
            elif not isinstance(v, (list, tuple)) and i.get(k) != v:
                origin_list.remove(i)
                break
    return origin_list


class MongoConfig(object):
    def __init__(self, host: str, port: int, targetdb: str, username: str = None, password: str = None):
        self.host = host
        self.port = port
        self.targetdb = targetdb
        self.username = username
        self.password = password
        self._i = 0

    def __iter__(self):
        return self

    def __next__(self):
        pass


class MgClient(object):
    """docstring for MgClient"""

    def __init__(self, config: MongoConfig, do_connect: bool = True, max_pool: int = 1, retries: int = 5):
        self.config = config

        self.host = self.config.host
        self.port = self.config.port
        self._db = self.config.targetdb
        self.username = self.config.username
        self.password = self.config.password
        self.max_pool = max_pool

        self.do_connect = do_connect
        self.retries = retries

        self._conn = None
        self._alive = False
        self._db_names = None

        self.connect()
        self.auth_if_required()
        try:
            self.update_db_names()
        except Exception as e:
            self._db_names = []
        self._timest = time.strftime("%d %H:%M:%S", time.localtime(time.time()))

    def _client_opts(self):
        opts = {
            "host": self.host,
            "port": self.port,
            "maxPoolSize": self.max_pool,
            "connectTimeoutMS": 1000,
            "serverSelectionTimeoutMS": 1000
        }
        return opts

    def _connect(self):
        conn = MongoClient(**self._client_opts())
        if conn is not None:
            self._conn = conn
            try:
                """
                Cannot judge whether the connection is successful through the return value.
                Making a command to admin-db is just a patch.
                """
                self._conn["admin"].command("ismaster")
                self._alive = True
            except ConnectionFailure:
                self._alive = False
                print("Server not available")
                return -1
            if self._alive is True and self.do_connect is True:
                res = self._conn["admin"].command({"ping": 1})
            else:
                pass
        else:
            pass
        return 1

    def connect(self):
        """
        Connect to Mongodb and get a client instance
        If the connection fails, try again 5 times
        """
        for tries in range(self.retries):
            if self._connect() == 1:
                break
            else:
                print(f"Retries {tries}: connect")
        return self._conn

    def reconnect(self):
        self.close()
        time.sleep(1)
        self.connect()

    def db_connection(self):
        return self._conn, self._db

    def auth_if_required(self, target: str = None):
        """
        Need to verify before using a particular db.
        But I am not sure if this method is better integrated to a higher level
        """
        if self._alive is False:
            return
        if self.username is not None and self.password is not None:
            try:
                if target is None:
                    self._conn[self._db].authenticate(self.username, self.password)
                else:
                    self._conn[target].authenticate(self.username, self.password)
                print("auth successfully")
            except Exception as e:
                print(f"Reconnect 1: {e}")
                self.reconnect()
                # raise Exception("Auth failed") from e
                raise
        else:
            pass

    def change_db(self, target: str):
        self._db = target

    def get_db(self, target: str = None):
        """
        Get db is abstract enough.
        Obtaining a set through this class may cause abstraction leakage.
        """
        if target in self._db_names:
            if target is None:
                return self._conn[self._db]
            else:
                return self._conn[target]
        else:
            pass

    def new_db(self, target: str):
        return self._conn[target]

    def get_database_name(self):
        return self._db_names

    def get_collection_names(self, target: str = None):
        if target is None:
            return self._conn[self._db].list_collection_names()
        else:
            return self._conn[target].list_collection_names()

    def update_db_names(self):
        self._db_names = self._conn.list_database_names()

    def _command(self, *args, **kwargs):
        """
        Run a db command and return the results
        """
        target = self._db
        return self._conn[target].command(*args, **kwargs)

    def db_stats(self, scale: str = "bytes"):
        """
        Return a dict of info about the db

        - scale: one of bytes, KB, MB, GB
            - ps: avgObeSize is always in bytes no matter what the scale is

        From: https://docs.mongodb.com/manual/reference/command/dbStats/#output
        """
        try:
            scale_val = __SCALE_DICT__[scale]
        except KeyError:
            scale_val = 1
        return self._command("dbStats", scale=scale_val)

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        else:
            pass

    def __del__(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        else:
            pass


def get_date_query(
        date_string: str,
        fmt: str = "%Y-%m-%d",
        timezone: str = "Asia/Shanghai",
        timestamp_field: str = "_id"
):
    date_start = dh.date_start_utc(date_string, fmt, timezone)
    query = {timestamp_field: {}}
    start = date_start
    end = date_start + dh.timedelta(days=1)
    if timestamp_field is "_id":
        start = ObjectId.from_datetime(start)
        end = ObjectId.from_datetime(end)
    query[timestamp_field]["$gte"] = start
    query[timestamp_field]["$lt"] = end
    return query


def get_days_ago_query(
        days_ago: int = 0,
        until_days_ago: int = 0,
        timezone: str = "America/Chicago",
        timestamp_field: str = "_id"
):
    assert days_ago >= until_days_ago
    if days_ago > 0:
        assert days_ago != until_days_ago
    query = {timestamp_field: {}}
    start = dh.days_ago(days_ago, timezone=timezone)
    end = dh.days_ago(until_days_ago, timezone=timezone)
    if until_days_ago == 0:
        end = dh.date_start_utc(dh.utc_now_float_string("%Y-%m-%d"), "%Y-%m-%d", timezone) + dh.timedelta(days=1)
    if timestamp_field == "_id":
        start = ObjectId.from_datetime(start)
        end = ObjectId.from_datetime(end)
    query[timestamp_field]["$gte"] = start
    if days_ago > 0:
        query[timestamp_field]["$lt"] = end
    return query


def get_hours_ago_query(
        hours_ago: int = 1,
        until_hours_ago: int = 0,
        timestamp_field: str = "_id"
):
    assert hours_ago > until_hours_ago
    now = dh.utc_now_localized()
    query = {timestamp_field: {}}
    start = now - dh.timedelta(hours=hours_ago)
    end = now - dh.timedelta(hours=until_hours_ago)
    if timestamp_field == "_id":
        start = ObjectId.from_datetime(start)
        end = ObjectId.from_datetime(end)
    query[timestamp_field]["$gte"] = start
    query[timestamp_field]["$lt"] = end
    return query


def get_minutes_ago_query(
        minutes_ago: int = 1,
        until_minutes_ago: int = 0,
        timestamp_field: str = "_id"
):
    """
    Return a dict representing a query for matching minute(s)
        - timestamp_field: name of timestamp field to query on
    """
    assert minutes_ago > until_minutes_ago
    now = dh.utc_now_localized()
    query = {timestamp_field: {}}
    start = now - dh.timedelta(minutes=minutes_ago)
    end = now - dh.timedelta(minutes=until_minutes_ago)
    if timestamp_field == "_id":
        start = ObjectId.from_datetime(start)
        end = ObjectId.from_datetime(end)
    query[timestamp_field]["$gte"] = start
    query[timestamp_field]["$lt"] = end
    return query


class MongoTools(object):

    def __init__(self, config: MongoConfig = None, mongo_client: MgClient = None):
        self._config = config

        if mongo_client is None:
            """
            It means new a client, so caller should apply a config
            """
            self._conn = MgClient(config)

        else:
            self._conn = mongo_client

        self._mongo_client, self._db = self._conn.db_connection()

        self._loop = asyncio.get_event_loop()
        self._tasks = []
        self._tasks_res = None

    def _find_one(self, collection: str, *args,
                  fields: str = None, ignore_fields: str = None, **kwargs):
        """
        Return a dict or a value to represent the query result
            - fields: string containing fields to return,
                separated by any of , ; |
            - If exactly 1 field is specified,
                the value of that field will be returned instead of a dict
            - ignore_fields: string containing fields to ignore,
                separated by any of , ; |

        Other kwargs are the same as those to _find
        """
        self.commit_tasks()
        if fields is not None and ignore_fields is not None:
            raise Exception("Cannot specify both 'fields' and 'ignore_fields'")

        db = self._db
        force_value = False

        if fields is not None:
            fields = ih.get_list_from_arg_strings(fields)
            kwargs["projection"] = {k: 1 for k in fields}

            if "_id" not in fields:
                kwargs["projection"]["_id"] = 0
            if len(fields) == 1:
                force_value = True
        else:
            kwargs["projection"] = {"_id": 1}

        if ignore_fields is not None:
            ignore_fields = ih.get_list_from_arg_strings(ignore_fields)
            kwargs["projection"] = {k: 0 for k in ignore_fields}

        result = self._mongo_client[db][collection].find_one(*args, **kwargs)

        if result is None:
            result = {}
        elif force_value is True:
            """
            result is not None, and force_value is True
            it means just find single field
            """
            result = result.get(fields[0])

        return result

    def _find(self, collection, *args,
              fields: str = None, ignore_fields: str = None, to_list: bool = False, **kwargs):
        """
        Return a db cursor
            - fields: string containing fields to return,
                separated by any of , ; |
                - If exactly 1 field is specified,
                    the value of that field will be returned instead of a dict
            - ignore_fields: string containing fields to ignore,
                separated by any of , ; |
            - to_list: if True, return a list of dicts instead of a cursor

        The following is the description of Other kwargs:
            - sort:
                list of (key, direction) pairs for sort order of results
            - limit:
                max number of results to return
            - skip:
                number of documents to omit from the start of result set
            - return_key:
                if True, only return the index keys in each document
            - max_time_ms:
                max number of milliseconds the find operation is allowed to run
            - hint:
                an index to use in the format passed to _create_index...
                [(field, direction)]
            - batch_size:
                limit number of documents returned in a single batch (> 1)
            - show_record_id:
                if True, add a "$recordId" field in each document
                with the storage engine"s internal record identifier
            - no_cursor_timeout:
                if True, returned cursor will never timeout on the server (instead of being closed after 10 mins)
            - cursor_type:  type of cursor to return:
                - pymongo.cursor.CursorType.NON_TAILABLE: standard cursor
                - pymongo.cursor.CursorType.TAILABLE: only for use
                    with capped
                     collections; cursor is not closed when last data received
                - if more data is received, iteration of cursor will continue from last document received
                - pymongo.cursor.CursorType.TAILABLE_AWAIT:
                    tailable cursor with await flag set
                - server will wait a few seconds
                    after returning the full result set
                    so that it can capture and return additional data added during the query
                - pymongo.cursor.CursorType.EXHAUST:
                    stream batched results
                    without waiting for client to request each batch
        From:
            https://docs.mongodb.com/manual/tutorial/query-arrays/
            https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/
        How to use cursor:
            https://docs.mongodb.com/manual/tutorial/iterate-a-cursor/
        """
        self.commit_tasks()
        if fields is not None and ignore_fields is not None:
            raise Exception("Cannot specify both 'fields' and 'ignore_fields'")

        db = self._db
        force_value = False

        if fields is not None:
            fields = ih.get_list_from_arg_strings(fields)
            kwargs["projection"] = {k: 1 for k in fields}

            if "_id" not in fields:
                kwargs["projection"]["_id"] = 0

            if len(fields) == 1:
                force_value = True
        # force_value = True

        if ignore_fields is not None:
            ignore_fields = ih.get_list_from_arg_strings(ignore_fields)
            kwargs["projection"] = {k: 0 for k in ignore_fields}

        ans_cursor = self._mongo_client[db][collection].find(*args, **kwargs)

        if force_value is True:
            return [i.get(fields[0]) for i in ans_cursor]

        if to_list is True:
            ans_list = list(ans_cursor)
            ans_cursor.close()
            return ans_list
        else:
            return ans_cursor

    async def _insert_one(self, collection: str, document: dict):
        """
        Add a document to the collection and return a inserted_id
            - document: a dict of info to be inserted
        ps: the returned type is pymongo.results.InsertOneResult
        """
        db = self._db
        try:
            result = self._mongo_client[db][collection].insert_one(document)
        except Exception as e:
            raise e
        return result.inserted_id

    async def _insert_many(self, collection: str, documents: List[dict]):
        """
        Add several documents to the collection and return a list of inserted_ids
        """
        db = self._db
        try:
            result = self._mongo_client[db][collection].insert_many(documents)
        except Exception as e:
            raise e

        return result.inserted_ids

    def _update_one(self, collection: str, match: dict, update: dict, upsert: bool = False):
        """
        Update the first matching item in the collection and return the modified number
            - match: a dict of the query matching document to update
            - update: dict of modifications to apply
            - upsert: if True, perform an insert if no documents match
        """
        db = self._db
        result = self._mongo_client[db][collection].update_one(match, update, upsert=upsert)
        return result.modified_count

    def _update_many(self, collection: str, match: dict, update: dict, upsert: bool = False):
        """
        Update all the matching items from the collection and return num modified
            - match: a dict of the query matching document to update
            - update: dict of modifications to apply
            - upsert: if True, perform an insert if no documents match
        """
        db = self._db
        result = self._mongo_client[db][collection].update_many(match, update, upsert=upsert)
        return result.modified_count

    async def _delete_one(self, collection: str, match: dict):
        """
        Delete one matching item from the collection and return num deleted
            - match: a dict of the query matching document to delete
        """
        db = self._db
        result = self._mongo_client[db][collection].delete_one(match)
        return result.deleted_count

    async def _delete_many(self, collection: str, match: dict):
        """
        Delete all matching items from the collection and return num deleted
            - match: a dict of the query matching documents to delete
        """
        db = self._db
        result = self._mongo_client[db][collection].delete_many(match)
        return result.deleted_count

    def _distinct(self, collection: str, key: str, match: dict = {}, **kwargs):
        """
        Return a list of distinct values for key among documents in collection
            - key:
                name of the field for which we want to get the distinct values
            - match (optional):
                A query document that specifies the documents from which to retrieve the distinct values.
            - maxTimeMS:
                max number of milliseconds the operation is allowed to run
        ps: the key is required
        """
        db = self._db
        return self._mongo_client[db][collection].distinct(key, match, **kwargs)

    def _count(self, collection: str, match: dict = {}, *args, **kwargs):
        """
        Return total count of matching documents in collection

            - match: a dict of the query matching documents to count
            - maxTimeMS: max number of milliseconds the count_documents operation is
              allowed to run
            - limit: max number of documents to count
            - skip: number of documents to skip before returning results
            - hint: index to use (string, or list of tuples)
        """
        db = self._db
        return self._mongo_client[db][collection].count_documents(match, *args, **kwargs)

    def _create_index(self,
                      collection: str, keys, unique: bool = False, ttl: int = None,
                      sparse: bool = False, background: bool = False, **kwargs):
        """
        Create an index on the collection，
        return the index.
            - keys: list of 2-item tuples
                where first item is a field name
                and second item is a direction (1 for ascending, -1 for descending)
            - keys can also be a single key (string) or list of strings
            - unique: if True, create a uniqueness constraint
            - ttl: int representing "time to live" (in seconds)
                for documents in the collection
            - sparse: if True, only index documents that contain the indexed field
            - background: if True, create the index in the background
            - name: custom name to use for the index; auto-generated by default
            - partialFilterExpression: a document
                that specifies a filter for a partial index
        After this method, you may use the returned index in _find()
        """
        kwargs["unique"] = unique
        kwargs["sparse"] = sparse
        kwargs["background"] = background
        if ttl is not None:
            kwargs["expireAfterSeconds"] = ttl
        db = self._db
        return self._mongo_client[db][collection].create_index(keys, **kwargs)

    def _drop_all_indexes(self, collection: str, **kwargs):
        """
        Drop all the indexes rom the collection
        """
        db = self._db
        return self._mongo_client[db][collection].drop_indexes(**kwargs)

    def _index_information(self, collection):
        """Return a dict of info about indexes on collection"""
        db = self._db
        try:
            index_info = self._mongo_client[db][collection].index_information()
        except Exception as e:
            index_info = {}
            raise e
        return index_info

    def _index_names(self, collection: str):
        """
        Return list of index names
        """
        return sorted(list(self._index_information(collection).keys()))

    def _index_sizes(self, collection: str, scale: str = "bytes"):
        """
        - scale: one of bytes, KB, MB, GB

        Wrapper to coll_stats
        """
        return self.coll_status(collection, scale=scale).get("indexSizes", {})

    def _index_usage(self, collection: str, name: str = "", full: bool = False):
        """
        Return list of tuples, which sorted by number of operations that used index
            - name: name of specific index
            - full: if True, return full list of dicts from $indexStats aggregation
        """
        db = self._db
        pipeline = [{"$indexStats": {}}]
        if name != "":
            pipeline.append({"$match": {"name": name}})
        cursor = self._mongo_client[db][collection].aggregate(pipeline)
        if full is True:
            return list(cursor)
        else:
            results = [
                (d["accesses"]["ops"], d["name"])
                for d in cursor
            ]
            return sorted(results, reverse=True)

    def create_unique_index(self, collection: str, index_list: List[Tuple[str, int]]):
        return self._create_index(collection, index_list, unique=True)

    def drop_index(self, collection: str, index_name: str, **kwargs):
        db = self._db
        return self._mongo_client[db][collection].drop_index(index_name, **kwargs)

    def drop_all_indexes(self, collection: str, **kwargs):
        """
        Drop all the indexes rom the collection
        """
        db = self._db
        self._mongo_client[db][collection].drop_indexes(**kwargs)

    def all_reindex(self, collection: str, **kwargs):
        db = self._db
        self._mongo_client[db][collection].reindex(**kwargs)
        
    def _command(self, *args, **kwargs):
        """
        Run a db command and return the results
        """
        db = self._db
        return self._mongo_client[db].command(*args, **kwargs)

    def documents_total(self, collection: str):
        """
        Return total count of documents in collection
        """
        db = self._db
        return self._mongo_client[db][collection].estimated_document_count()

    def db_status(self, scale: str = "bytes"):
        """
        Return a dict of the information about the db
            - scale: one of bytes, KB, MB, GB
                - NOTE: avgObeSize is always in bytes no matter what the scale is
        From: https://docs.mongodb.com/manual/reference/command/dbStats/#output
        """
        try:
            scale_val = __SCALE_DICT__[scale]
        except KeyError:
            scale_val = 1
        return self._command("dbStats", scale=scale_val)

    def coll_status(self, collection: str,
                    ignore_fields: str = "wiredTiger, indexDetails",
                    scale: str = "bytes"):
        """
        Return a dict of info about the collection

            - ignore_fields: string containing output fields to ignore, separated by
              any of , ; |
            - scale: one of bytes, KB, MB, GB
                - NOTE: avgObeSize is always in bytes no matter what the scale is

        From: https://docs.mongodb.com/manual/reference/command/collStats/#output
        """
        try:
            scale_val = __SCALE_DICT__[scale]
        except KeyError:
            scale_val = 1
        output = self._command("collStats", collection, scale=scale_val)
        if ignore_fields is not None:
            output = ih.ignore_keys(output, ignore_fields)
        return output

    def server_status(self,
                      ignore_fields: str = "wiredTiger, tcmalloc, metrics, logicalSessionRecordCache"):
        """
        Return a dict of output from serverStatus db command

            - ignore_fields: string containing output fields to ignore, separated by
              any of , ; |

        From: https://docs.mongodb.com/manual/reference/command/serverStatus/#output
        """
        output = self._command("serverStatus")
        if ignore_fields is not None:
            output = ih.ignore_keys(output, ignore_fields)
        return output

    def last_obj(self, collection: str, *args,
                 timestamp_field: str = "_id",
                 fields: str = None, ignore_fields: str = None, **kwargs):
        """
        Return the last item inserted into the collection

            - args: passed to "self._find_one"
            - timestamp_field: name of timestamp field to sort on
            - fields: string containing fields to return, separated by any of , ; |
            - ignore_fields:
                string containing fields to ignore, separated by any of , ; |
            - kwargs: passed to "self._find_one"
        """
        if "sort" not in kwargs:
            kwargs["sort"] = [(timestamp_field, -1)]
        return self._find_one(
            collection, *args,
            fields=fields, ignore_fields=ignore_fields, **kwargs
        )

    def first_obj(self, collection: str, *args,
                  timestamp_field: str = "_id",
                  fields: str = None, ignore_fields: str = None, **kwargs):
        """
        Return the last item inserted into the collection

        - args: passed to "self._find_one"
        - timestamp_field: name of timestamp field to sort on
        - fields: string containing fields to return, separated by any of , ; |
        - ignore_fields:
            string containing fields to ignore, separated by any of , ; |
        - kwargs: passed to "self._find_one"
        """
        if "sort" not in kwargs:
            kwargs["sort"] = [(timestamp_field, 1)]
        return self._find_one(
            collection, *args,
            fields=fields, ignore_fields=ignore_fields, **kwargs
        )

    def head_objs(self, collection: str, match: dict = {}, to_list: bool = True, head: int = 10, days_ago: int = 3,
                  fields: str = None, ignore_fields: str = None):
        """
        Return a set or list of the five earliest items as of xxx days ago
        """
        query_dict = get_days_ago_query(days_ago, until_days_ago=0)
        match = dict(match, **query_dict)
        kwargs = {
            "sort": [("_id", 1)],
            "limit": head
        }
        return self._find(collection, match, to_list=to_list, fields=fields, ignore_fields=ignore_fields, **kwargs)

    def tail_objs(self, collection: str, match: dict = {}, to_list: bool = True, tail: int = 10, fields: str = None,
                  ignore_fields: str = None):
        """
        Return a set or list of the five latest items
        """
        # query_dict = get_days_ago_query(days_ago, until_days_ago=0)
        kwargs = {
            "sort": [("_id", -1)],
            "limit": tail
        }
        return self._find(collection, match, to_list=to_list, fields=fields, ignore_fields=ignore_fields, **kwargs)

    def days_ago_objs(self, collection: str, match: dict = {}, to_list: bool = True, days_ago: int = 0,
                      until_days_ago: int = 0,
                      fields: str = None,
                      ignore_fields: str = None):
        query_dict = get_days_ago_query(days_ago, until_days_ago)
        match = dict(match, **query_dict)
        kwargs = {
            "sort": [("_id", -1)],
        }
        return self._find(collection, match, to_list=to_list, fields=fields, ignore_fields=ignore_fields, **kwargs)

    def hours_ago_objs(self, collection: str, match: dict = {}, to_list: bool = True, hours_ago: int = 1,
                       until_hours_ago: int = 0,
                       fields: str = None,
                       ignore_fields: str = None):
        query_dict = get_hours_ago_query(hours_ago, until_hours_ago)
        match = dict(match, **query_dict)
        kwargs = {
            "sort": [("_id", -1)],
        }
        return self._find(collection, match, to_list=to_list, fields=fields, ignore_fields=ignore_fields, **kwargs)

    def minutes_ago_objs(self, collection: str, match: dict = {}, to_list: bool = True, minutes_ago: int = 1,
                         until_minutes_ago: int = 0,
                         fields: str = None,
                         ignore_fields: str = None):
        query_dict = get_minutes_ago_query(minutes_ago, until_minutes_ago)
        match = dict(match, **query_dict)
        # print(match)
        kwargs = {
            "sort": [("_id", -1)],
        }
        return self._find(collection, match, to_list=to_list, fields=fields, ignore_fields=ignore_fields, **kwargs)

    def all_obj_id(self, collection: str, match: dict, res_type: str = "set"):
        """
        Return a set or list of ObjectIds of matching
            - match: limit document matching conditions
        ps: Author think the list of id should be a set...
            but possibly it also can be a list in some scenes
        """
        if res_type == "list":
            return list([
                it["_id"]
                for it in self._find(collection, match, projection=["_id"])
            ])
        elif res_type == "set":
            return set([
                it["_id"]
                for it in self._find(collection, match, projection=["_id"])
            ])
        else:
            return set([
                it["_id"]
                for it in self._find(collection, match, projection=["_id"])
            ])

    def delete_one(self, collection: str, match: dict):
        """
        Delete an item asynchronously, and return the number of deletions.
        """
        get_future = asyncio.ensure_future(self._delete_one(collection, match))
        result = self._loop.run_until_complete(get_future)
        return get_future.result()

    def delete_many(self, collection: str, match: dict):
        """
        Delete several documents asynchronously, and return the number of deletions.
        """
        get_future = asyncio.ensure_future(self._delete_many(collection, match))
        result = self._loop.run_until_complete(get_future)
        return get_future.result()

    def insert_one(self, collection: str, document: dict, lazy_tag: bool = True):
        """
        Insert a document asynchronously, and return the number of deletions.
            - lazy_tag: If True, inserting will not be called right now.
                If False, it will be called asynchronously right now.
        """
        if lazy_tag is True:
            res = self.add_task(self._insert_one, collection=collection, document=document)
            return res
        else:
            return self._insert_one(collection, document)

    def insert_many(self, collection: str, documents: List[dict], lazy_tag: bool = True):
        """
        Insert several documents asynchronously, and return the number of deletions.
            - lazy_tag: If True, inserting will not be called right now.
                If False, it will be called asynchronously right now.
        """
        if lazy_tag is True:
            res = self.add_task(self._insert_many, collection=collection, documents=documents)
            return res
        else:
            return self._insert_many(collection, documents)

    def add_task(self, func: Callable, **kwargs):
        """
        Add a task to the task list, but do not run the tasks right now.
        """
        self._tasks.append(asyncio.ensure_future(func(**kwargs)))
        if len(self._tasks) >= 20:
            self.commit_tasks()
        return len(self._tasks)

    async def _ready_commit(self):
        """
        For getting compete ans that part the readying and committing.
        """
        complete, pending = await asyncio.wait(self._tasks)
        self._tasks_res = {}
        for it in complete:
            self._tasks_res["result_data"] = it.result()
            self._tasks_res["result_type"] = type(it.result())
            # pass

    def commit_tasks(self):
        """
        Run the callers from the task list, and clear the list soon.
        When the query related method is called, this method will be called definitely.
        """
        # tasks_cnt = 0
        # print(f"len: {len(self._tasks)}")
        try:
            if len(self._tasks) > 0:
                self._loop.run_until_complete(self._ready_commit())
                self._tasks.clear()
        except Exception as e:
            raise e
        return self.get_tasks_res(destroy=False)

    def clear_task_res(self):
        """
        Clear the task res which was recorded previously.
        """
        self._tasks_res = None
        return 1

    def get_tasks_res(self, destroy: bool = True):
        """
        Get the task res.
            - destroy: If true, the res will be cleared when it has been taken away.
        """
        if destroy is True:
            res = self._tasks_res
            self.clear_task_res()
            return res
        else:
            return self._tasks_res

    def __del__(self):
        if len(self._tasks) > 0 and self._loop is not None:
            self.commit_tasks()

        self._loop.close()
        self._tasks.clear()
        self._tasks_res = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        else:
            pass


if __name__ == "__main__":
    # mongo_config = MongoConfig("127.0.0.1", 27017, "test")
    mongo_config = MongoConfig("mongodb://127.0.0.1", 27017, "test", "root", "watermelon233")
    # mongo_client = MgClient(mongo_config)
    # print(mongo_client.timest)
    # conn = mongo_client.db_connection()
    # # time.sleep(3)
    # # mongo_client.reconnect()
    # # ans = mongo_client.get_db()
    # print(mongo_client.get_collection_names())
    # print(mongo_client.db_stats())
    # cur_db = mongo_client.get_db()
    # cur_db.test_set.save({"name":"r0", "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))})
    # cur_db.test_set.save({"name":"r0", "timestamp": datetime.utcnow().strftime("%d %H:%M:%S")})
    # print(json.dumps(list(ans["test_set"].find())))
    # ans["test_set"].drop()
    # mongo_tools = MongoTools(mongo_client)
    # # mongo_client.close()
    # del mongo_client
    # gc.collect()
    # mongo_tools.test()

    mongo_tools = MongoTools(mongo_config)
    ans = get_hours_ago_query(3, 0)
    print(ans)
    # for i in range(10):
    #     mongo_tools._insert_one("test_set",
    #                         {
    #                             "name": "qwq",
    #                             "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))
    #                         })
    #     time.sleep(2)
    print(mongo_tools.last_obj("test_set", fields="_id | name | timestamp"))
    print(mongo_tools.first_obj("test_set", fields="_id | name | timestamp"))
    print(mongo_tools.all_obj_id("test_set", match=ans))
    # print(mongo_tools._distinct("test_set", key="timestamp", match=ans))

    print(mongo_tools.documents_total("test_set"))
    # ans = mongo_tools._create_index("test_set", [("name", 1)])
    # print(ans)
    # mongo_tools.add_task(mongo_tools._delete_one("test_set", {"name": "qwq"}))
    # mongo_tools.add_task(mongo_tools._delete_one("test_set", {"name": "qwq"}))
    print(mongo_tools.first_obj("test_set", fields="_id | name | timestamp", hint=[("name", 1)]))
    mongo_tools.commit_tasks()
    ret = mongo_tools.insert_one(
        "test_set", {"name": "qwq", "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))}
    )
    print(ret)
    ret = mongo_tools.insert_many(
        "test_set",
        [
            {
                "name": "qwq1",
                "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))
            },
            {
                "name": "qwq2",
                "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))
            },
            {
                "name": "qwq3",
                "timestamp": time.strftime("%d %H:%M:%S", time.localtime(time.time()))
            },
        ]
    )
    print(ret)
    print(mongo_tools.get_tasks_res())
    print(mongo_tools.first_obj("test_set", fields="_id | name | timestamp"))
    print(mongo_tools.get_tasks_res())
    print(mongo_tools.documents_total("test_set"))
    print(mongo_tools.head_objs("test_set", match={
        "name": "qwq"
    }, fields="timestamp", head=10))
    print(mongo_tools.tail_objs("test_set", match={
        "name": "qwq3"
    }, fields="timestamp", tail=10))
    print(mongo_tools.days_ago_objs("test_set", match={
        "name": "qwq"
    }, fields="timestamp", days_ago=2))
    print(mongo_tools.hours_ago_objs("test_set", match={
        "name": "qwq1"
    }, fields="timestamp", hours_ago=24))
    print(mongo_tools.minutes_ago_objs("test_set", match={
        "name": "qwq2"
    }, fields="timestamp", minutes_ago=220))
    print(mongo_tools.documents_total("test_set"))
