import copy
import time
from datetime import datetime
from typing import List, Callable, Union, Tuple

from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId

import dt_helper as dh
import input_helper as ih

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
    if timestamp_field == "_id":
        start = ObjectId.from_datetime(start)
        end = ObjectId.from_datetime(end)
    query[timestamp_field]["$gte"] = start
    query[timestamp_field]["$lt"] = end
    return query


def get_days_ago_query(
        days_ago: int = 0,
        until_days_ago: int = 0,
        timezone: str = "Asia/Shanghai",
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


class MongoConfig(object):
    def __init__(self, host: str, port: int, targetdb: str, username: str, password: str,
                 do_connect: bool = True, max_pool: int = 1, retries: int = 5):
        self.host = host
        self.port = port
        self.targetdb = targetdb
        self.username = username
        self.password = password
        self.do_connect: bool = do_connect
        self.max_pool: int = max_pool
        self.retries: int = retries
        self._i = 0

    def __iter__(self):
        return self

    def __next__(self):
        pass


class MgClient(object):
    """docstring for MgClient"""

    def __init__(self, config: MongoConfig):
        self._time_st = time.strftime("%d %H:%M:%S", time.localtime(time.time()))
        self.config = config

        self.host = self.config.host
        self.port = self.config.port
        self._db = self.config.targetdb
        self.username = self.config.username
        self.password = self.config.password
        self.max_pool = self.config.max_pool

        self.do_connect = self.config.do_connect
        self.retries = self.config.retries

        self._conn = None
        self._alive = False
        self._db_list = None

        self.connect()
        self.auth_if_required()
        try:
            self._new_db(self._db)
            self.update_db_list()
        except Exception as e:
            print(e)
            self._db_list = []

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
                print(f"Mongo Server ping: {res['ok']}")
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
                # if target is None:
                #     self._conn[self._db].authenticate(self.username, self.password)
                # else:
                #     self._conn[target].authenticate(self.username, self.password)
                self._conn["admin"].authenticate(self.username, self.password)
                print("Mongo client admin auth successfully")
            except Exception as e:
                print(f"Reconnect 1: {e}")
                self.reconnect()
                # raise Exception("Auth failed") from e
                raise
        else:
            pass

    def change_db(self, targetdb: str):
        self._db = targetdb

    def get_db_conn(self, targetdb: str):
        """
        Get db is abstract enough.
        Obtaining a set through this class may cause abstraction leakage.
        """
        self.update_db_list()
        if targetdb in self._db_list:
            return self._conn[self._db]
        else:
            return self._new_db(targetdb=targetdb)

    def _new_db(self, targetdb: str):
        self.update_db_list()

        if self._alive:
            if targetdb in self._db_list:
                self._conn[targetdb].authenticate(self.username, self.password)
                print(f"Mongo client {targetdb} auth successfully")
                return self._conn[targetdb]
            else:
                self._conn[targetdb].add_user(
                    name=self.username,
                    password=self.password,
                    roles=[{"role": "dbOwner", "db": targetdb}]
                )
                self._conn[targetdb].authenticate(self.username, self.password)
                # self._conn[targetdb]["dbini"].insert_one({
                #     "create_time": self._time_st
                # })
                print(f"New db: {targetdb}")
                return self._conn[targetdb]

    def get_database_name(self):
        self.update_db_list()
        return self._db_list

    def get_collection_names(self, targetdb: str = None):
        if targetdb is None:
            if self._db:
                return self._conn[self._db].list_collection_names()
        else:
            return self._conn[targetdb].list_collection_names()

    def update_db_list(self):
        self._db_list = self._conn.list_database_names()

    def _command(self, *args, **kwargs):
        """
        Run a db command and return the results
        """
        targetdb = self._db
        self._conn[targetdb].authenticate(self.username, self.password)
        return self._conn[targetdb].command(*args, **kwargs)

    def drop_db(self):
        self._command("dropDatabase")

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


class MongoTools(object):
    DESCENDING: int = DESCENDING

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

    def _insert_one(self, collection: str, document: dict, index_list: List[Tuple[str, int]] = None):
        """
        Add a document to the collection and return a inserted_id
            - document: a dict of info to be inserted
        ps: the returned type is pymongo.results.InsertOneResult
        """
        db = self._db
        try:
            result = self._mongo_client[db][collection].insert_one(document)
        except Exception as e:
            print(e)
            return -1
        else:
            if index_list:
                self.create_unique_index(collection, index_list)
            return result.inserted_id

    def _insert_many(self, collection: str, documents: List[dict], index_list: List[Tuple[str, int]] = None):
        """
        Add several documents to the collection and return a list of inserted_ids
        """
        db = self._db
        try:
            result = self._mongo_client[db][collection].insert_many(documents)
        except Exception as e:
            print(e)
            return -1
        else:
            if index_list:
                self.create_unique_index(collection, index_list)
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

    def _delete_one(self, collection: str, match: dict):
        """
        Delete one matching item from the collection and return num deleted
            - match: a dict of the query matching document to delete
        """
        db = self._db
        result = self._mongo_client[db][collection].delete_one(match)
        return result.deleted_count

    def _delete_many(self, collection: str, match: dict):
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
                      collection: str, index_list: List[Tuple[str, int]], unique: bool = False, ttl: int = None,
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

        try:
            res = self._mongo_client[db][collection].create_index(index_list, **kwargs)
        except Exception as e:
            print(e)
        else:
            return res

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

    def _command(self, *args, **kwargs):
        """
        Run a db command and return the results
        """
        db = self._db
        return self._mongo_client[db].command(*args, **kwargs)

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
        Delete an item ronously, and return the number of deletions.
        """
        self._delete_one(collection, match)

    def delete_many(self, collection: str, match: dict):
        """
        Delete several documents ronously, and return the number of deletions.
        """
        self._delete_many(collection, match)

    def insert_one(self, collection: str, document: dict, index_list: List[Tuple[str, int]] = None):
        """
        Insert a document ronously, and return the number of deletions.
            - lazy_tag: If True, inserting will not be called right now.
                If False, it will be called ronously right now.
        """
        self._insert_one(collection, document, index_list)

    def insert_many(self, collection: str, documents: List[dict], index_list: List[Tuple[str, int]] = None):
        """
        Insert several documents ronously, and return the number of deletions.
            - lazy_tag: If True, inserting will not be called right now.
                If False, it will be called ronously right now.
        """
        self._insert_many(collection, documents, index_list)

    def __del__(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        else:
            pass
