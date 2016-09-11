import os
import sys
import psycopg2
from psycopg2.extras import Json
import json
import logging
import pandas as pd

import toolkit_config


logger = logging.getLogger()

def _get_cursor_and_connection():
    dbconn = psycopg2.connect(toolkit_config.pg_conn_string())
    return dbconn.cursor(), dbconn

def import_json_into_pg(table_name, data, id_getter, create_table=False, skip_duplicates=False):
    cursor, dbconn = _get_cursor_and_connection()
    if create_table:
        cursor.execute("""CREATE TABLE %s (id varchar(64) PRIMARY KEY, data JSONB)""" % table_name)
        dbconn.commit()

    for record in data:
        id = id_getter(record)
        if skip_duplicates:
            cursor.execute("""
                           INSERT INTO """ + table_name + """(id, data)
                           SELECT %s, %s
                           WHERE NOT EXISTS (SELECT 1 FROM """ + table_name +
                           """ WHERE id=%s)""", [id, Json(record), id])
        else:
            cursor.execute("""
               INSERT INTO """ + table_name + """(id, data)
               VALUES (%s, %s)
               """, [id, Json(record)])

    dbconn.commit()

def pg_query(query):
    cursor, _ = _get_cursor_and_connection()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=column_names)

def pg_query_by_id(table_name, id):
    cursor, _ = _get_cursor_and_connection()
    cursor.execute("SELECT data FROM " + table_name + " WHERE id=%s", [id])
    return cursor.fetchone()[0]

def pg_update_record(table_name, id, record):
    cursor, dbconn = _get_cursor_and_connection()
    cursor.execute("UPDATE " + table_name + " SET data=%s WHERE id=%s", [Json(record), id])
    dbconn.commit()
    cursor.close()

