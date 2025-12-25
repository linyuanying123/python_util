import pymysql
import logging


def query_batch_deal(connection, deal_flag):
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        query = "SELECT * FROM batch_deal_config WHERE DEAL_FLAG = %s and STATUS = '0' ORDER BY SUBS_UNIQUE_ID"
        cursor.execute(query, (deal_flag,))
        results = cursor.fetchall()
        cursor.close()
        return results
    except pymysql.Error as e:
        logging.error(f"Error querying batch_deal_config: {e}")
        return []


def execute_query(connection, query):
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        cursor.close()
        return results, column_names
    except pymysql.Error as e:
        logging.error(f"Error executing query: {e}")
        return [], []