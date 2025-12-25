import pymysql
import logging

def execute_query_stream(connection, query):
    """
    Executes a SQL query using a server-side cursor (SSDictCursor) to handle large datasets efficiently.
    This function yields rows one by one instead of loading them all into memory.

    :param connection: An active database connection.
    :param query: The SQL query to execute.
    :return: A tuple containing the cursor and the column names. Returns (None, []) on error.
    """
    try:
        # Use SSDictCursor for server-side processing, which is memory-efficient for large results.
        cursor = connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute(query)
        column_names = [i[0] for i in cursor.description]
        return cursor, column_names
    except pymysql.Error as e:
        logging.error(f"Error executing streaming query: {e}")
        return None, []