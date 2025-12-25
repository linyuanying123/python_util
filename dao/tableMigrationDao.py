import logging
import pymysql


def get_table_columns(connection, table_name):
    """获取表的列名"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()
        return column_names
    except pymysql.Error as e:
        logging.error(f"获取表 '{table_name}' 的列名时出错: {e}")
        return None


def get_all_data_from_table(connection, table_name):
    """从源表中获取所有数据"""
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        cursor.close()
        return results
    except pymysql.Error as e:
        logging.error(f"从表 '{table_name}' 获取数据时出错: {e}")
        return None


def batch_insert_data(connection, table_name, data):
    """向目标表批量插入数据"""
    if not data:
        return True, 0

    columns = data[0].keys()
    columns_sql = ", ".join([f"`{col}`" for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

    try:
        cursor = connection.cursor()
        # 将字典列表转换为元组列表
        values = [tuple(row[col] for col in columns) for row in data]
        cursor.executemany(sql, values)
        connection.commit()
        inserted_rows = cursor.rowcount
        cursor.close()
        return True, inserted_rows
    except pymysql.Error as e:
        logging.error(f"向表 '{table_name}' 批量插入数据时出错: {e}")
        connection.rollback()
        return False, 0

def describe_table(connection, table_name):
    """获取表的结构信息"""
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"DESCRIBE {table_name}")
        results = cursor.fetchall()
        cursor.close()
        return results
    except pymysql.Error as e:
        logging.error(f"获取表 '{table_name}' 的结构时出错: {e}")
        return None
