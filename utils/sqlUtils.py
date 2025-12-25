import time

from mysql.connector import Error
import logging
import sqlparse
from migration_tools.constants import MESSAGES
import re


def execute_sql(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        return True, None
    except Error as e:
        logging.error(f"   SQL Execution Error: {e}")
        time.sleep(0.1)
        return False, e.msg


def check_create_table(create_sql, original_table_name):
    """ 校验建表语句"""
    parsed = sqlparse.parse(create_sql)
    if len(parsed) > 0 and parsed[0].get_type() == "CREATE":
        table_name = get_table_name(create_sql)
        if table_name == original_table_name:
            return True
        else:
            logging.info("☆ 表名与原来不一致，请重新输入。")
            time.sleep(0.1)
            return False
    else:
        return False


def get_table_name(create_sql):
    # 正则表达式匹配建表语句中的表名
    pattern = r"CREATE\s+TABLE\s+([^\s.]+)\s*"
    match = re.search(pattern, create_sql, re.IGNORECASE)
    if match:
        return match.group(1).replace("`", "")
    else:
        return None


def input_create_table(table_name, create_sql):
    create_table_before_flag = input(MESSAGES.CREATE_TABLE_BEFORE)
    if create_table_before_flag == 'no' or create_table_before_flag == 'n':
        print("\n☆ 现在进行修改语句操作！")
        print("\n☆ 需要复制原本的建表语句，表名和字段名不许变，非建表语句不允许执行")
        new_sql = input(MESSAGES.INPUT_CREATE_TABLE)
        flag = check_create_table(new_sql, table_name)
        if flag is True:
            return new_sql
        else:
            logging.info("☆ 请检查语句！")
            time.sleep(0.1)
            return input_create_table(table_name, create_sql)
    else:
        return create_sql
