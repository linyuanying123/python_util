import pymysql
import logging
import datetime


def check_table_existence(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except pymysql.Error as e:
        logging.error(f"Error checking table existence: {e}")
        return False


def query_deal_flag(connection,menu_choice):
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        query = "SELECT distinct DEAL_FLAG FROM batch_deal_config"
        if menu_choice == '2':
            query += " WHERE DEAL_FLAG NOT LIKE '%_query'"
        elif menu_choice == '6':
            query += " WHERE DEAL_FLAG LIKE '%_query'"
        query += " order by DEAL_FLAG"
        cursor.execute(query)
        results = cursor.fetchall()
        print("☆ [可使用的DEAL_FLAG]:")
        # for row in results:
        #     print(f"    {row['DEAL_FLAG']}")
        output_str = ""
        count = 0  #
        for index, row in enumerate(results):
            output_str += f"  {row['DEAL_FLAG']}"
            count += 1
            if index < len(results) - 1:
                output_str += ", "
            if count % 5 == 0 and index < len(results) - 1:
                output_str += "\n"
        print(output_str)
        print()
        cursor.close()
        return results
    except pymysql.Error as e:
        print(f"Error querying batch_deal_config: {e}")
        return []


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


def update_batch_deal_status(connection, unique_id, status, exe_time, remark):
    try:
        cursor = connection.cursor()
        query = "UPDATE batch_deal_config SET STATUS = %s, EXE_TIME = %s, REMARK = %s WHERE UNIQUE_ID = %s"
        cursor.execute(query, (status, exe_time, remark, unique_id))
        connection.commit()
        cursor.close()
    except pymysql.Error as e:
        print(f"Error updating batch_deal_config: {e}")



def log_execution(connection, unique_id, deal_flag, deal_sql, begin_time, end_time):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO data_deal_prc_log (UNIQUE_ID, DEAL_FLAG, DEAL_SQL, CREATE_TIME, BEGIN_EXE_TIME, END_EXE_TIME)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        create_time = datetime.datetime.now()
        cursor.execute(query, (unique_id, deal_flag, deal_sql, create_time, begin_time, end_time))
        connection.commit()
        cursor.close()
    except pymysql.Error as e:
        print(f"Error logging execution: {e}")


def fresh_staus(connection, deal_flag, status):
    try:
        cursor = connection.cursor()
        query = "UPDATE batch_deal_config SET STATUS = %s, REMARK = %s WHERE DEAL_FLAG = %s AND STATUS IN ('-1','2')"
        cursor.execute(query, (status, '', deal_flag))
        connection.commit()
        cursor.close()
        logging.info("更新可执行状态成功")
    except pymysql.Error as e:
        logging.error(f"Error updating batch_deal_config: {e}")
