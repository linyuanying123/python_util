import time
import logging
import datetime
import pandas as pd

from migration_tools.constants import MESSAGES
from migration_tools.config import mysql_config
from migration_tools.dao import mysqlQueryToExcelDao, mysqlBatchExecuteDao
from migration_tools.utils import excelUtils


def mysql_query_to_excel():
    logging.info("开始执行SQL查询并导出到Excel...")
    time.sleep(0.1)
    connection = mysql_config.get_mysql_connection()
    if connection is None:
        logging.error("数据库连接失败，程序退出。")
        return

    if not mysqlBatchExecuteDao.check_table_existence(connection, 'batch_deal_config'):
        connection.close()
        print(MESSAGES.MBE_MISS_TABLE)
        print(MESSAGES.BATCH_DEAL_CONFIG_SQL)
        return

    mysqlBatchExecuteDao.query_deal_flag(connection, '6')
    deal_flag = input(MESSAGES.INPUT_EXECUTE_FLAG)
    results = mysqlQueryToExcelDao.query_batch_deal(connection, deal_flag)

    if not results:
        connection.close()
        logging.info(f"根据执行标识'{deal_flag}'查询不到待执行的数据。\n")
        time.sleep(0.1)
        return

    logging.info(f"根据执行标识'{deal_flag}'查询到【{len(results)}】条SQL记录. \n")
    time.sleep(0.1)
    proceed = input(MESSAGES.IF_NOT_GO_ON_MYSQL).strip().lower()
    if proceed != 'yes' and proceed != 'y':
        connection.close()
        return

    dataframes = {}
    for row in results:
        unique_id = row['UNIQUE_ID']
        deal_sql = row['DEAL_SQL']
        subs_unique_id = row['SUBS_UNIQUE_ID']
        sheet_name = f"Sheet_{subs_unique_id}"

        logging.info(f"[ID:{unique_id}][顺序号{subs_unique_id}]: 执行查询 --> {deal_sql}")
        query_results, column_names = mysqlQueryToExcelDao.execute_query(connection, deal_sql)

        if query_results:
            df = pd.DataFrame(query_results, columns=column_names)
            dataframes[sheet_name] = df
            logging.info(f"查询成功，获取到 {len(df)} 条记录。")
        else:
            logging.warning(f"查询未返回结果或执行失败: {deal_sql}")

    if dataframes:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
        output_filename = f"econe_query_result_{timestamp}.xlsx"
        excelUtils.save_dataframes_to_excel(dataframes, output_filename)
    else:
        logging.info("没有查询到任何数据以生成Excel文件。")

    connection.close()
    logging.info("SQL查询并导出到Excel的操作已完成。")
    time.sleep(0.1)