import time

from migration_tools.constants import MESSAGES
from migration_tools.config import mysql_config
from migration_tools.dao import mysqlBatchExecuteDao
from migration_tools.utils import sqlUtils, sysUtils
import logging
import datetime


def mysql_batch_execute():
    logging.info(MESSAGES.MYSQL_BATCH_EXECUTE_INFO)
    time.sleep(0.1)
    connection = mysql_config.get_mysql_connection()
    if connection is None:
        logging.error("数据库连接失败，程序退出。")
        # sysUtils.exit_handle()
        return

    if not mysqlBatchExecuteDao.check_table_existence(connection, 'batch_deal_config') \
            or not mysqlBatchExecuteDao.check_table_existence(connection, 'data_deal_prc_log'):
        connection.close()
        print(MESSAGES.MBE_MISS_TABLE)
        print(MESSAGES.BATCH_DEAL_CONFIG_SQL)
        print(MESSAGES.DATA_DEAL_PRC_LOG_SQL)
        # sysUtils.exit_handle()
        return None
    mysqlBatchExecuteDao.query_deal_flag(connection, '2')
    deal_flag = input(MESSAGES.INPUT_EXECUTE_FLAG)
    is_fresh = input(f">>> 执行标识{deal_flag},是否需要刷新可执行状态 (yes or y / no or n):")
    if is_fresh == 'yes' or is_fresh == 'y':
        mysqlBatchExecuteDao.fresh_staus(connection, deal_flag, 0)
    results = mysqlBatchExecuteDao.query_batch_deal(connection, deal_flag)
    if not results:
        connection.close()
        logging.info(f"根据执行标识'{deal_flag}'查询不到待执行的数据.\n")
        time.sleep(0.1)
        # sysUtils.exit_handle()
        return None
    else:
        logging.info(f"根据执行标识'{deal_flag}'查询到【{len(results)}】记录. \n")
        time.sleep(0.1)
        proceed = input(MESSAGES.IF_NOT_GO_ON_MYSQL).strip().lower()
        if proceed != 'yes' and proceed != 'y':
            connection.close()
            # sysUtils.exit_handle()
            return None
    for row in results:
        unique_id = row['UNIQUE_ID']
        deal_sql = row['DEAL_SQL']
        subs_unique_id = row['SUBS_UNIQUE_ID']
        begin_time = datetime.datetime.now()
        logging.info(f"[ID:{unique_id}][顺序号{subs_unique_id}]:执行语句为--> {deal_sql}")
        success, remark = sqlUtils.execute_sql(connection, deal_sql)
        end_time = datetime.datetime.now()
        status = '2' if success else '-1'
        # 更新状态
        mysqlBatchExecuteDao.update_batch_deal_status(connection, unique_id, status, end_time, remark)
        # 记录入log表
        mysqlBatchExecuteDao.log_execution(connection, unique_id, deal_flag, deal_sql, begin_time, end_time)
        logging.info(f"Execution {'succeeded' if success else 'failed'} for UNIQUE_ID {unique_id}.")
    connection.close()
    logging.info(MESSAGES.EXECUTION_COMPLETED_INFO)
    time.sleep(0.1)
    # sysUtils.exit_handle()
    return None
