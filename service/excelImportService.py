import logging
import time

from migration_tools.constants import MESSAGES
from migration_tools.config import mysql_config
from migration_tools.dao import excelImportDao
import mysql.connector
from migration_tools.utils import sqlUtils, excelUtils, sysUtils


def excel_to_mysql():
    logging.info(MESSAGES.EXCEL_IMPORT_INFO)
    time.sleep(0.1)
    excel_files = excelUtils.find_excel_file()
    if not excel_files:
        return
    file_path = excelUtils.user_choose_file(excel_files)

    # 加载文件数据
    df = excelUtils.load_data(file_path)
    if df is None:
        logging.error("无法加载文件数据，请检查文件格式或内容。\n")
        return
    logging.info("数据加载成功！\n")
    time.sleep(0.1)

    conn = mysql_config.get_mysql_connection()

    # 获取表名
    while True:
        table_name = input(MESSAGES.INPUT_MYSQL_TABLE).strip()
        if table_name:
            break
        print("表名不能为空，请重新输入。")

    # 检查表是否已存在
    cursor = conn.cursor()
    if excelImportDao.check_table_exists(cursor, table_name):
        print(f"表 `{table_name}` 已存在，程序将退出。\n")
        cursor.close()
        conn.close()
        # sysUtils.exit_handle()
        return None

    # 生成并执行创建表 SQL
    create_sql = excelImportDao.generate_create_table_sql(table_name, df)
    print(f"create table >>> \n{create_sql}\n")
    create_sql = sqlUtils.input_create_table(table_name, create_sql)

    try:
        cursor.execute(create_sql)
        logging.info(f"表 `{table_name}` 创建成功！\n")
        time.sleep(0.1)
    except mysql.connector.Error as err:
        logging.error(f"创建表时发生错误: {err}\n")
        cursor.close()
        conn.close()
        return

    excelImportDao.import_data_to_mysql(conn, table_name, df)
    cursor.close()
    conn.close()
    time.sleep(0.1)
    # sysUtils.exit_handle()


def excel_batch_to_mysql():
    logging.info(MESSAGES.EXCEL_BATCH_IMPORT_INFO)
    time.sleep(0.1)
    excel_files = excelUtils.find_excel_file_V2()
    if not excel_files:
        return
    # excelUtils.print_file_list(excel_files)
    conn = mysql_config.get_mysql_connection()
    cursor = conn.cursor()
    need_control_file_name = input(">>> 请输出需要操作的file_name[全部输入all,单个就是具体的文件名]:")
    if need_control_file_name == 'all':
        cursor.execute(
            "SELECT file_name, sheet_name, new_table_name as tableName, new_create_sql as createSql, operator, status, remarks FROM batch_excel_deal_config where status = 0")
    else:
        cursor.execute(
            f"SELECT file_name, sheet_name, new_table_name as tableName, new_create_sql as createSql, operator, status, remarks FROM batch_excel_deal_config where file_name = '{need_control_file_name}' and status = 0")
    excel_files_list = cursor.fetchall()
    for row in excel_files_list:
        file_name, sheet_name, tableName, createSql, operator, status, remarks = row
        logging.info(f"开始操作-->{file_name} [{sheet_name}]")
        file_path = excelUtils.map_excel_file(file_name)
        # 加载文件数据
        df = excelUtils.load_data_v2(file_path, sheet_name)
        if df is None:
            logging.error("无法加载文件数据，请检查文件格式或内容。")
            continue
        logging.info("数据加载成功！")

        # 检查表是否已存在
        cursor = conn.cursor()
        if excelImportDao.check_table_exists(cursor, tableName):
            logging.error(f"表 `{tableName}` 已存在\n")
            continue
        create_sql = excelImportDao.generate_create_table_sql(tableName, df) if createSql is None or createSql == '' else createSql
        if createSql is not None or createSql != '':
            flag = sqlUtils.check_create_table(create_sql, tableName)
            if flag is False:
                logging.info(createSql)
                logging.error("请检查batch_excel_deal_config表里所填写的语句是否符合规范！\n")
                continue
        logging.info(f"create table >>>{create_sql}")
        try:
            cursor.execute(create_sql)
            logging.info(f"表 `{tableName}` 创建成功！")
        except mysql.connector.Error as err:
            logging.error(f"创建表时发生错误: {err}\n")
            continue
        excelImportDao.import_data_to_mysql_V2(conn, tableName, df)
    cursor.close()
    conn.close()
    time.sleep(0.1)
    # sysUtils.exit_handle()