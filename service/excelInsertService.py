import logging
import time

from migration_tools.constants import MESSAGES
from migration_tools.config import mysql_config
from migration_tools.dao import excelImportDao
from migration_tools.utils import excelUtils

def excel_to_existing_table():
    """
    将Excel数据导入到【已存在】的MySQL表中。
    字段以Excel文件的列标题为准。
    """
    logging.info("--- 将Excel数据导入到【已存在】的MySQL表 ---")
    time.sleep(0.1)

    # 1. 选择并加载Excel文件
    excel_files = excelUtils.find_excel_file()
    if not excel_files:
        return
    file_path = excelUtils.user_choose_file(excel_files)

    df = excelUtils.load_data(file_path)
    if df is None:
        logging.error("无法加载文件数据，请检查文件格式或内容。\n")
        return
    logging.info("数据加载成功！\n")
    time.sleep(0.1)

    # 2. 获取数据库连接
    conn = mysql_config.get_mysql_connection()
    if not conn:
        logging.error("获取数据库连接失败。")
        return
    
    cursor = conn.cursor()

    # 3. 获取并验证表名
    while True:
        table_name = input(MESSAGES.INPUT_MYSQL_TABLE).strip()
        if table_name:
            break
        print("表名不能为空，请重新输入。")

    if not excelImportDao.check_table_exists(cursor, table_name):
        logging.error(f"表 `{table_name}` 不存在，程序将退出。\n")
        cursor.close()
        conn.close()
        return

    logging.info(f"表 `{table_name}` 存在，准备导入数据...")

    # 4. 调用现有的DAO函数导入数据
    # 这个函数会根据DataFrame的列自动生成INSERT语句
    excelImportDao.import_data_to_mysql(conn, table_name, df)

    # 5. 清理
    # cursor.close() # import_data_to_mysql 内部会关闭 cursor
    conn.close()
    time.sleep(0.1)