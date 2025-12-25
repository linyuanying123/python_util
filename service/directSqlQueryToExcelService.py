import logging
import sys
import time
import datetime

from migration_tools.config import mysql_config
from migration_tools.dao import directSqlQueryToExcelDao
from migration_tools.utils import excelUtils
from migration_tools.constants import MESSAGES


def direct_sql_query_to_excel():
    """
    Allows the user to input a SQL query directly, executes it, and saves the results to an Excel file.
    This service is designed to handle large datasets by streaming results.
    """
    logging.info("开始执行直接SQL查询并导出到Excel...")
    time.sleep(0.1)

    # 1. Get Database Connection
    connection = mysql_config.get_mysql_connection()
    if connection is None:
        logging.error("数据库连接失败，程序退出。")
        return

    logging.info(f"DEBUG: 当前连接的数据库信息: Host={connection.host}, Database={connection.db}")

    # 2. Get SQL query from user
    print("\n请输入您要执行的SQL查询语句 (粘贴后，在新的一行按回车键确认):")
    lines = []
    while True:
        try:
            line = input()
            if not line:  # Break on empty line
                break
            lines.append(line)
        except EOFError:  # Also handle Ctrl+Z/D gracefully
            break
    user_sql = " ".join(lines).strip()

    # Remove the trailing semicolon if it exists, as user might still paste it
    if user_sql.endswith(';') or user_sql.endswith('；'):
        user_sql = user_sql[:-1].strip()

    if not user_sql:
        logging.warning("未输入任何SQL语句，操作取消。")
        connection.close()
        return

    # 3. Get page size from user
    try:
        page_size_str = input(">>> 请输入每个分页的行数 (默认 100000): ").strip()
        page_size = int(page_size_str) if page_size_str else 100000
    except ValueError:
        logging.warning("无效的行数，将使用默认值 100000。")
        page_size = 100000

    # 4. Execute query and save to Excel with pagination
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"direct_query_result_{timestamp}.xlsx"
    
    logging.info(f"查询将以每页 {page_size} 行进行分页，结果将保存到: {output_filename}")

    total_rows = excelUtils.save_query_to_excel_paginated(
        connection=connection,
        base_query=user_sql,
        filename=output_filename,
        page_size=page_size
    )

    if total_rows > 0:
        logging.info(f"成功将 {total_rows} 条记录写入Excel文件。")
    else:
        logging.info("查询没有返回任何数据，或写入过程中发生错误。")

    # 5. Clean up
    connection.close()
    logging.info("直接SQL查询并导出到Excel的操作已完成。")
    time.sleep(0.1)