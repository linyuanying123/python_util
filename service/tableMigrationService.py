import logging
import time

from migration_tools.config import mysql_config
from migration_tools.dao import tableMigrationDao
from migration_tools.constants import MESSAGES


def migrate_table_data():
    """数据迁移主函数"""
    logging.info("开始表数据迁移...")
    time.sleep(0.1)

    # 获取源数据库连接
    print("\n--- 选择源数据库 ---")
    source_conn = mysql_config.get_mysql_connection()
    if source_conn is None:
        logging.error("源数据库连接失败，程序退出。")
        return

    # 获取源表名
    source_table_name = input(">>> 请输入源表名: ").strip()
    source_columns = tableMigrationDao.get_table_columns(source_conn, source_table_name)
    if not source_columns:
        logging.error(f"无法获取源表 '{source_table_name}' 的列信息，请检查表是否存在。")
        source_conn.close()
        return

    # 获取目标数据库连接
    print("\n--- 选择目标数据库 ---")
    target_conn = mysql_config.get_mysql_connection()
    if target_conn is None:
        logging.error("目标数据库连接失败，程序退出。")
        source_conn.close()
        return

    # 获取目标表名
    target_table_name = input(">>> 请输入目标表名: ").strip()
    target_columns = tableMigrationDao.get_table_columns(target_conn, target_table_name)
    if not target_columns:
        logging.error(f"无法获取目标表 '{target_table_name}' 的列信息，请检查表是否存在。")
        source_conn.close()
        target_conn.close()
        return

    # 比较表结构
    if set(source_columns) != set(target_columns):
        logging.warning("源表和目标表的列不完全匹配。")
        logging.warning(f"源表列: {source_columns}")
        logging.warning(f"目标表列: {target_columns}")
        proceed = input(">>> 列不匹配，是否仍然继续？ (yes/y): ").strip().lower()
        if proceed not in ['yes', 'y']:
            source_conn.close()
            target_conn.close()
            return

    # 从源表获取数据
    logging.info(f"正在从源表 '{source_table_name}' 读取数据...")
    data_to_migrate = tableMigrationDao.get_all_data_from_table(source_conn, source_table_name)
    if data_to_migrate is None:
        logging.error("从源表读取数据失败。")
        source_conn.close()
        target_conn.close()
        return

    if not data_to_migrate:
        logging.info("源表没有数据需要迁移。")
        source_conn.close()
        target_conn.close()
        return

    logging.info(f"成功从源表读取 {len(data_to_migrate)} 条数据。")

    # 批量插入数据到目标表
    logging.info(f"正在向目标表 '{target_table_name}' 插入数据...")
    success, inserted_rows = tableMigrationDao.batch_insert_data(target_conn, target_table_name, data_to_migrate)

    if success:
        logging.info(f"成功迁移 {inserted_rows} 条数据到目标表 '{target_table_name}'。")
    else:
        logging.error("数据迁移过程中发生错误。")

    # 关闭连接
    source_conn.close()
    target_conn.close()
    logging.info("数据迁移完成。")

def show_table_structure():
    """显示表的结构"""
    logging.info("开始显示表结构...")
    time.sleep(0.1)

    # 获取数据库连接
    conn = mysql_config.get_mysql_connection()
    if conn is None:
        logging.error("数据库连接失败，程序退出。")
        return

    # 获取表名
    table_name = input(">>> 请输入要查看结构的表名: ").strip()
    if not table_name:
        logging.warning("未输入表名，操作取消。")
        conn.close()
        return

    # 获取表结构
    structure = tableMigrationDao.describe_table(conn, table_name)
    conn.close()

    if structure:
        print(f"\n--- 表 '{table_name}' 的结构 ---")
        # 打印表头
        headers = structure[0].keys()
        print(" | ".join(f"{h:<20}" for h in headers))
        print("-" * (23 * len(headers)))
        # 打印每一行
        for row in structure:
            print(" | ".join(f"{str(v):<20}" for v in row.values()))
        print("-------------------------------------\n")
    else:
        logging.warning(f"无法获取表 '{table_name}' 的结构，请检查表是否存在。")
