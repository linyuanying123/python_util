import time

import pymysql
import logging
from migration_tools.constants import MESSAGES
from migration_tools.utils import sysUtils


DEFAULT_CONFIG_FILE = 'db_config.txt'


def load_db_configs(config_file=DEFAULT_CONFIG_FILE):
    db_configs = []
    # 使用resource_path确保在任何环境下都能找到配置文件
    resolved_path = sysUtils.resource_path(config_file)
    try:
        with open(resolved_path, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) == 6:
                    db_configs.append({
                        'id': parts[0],
                        'host': parts[1],
                        'port': int(parts[2]),
                        'user': parts[3],
                        'password': parts[4],
                        'database': parts[5]
                    })
    except FileNotFoundError as e:
        logging.error(f"配置文件 {resolved_path} 不存在，请确保 db_config.txt 文件与 exe 文件在同一目录下。")
        # sysUtils.exit_handle()
    return db_configs


def get_mysql_connection():
    db_configs = load_db_configs()
    if not db_configs:
        logging.warning("数据库配置列表为空，无法进行连接。")
        return None

    print(MESSAGES.MYSQL_TABLE_LIST)
    for config in db_configs:
        print(f"☆ [{config['id']}] {config['database']} ({config['host']}:{config['port']}/{config['user']})")

    while True:
        db_id = input(MESSAGES.MYSQL_TABLE_INPUT)
        selected_db = next((cfg for cfg in db_configs if cfg['id'] == db_id), None)
        if selected_db:
            logging.info(f"选择的数据库: {selected_db['database']} ({selected_db['host']}:{selected_db['port']}/{selected_db['user']})")
            break
        print("\n无效的数据库编号，请重新选择。\n")

    try:
        conn = pymysql.connect(
            host=selected_db['host'],
            user=selected_db['user'],
            port=selected_db['port'],
            password=selected_db['password'],
            database=selected_db['database'],
            charset='utf8mb4',
            ssl={'ssl': {'verify_cert': False}}  #禁用证书
        )
        logging.info(f"成功连接到数据库: {selected_db['database']}\n")
        time.sleep(0.1)
        return conn
    except Exception as err:
        # 强制将详细错误写入日志文件，以诊断打包后的闪退问题
        with open('crash_log.txt', 'w', encoding='utf-8') as f:
            import traceback
            f.write(f"数据库连接期间发生严重错误: {str(err)}\n")
            f.write(traceback.format_exc())
        logging.error(f"连接数据库 {selected_db['database']} 时发生严重错误: {err}")
        # sysUtils.exit_handle()
        return None
