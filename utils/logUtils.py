import logging
import os
from migration_tools.utils import sysUtils


def setup_logging():
    """
    配置全局的日志记录设置，包括日志级别、输出格式以及输出到控制台等。
    """
    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 创建控制台处理器（用于将日志输出到控制台）
    console_handler = logging.StreamHandler()

    # 使用绝对路径确保日志文件位置正确
    log_file_path = sysUtils.resource_path('App.log')

    # 配置日志记录基本设置
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        filemode='a', filename=log_file_path)
    # 定义日志格式
    formatter = logging.Formatter("\033[31m" + '%(asctime)s - %(levelname)s - %(message)s' + "\033[0m")
    console_handler.setFormatter(formatter)

    # 将控制台处理器添加到根日志记录器
    logger.addHandler(console_handler)

