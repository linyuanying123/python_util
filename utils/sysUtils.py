import time
import sys
import os
import logging
from migration_tools.constants import MESSAGES


def resource_path(relative_path):
    """
    获取资源的绝对路径，适用于开发环境和PyInstaller打包环境。
    打包后，资源文件（如db_config.txt, import_excel/）应与exe文件在同一目录下。
    """
    if getattr(sys, 'frozen', False):
        # 如果是打包后的 exe
        base_path = os.path.dirname(sys.executable)
    else:
        # 如果是直接运行的 .py 脚本
        # 如果是直接运行的 .py 脚本,基路径应该是项目根目录
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    return os.path.join(base_path, relative_path)


def exit_handle():
    """
    处理程序退出，提示用户并确保日志被写入。
    """
    logging.info(MESSAGES.EXIT_HANDLE_INFO)
    time.sleep(0.1)
    # 移除了重启功能以避免循环依赖和无限递归
    input(MESSAGES.RETURN_BACK)
    # logging.shutdown()  # 确保所有挂起的日志都写入文件
    # sys.exit(0)
