import time

from migration_tools.constants import MESSAGES
from migration_tools.service import excelImportService,mysqlBatchExecuteService,excelSplitColumnService,deleteRowsService,mysqlQueryToExcelService,tableMigrationService,directSqlQueryToExcelService,excelInsertService
from migration_tools.utils import logUtils
import logging



def choose_menu():
    print(MESSAGES.CHOOSE_FUNCTION_INFO)
    choice = input(">>> 请输入选项（1/2/3/4/5/6/7/8/9/99）: ")
    if choice == "1":
        excelImportService.excel_to_mysql()
    elif choice == "2":
        mysqlBatchExecuteService.mysql_batch_execute()
    elif choice == "3":
        excelImportService.excel_batch_to_mysql()
    elif choice == "4":
        excelSplitColumnService.excel_to_Split()
    elif choice == "5":
        deleteRowsService.delete_rows_entrypoint()
    elif choice == "6":
        mysqlQueryToExcelService.mysql_query_to_excel()
    elif choice == "7":
        tableMigrationService.migrate_table_data()
    elif choice == "8":
        directSqlQueryToExcelService.direct_sql_query_to_excel()
    elif choice == "9":
        excelInsertService.excel_to_existing_table()
    elif choice == "99":  # 退出
        print(MESSAGES.EXIT_HANDLE_INFO)
        raise SystemExit
    else:
        print(MESSAGES.RE_CHOOSE_INFO)
        choose_menu()


if __name__ == "__main__":
    logUtils.setup_logging()
    logger = logging.getLogger()
    print(MESSAGES.PROGRAM_START_INFO)
    logging.info("程序开始了！！！！")
    time.sleep(0.1)
    while True:
        try:
            choose_menu()
        except SystemExit:
            logging.info("程序退出。")
            break
        except Exception as e:
            logging.error(f"程序发生未处理的异常: {e}", exc_info=True)
        finally:
            input("\n操作完成，按任意键返回主菜单...")


