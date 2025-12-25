PROGRAM_START_INFO = """☆ 开发人员: 吴日彬
☆ 脚本名称: migration_tools_runs 
☆ 创建时间: 2024-11-19                                                                                                                                                                                                           
            """

CHOOSE_FUNCTION_INFO = """
☆ 请选择需要执行的功能
☆ 1. EXCEL/CSV文件单个快速导入数据库功能【DIY】
☆ 2. MYSQL语句批量执行功能【预先配置batch_deal_config表】
☆ 3. Excel/CSV文件自动化批量导入数据库功能【预先配置batch_excel_deal_config表】
☆ 4. Excel/CSV文件按指定列根据逗号切割分行功能【DIY】
☆ 5. Excel/CSV文件删除指定sheet带删除线（依据第一、二或三列带删除线)的记录功能【DIY】
☆ 6. MYSQL语句批量执行功能，结果保存到Excel文件中【预先配置batch_deal_config表】
☆ 7. 数据库表数据迁移功能【DIY】
☆ 8. 自定义SQL查询导出excel【DIY】
☆ 9. 自定义SQL导入excel数据到存在的表【DIY】
☆ 99. 退出
"""

BATCH_DEAL_CONFIG_SQL = """
  CREATE TABLE BATCH_DEAL_CONFIG 
   (	
    UNIQUE_ID int(9) NOT NULL COMMENT '主键', 
	SUBS_UNIQUE_ID int(9) NOT NULL COMMENT '排序编码', 
	DEAL_NAME varchar(400)  DEFAULT NULL COMMENT '执行脚本简单描述',
	DEAL_FLAG varchar(120) NOT NULL COMMENT '执行标识，作为运行标志',
	DEAL_SQL varchar(4000) NOT NULL COMMENT '需要执行的sql',
	EXE_TIME  datetime DEFAULT NULL COMMENT '最近执行时间', 
	STATUS varchar(2) NOT NULL COMMENT '状态 -1 执行报错,0:待执行，2:执行成功,其它不执行', 
	REMARK varchar(120) DEFAULT NULL COMMENT '备注'
   )
"""

DATA_DEAL_PRC_LOG_SQL = """
  CREATE TABLE DATA_DEAL_PRC_LOG 
   (	
    UNIQUE_ID int(9) NOT NULL, 
	DEAL_FLAG varchar(120) NOT NULL COMMENT '执行标识，作为运行标志',
	DEAL_SQL varchar(4000) NOT NULL COMMENT '执行的sql',
	CREATE_TIME datetime DEFAULT NULL COMMENT '创建时间',  
	BEGIN_EXE_TIME datetime DEFAULT NULL COMMENT '开始执行时间',
	END_EXE_TIME datetime DEFAULT NULL COMMENT '执行结束创建时间'
   ) 
"""

EXIT_HANDLE_INFO = """☆ 程序已经结束 ☆\n """

EXECUTION_COMPLETED_INFO = "☆ 执行完毕\n"
RE_CHOOSE_INFO = "☆ 无效选项，请重新输入\n"
MYSQL_BATCH_EXECUTE_INFO = "执行mysql批量执行功能板块\n"
MBE_MISS_TABLE = "☆ 缺失batch_deal_config或者data_deal_prc_log数据表,退出程序\n"
MYSQL_TABLE_LIST = "☆ 可用的数据库列表："
MYSQL_TABLE_INPUT = ">>> 请输入要连接的数据库编号: "
INPUT_EXECUTE_FLAG = ">>> 请输入执行标识【batch_deal_config表中DEAL_FLAG字段】: "
IF_NOT_GO_ON_MYSQL = ">>> 是否继续执行返回结果中的SQL语句? (yes or y / no or n):"
IF_NOT_GO_ON_PROGRAM = ">>> 程序已经结束,是否重新启动程序? (yes or y / no or n): "
RESTART_INFO = "☆ 程序重新启动！！！ \n"
RETURN_BACK = "☆ 回车退出！\n"
EXCEL_IMPORT_INFO = "开启根据excel[xls、xlsx、csv格式]快速导入mysql数据库功能\n"
EXCEL_SPLIT_COL = "开启根据excel[xls、xlsx、csv格式]快速按逗号切割列分行功能\n"
PRINT_FILE_INFO = "☆ 以下是可用的文件："
CHOOSE_USE_FILE = ">>> 请输入要使用的文件编号(例如: 0): "
ERROR_INPUT = "☆ 无效的数字，请重新选择。\n"
INPUT_CHOOSE_SHEET = ">>> 请输入要导入的sheet编号(例如: 0): "
INPUT_MYSQL_TABLE = "☆ 请输入目标表名: "
MYSQL_CREATE_TABLE = "☆ 正在创建表...\n"
CREATE_TABLE_BEFORE = "☆ 建表前确认！！支持修改语句，是否使用默认建表语句? (yes or y / no or n):"
INPUT_CREATE_TABLE = ">>> 请输入建表语句:"
EXCEL_BATCH_IMPORT_INFO = "开启批量快速导入mysql数据库功能\n"