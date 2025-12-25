import logging
import time
import os
import pandas as pd

from migration_tools.constants import MESSAGES
from migration_tools.utils import excelUtils


def excel_to_Split():
    logging.info(MESSAGES.EXCEL_SPLIT_COL)
    time.sleep(0.1)
    excel_files = excelUtils.find_excel_file()
    if not excel_files:
        return
    file_path = excelUtils.user_choose_file(excel_files)
    if not file_path:
        return

    # 获取并选择 sheet
    try:
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        if not sheet_names:
            logging.error("Excel文件中没有找到工作表。")
            return
    except Exception as e:
        logging.error(f"无法读取Excel文件的工作表: {e}")
        return

    logging.info("可用的工作表:")
    for idx, sheet in enumerate(sheet_names, start=1):
        logging.info(f"{idx}. {sheet}")

    selected_sheet = ""
    while True:
        try:
            sheet_choice = input(f"\n请输入要操作的工作表编号(1-{len(sheet_names)}): ")
            sheet_index = int(sheet_choice) - 1
            if 0 <= sheet_index < len(sheet_names):
                selected_sheet = sheet_names[sheet_index]
                logging.info(f"已选择工作表: {selected_sheet}")
                break
            else:
                logging.error("输入的数字超出范围，请重新输入。")
        except ValueError:
            logging.error("请输入有效的数字。")

    # 加载选定sheet的数据
    try:
        df = pd.read_excel(file_path, sheet_name=selected_sheet)
        logging.info("数据加载成功！\n")
    except Exception as e:
        logging.error(f"无法加载工作表 '{selected_sheet}' 的数据: {e}\n")
        return

    # 显示所有列并让用户选择
    columns = df.columns.tolist()
    logging.info("可用列列表:")
    for idx, col in enumerate(columns, start=1):
        logging.info(f"{idx}. {col}")

    selected_column = ""
    while True:
        try:
            col_choice = input(f"\n请输入要操作的列编号(1-{len(columns)}): ")
            col_index = int(col_choice) - 1
            if 0 <= col_index < len(columns):
                selected_column = columns[col_index]
                logging.info(f"已选择列: {selected_column}")
                break
            else:
                logging.error("输入的数字超出范围，请重新输入。")
        except ValueError:
            logging.error("请输入有效的数字。")

    # 生成输出文件名并处理
    time.sleep(0.1)
    output_dir = os.path.dirname(file_path)
    timestamp = time.strftime("%Y%m%d%H%M%S")
    output_file = os.path.join(output_dir, f"{selected_sheet}_{timestamp}.xlsx")

    process_excel(df, output_file, selected_column)


def process_excel(df, output_file, column_name):
    # 确保目标列存在
    if column_name not in df.columns:
        print(f"错误：文件中找不到列 '{column_name}'")
        return

    # 创建一个新的 DataFrame 来存储结果
    new_rows = []
    
    # 遍历每一行
    for index, row in df.iterrows():
        # 获取 product code 的值并转换为字符串
        product_codes = str(row[column_name])
        
        # 检查是否包含逗号
        if ',' in product_codes:
            # 根据逗号拆分
            codes = [code.strip() for code in product_codes.split(',')]
            for code in codes:
                new_row = row.copy()
                new_row[column_name] = code
                new_rows.append(new_row)
        else:
            # 如果没有逗号，直接添加原始行
            new_rows.append(row)
            
    # 从行列表创建新的 DataFrame
    new_df = pd.DataFrame(new_rows)

    # 将结果保存到新的 Excel 文件
    try:
        new_df.to_excel(output_file, index=False)
        logging.info(f"处理完成！已将结果保存到 {output_file}")
    except Exception as e:
        logging.error(f"保存文件失败: {e}")