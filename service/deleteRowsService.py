import logging
import time
import os
import openpyxl
import pandas as pd
from migration_tools.utils import excelUtils

def delete_rows_entrypoint():
    """
    Main entry point for the delete rows service. Handles user interaction
    for file and sheet selection.
    """
    logging.info("启动“删除带有删除线的行”功能...")
    time.sleep(0.1)

    # 1. Select Excel file
    excel_files = excelUtils.find_excel_file()
    if not excel_files:
        return
    file_path = excelUtils.user_choose_file(excel_files)
    if not file_path:
        return

    # 2. Select sheets (multiple)
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

    selected_sheets = []
    while True:
        try:
            choices_str = input(f"\n请输入要操作的工作表编号(可输入多个，用逗号隔开, e.g., 1,3): ")
            choice_indices = [int(i.strip()) - 1 for i in choices_str.split(',')]
            
            valid_choices = True
            temp_sheets = []
            for idx in choice_indices:
                if 0 <= idx < len(sheet_names):
                    temp_sheets.append(sheet_names[idx])
                else:
                    logging.error(f"编号 {idx + 1} 超出范围，请重新输入。")
                    valid_choices = False
                    break
            
            if valid_choices:
                selected_sheets = list(dict.fromkeys(temp_sheets)) # remove duplicates
                logging.info(f"已选择工作表: {', '.join(selected_sheets)}")
                break
        except ValueError:
            logging.error("请输入有效的数字编号，并用逗号分隔。")

    if not selected_sheets:
        logging.info("没有选择任何工作表，操作取消。")
        return

    # 3. Generate output file name and process
    output_dir = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    timestamp = time.strftime("%Y%m%d%H%M%S")
    output_file = os.path.join(output_dir, f"{base_name}_cleaned_{timestamp}.xlsx")

    logging.info(f"准备处理文件，输出将保存到: {output_file}")
    process_and_delete_rows(file_path, selected_sheets, output_file)


def process_and_delete_rows(input_file, sheet_names, output_file):
    """
    Reads specified sheets from an Excel file, deletes rows where any of the
    first three columns have a strikethrough font, and saves the result to a new Excel file.
    """
    try:
        # Load the workbook with openpyxl to access formatting
        wb = openpyxl.load_workbook(input_file)

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Process selected sheets
            for sheet_name in sheet_names:
                logging.info(f"--- 正在处理工作表: {sheet_name} ---")
                try:
                    df = pd.read_excel(input_file, sheet_name=sheet_name)

                    if df.empty:
                        logging.warning(f"工作表 '{sheet_name}' 为空，跳过处理。")
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        continue

                    # Use openpyxl to find rows with strikethrough
                    sheet = wb[sheet_name]
                    rows_to_delete_indices = []
                    # Start from row 2 (after header) to check for strikethrough
                    for row_idx, row in enumerate(sheet.iter_rows(min_row=2), start=2):
                        # Check the first 3 cells in the row
                        for cell in row[:3]:
                            if cell.font and cell.font.strike:
                                # df index is (excel row number - 2)
                                rows_to_delete_indices.append(row_idx - 2)
                                break  # Move to the next row

                    original_row_count = len(df)

                    if rows_to_delete_indices:
                        df.drop(index=rows_to_delete_indices, inplace=True)

                    final_row_count = len(df)
                    rows_deleted = original_row_count - final_row_count

                    if rows_deleted > 0:
                        logging.info(f"在 '{sheet_name}' 中删除了 {rows_deleted} 行。")
                    else:
                        logging.info(f"在 '{sheet_name}' 中没有找到需要删除的行。")

                    df.to_excel(writer, sheet_name=sheet_name, index=False)

                except Exception as e:
                    logging.error(f"处理工作表 '{sheet_name}' 时出错: {e}")


        logging.info(f"处理完成！已将结果保存到 {output_file}")

    except Exception as e:
        logging.error(f"保存文件时出错: {e}", exc_info=True)