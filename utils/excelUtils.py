import os
import time
import pandas as pd
import logging
from migration_tools.constants import MESSAGES
from migration_tools.utils import sysUtils


IMPORT_EXCEL_FOLDER = sysUtils.resource_path('import_excel')
OUTPUT_EXCEL_FOLDER = sysUtils.resource_path('output_excel')


def find_excel_file(folder=IMPORT_EXCEL_FOLDER):
    """列出指定文件夹中的所有Excel文件"""
    if not os.path.exists(folder):
        logging.error(f"☆ 未找到文件夹 '{folder}'，请确保文件夹路径正确并存在。")
        return None
    excel_files = [file for file in os.listdir(folder) if file.endswith(('.xls', '.xlsx', '.csv', '.CSV'))]
    if not excel_files:
        logging.error(f"☆ 在文件夹 '{folder}' 中未找到任何 Excel 或 CSV 文件，请确认文件是否存在且格式正确。")
        return None

    print(f"☆ 在 '{folder}' 文件夹中找到了以下文件：")
    return excel_files


def user_choose_file(excel_files):
    try:
        # 用户选择文件
        print(MESSAGES.PRINT_FILE_INFO)
        for idx, file in enumerate(excel_files):
            print(f"  [{idx}] {file}")

        file_index = int(input(MESSAGES.CHOOSE_USE_FILE))
        selected_file = excel_files[file_index]
        file_path = os.path.join(IMPORT_EXCEL_FOLDER, selected_file)
        logging.info(f"☆ 您选择的文件是: {file_path}\n")
        time.sleep(0.1)
        return file_path
    except Exception as e:
        logging.error(MESSAGES.ERROR_INPUT)
        logging.error(e)
        return user_choose_file(excel_files)


def detect_csv_encoding(file_path):
    """检测CSV文件的正确编码"""
    # 简化的编码检测，避免使用chardet
    encodings_to_try = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin1', 'cp1252', 'iso-8859-1']

    for encoding in encodings_to_try:
        try:
            # 尝试读取文件前几行
            with open(file_path, 'r', encoding=encoding) as f:
                # 读取前3行进行测试
                for i in range(3):
                    line = f.readline()
                    if not line:
                        break
            logging.info(f"☆ 检测到文件编码: {encoding}")
            return encoding
        except UnicodeDecodeError:
            continue
        except Exception:
            continue

    logging.warning("☆ 无法确定文件编码，使用默认编码: utf-8")
    return 'utf-8'


def get_csv_file_info(file_path, encoding):
    """获取CSV文件的基本信息（行数、列数）"""
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            # 读取第一行获取列数
            header = f.readline()
            column_count = len(header.split(','))

            # 估算行数
            line_count = 0
            sample_lines = 1000  # 采样1000行来估算
            for i, line in enumerate(f):
                if i >= sample_lines:
                    break
            line_count = i + 1  # 加上标题行

            # 如果文件很大，给出警告
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logging.info(f"☆ 文件大小: {file_size:.2f} MB, 估算列数: {column_count}")

            return file_size, column_count
    except Exception as e:
        logging.warning(f"☆ 无法获取文件信息: {e}")
        return 0, 0


def load_large_csv_safely(file_path, encoding):
    """安全加载大CSV文件，采用更激进的内存优化"""
    try:
        # 获取文件信息
        file_size, column_count = get_csv_file_info(file_path, encoding)

        # 如果文件很大或列很多，使用更小的块大小
        if file_size > 100 or column_count > 50:  # 文件大于100MB或列数大于50
            chunk_size = 1000  # 使用更小的块
        else:
            chunk_size = 10000

        logging.info(f"☆ 采用分块读取，块大小: {chunk_size}")

        # 方法1: 使用Python引擎（更慢但更省内存）
        try:
            chunks = []
            for i, chunk in enumerate(pd.read_csv(file_path, encoding=encoding,
                                                  on_bad_lines='skip', chunksize=chunk_size,
                                                  engine='python', dtype=str)):
                chunks.append(chunk)
                if (i + 1) % 10 == 0:
                    logging.info(f"☆ 已读取 {len(chunks) * chunk_size} 行...")
                    # 定期清理内存
                    import gc
                    gc.collect()

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                logging.info(f"☆ 文件加载完成，总行数: {len(df)}")
                return df
        except MemoryError:
            logging.warning("☆ 方法1内存不足，尝试方法2...")

        # 方法2: 只读取前N行（如果文件太大）
        try:
            sample_size = 50000  # 只读取前5万行
            logging.info(f"☆ 文件过大，尝试加载前 {sample_size} 行作为样本")
            df = pd.read_csv(file_path, encoding=encoding, nrows=sample_size,
                             on_bad_lines='skip', engine='python', dtype=str)
            return df
        except MemoryError:
            logging.warning("☆ 方法2内存不足，尝试方法3...")

        # 方法3: 使用更小的样本
        try:
            sample_size = 10000  # 只读取前1万行
            logging.info(f"☆ 文件过大，尝试加载前 {sample_size} 行作为样本")
            df = pd.read_csv(file_path, encoding=encoding, nrows=sample_size,
                             on_bad_lines='skip', engine='python', dtype=str)
            return df
        except Exception as e:
            logging.error(f"☆ 所有方法都失败: {e}")
            return None

    except Exception as e:
        logging.error(f"☆ 读取CSV文件时发生错误: {e}")
        return None


def load_data(file_path):
    """根据文件类型加载数据"""
    if file_path.endswith(('.csv', '.CSV')):
        # 检测文件编码
        encoding = detect_csv_encoding(file_path)

        # 询问用户是否要采样读取（对于大文件）
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        if file_size > 50:  # 文件大于50MB时询问
            print(f"☆ 文件较大 ({file_size:.1f} MB)，是否采样读取前N行？")
            print("   [0] 完整读取（可能需要大量内存）")
            print("   [1] 采样读取前10万行")
            print("   [2] 采样读取前5万行")
            print("   [3] 采样读取前1万行")

            try:
                choice = int(input("请选择读取方式 (0-3): "))
                if choice == 1:
                    return load_csv_sample(file_path, encoding, 100000)
                elif choice == 2:
                    return load_csv_sample(file_path, encoding, 50000)
                elif choice == 3:
                    return load_csv_sample(file_path, encoding, 10000)
            except:
                pass  # 如果输入错误，继续完整读取

        # 安全加载CSV文件
        df = load_large_csv_safely(file_path, encoding)
        if df is not None:
            logging.info(f"☆ 成功加载 CSV 文件: {file_path}，数据形状: {df.shape}\n")
            time.sleep(0.1)
        else:
            logging.error(f"☆ 加载 CSV 文件失败: {file_path}\n")
        return df

    elif file_path.endswith(('.xls', '.xlsx', '.XLS', '.XLSX')):
        sheets = list_excel_sheets(file_path)
        if sheets:
            try:
                sheet_index = int(input(MESSAGES.INPUT_CHOOSE_SHEET))
                selected_sheet = sheets[sheet_index]
                df = pd.read_excel(file_path, sheet_name=selected_sheet, engine='openpyxl', dtype=str, keep_default_na=False, na_values=[''])
                logging.info(f"成功加载工作表: {selected_sheet}\n")
                time.sleep(0.1)
                return df
            except Exception as e:
                logging.error(f"☆ 读取 Excel 工作表时发生错误: {e}\n")
                time.sleep(0.1)
                return None
        else:
            return None
    else:
        print("☆ 不支持的文件格式，请选择 Excel 或 CSV 格式文件。\n")
        return None


def load_csv_sample(file_path, encoding, sample_size):
    """加载CSV文件的样本数据"""
    try:
        logging.info(f"☆ 采样读取前 {sample_size} 行数据")
        df = pd.read_csv(file_path, encoding=encoding, nrows=sample_size,
                         on_bad_lines='skip', engine='python', dtype=str)
        logging.info(f"☆ 成功加载样本数据，形状: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"☆ 采样读取失败: {e}")
        return None


def list_excel_sheets(file_path):
    """列出Excel文件中的所有工作表"""
    try:
        with pd.ExcelFile(file_path) as excel_file:
            sheets = excel_file.sheet_names
            print("☆ 该文件包含以下工作表：\n")
            for idx, sheet in enumerate(sheets):
                print(f"  [{idx}] {sheet}")
            return sheets
    except Exception as e:
        print(f"☆ 读取 Excel 文件时出错: {e}")
        return None


def print_file_list(excel_files):
    # 用户选择文件
    print(MESSAGES.PRINT_FILE_INFO)
    for idx, file in enumerate(excel_files):
        print(f"  [{idx}] {file}")
    print("\n")


def map_excel_file(file_name):
    return os.path.join(IMPORT_EXCEL_FOLDER, file_name)


def load_data_v2(file_path, p_sheet_name, folder=IMPORT_EXCEL_FOLDER):
    """根据文件类型加载数据 V2版本"""
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            # 尝试在指定文件夹中查找
            full_path = os.path.join(folder, file_path)
            if os.path.exists(full_path):
                file_path = full_path
            else:
                logging.error(f"☆ 文件未找到: {file_path}\n")
                return None
    except Exception as e:
        logging.error(f"☆ 读取文件时发生错误: {e}\n")
        return None

    if file_path.endswith(('.csv', '.CSV')):
        # 检测文件编码
        encoding = detect_csv_encoding(file_path)

        # 对于批处理，默认使用采样读取
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        if file_size > 50:
            logging.info(f"☆ 文件较大 ({file_size:.1f} MB)，自动采样读取前5万行")
            df = load_csv_sample(file_path, encoding, 50000)
        else:
            df = load_large_csv_safely(file_path, encoding)

        if df is not None:
            logging.info(f"☆ 成功加载 CSV 文件: {file_path}，数据形状: {df.shape}")
            time.sleep(0.1)
        else:
            logging.error(f"☆ 加载 CSV 文件失败: {file_path}")
        return df

    elif file_path.endswith(('.xls', '.xlsx', '.XLS', '.XLSX')):
        p_sheet_index, sheets = find_excel_sheets(file_path, p_sheet_name)
        if sheets and p_sheet_index is not None:
            try:
                selected_sheet = sheets[p_sheet_index]
                df = pd.read_excel(file_path, sheet_name=selected_sheet, engine='openpyxl', dtype=str, keep_default_na=False, na_values=[''])
                logging.info(f"成功加载工作表: {selected_sheet}")
                time.sleep(0.1)
                return df
            except Exception as e:
                logging.error(f"☆ 读取 Excel 工作表时发生错误: {e}\n")
                time.sleep(0.1)
                return None
        else:
            logging.error(f"☆ 未找到工作表: {p_sheet_name}")
            return None
    else:
        logging.error("☆ 不支持的文件格式，请选择 Excel 或 CSV 格式文件。\n")
        return None


def find_excel_sheets(file_path, p_sheet_name):
    """列出Excel文件中的所有工作表"""
    try:
        with pd.ExcelFile(file_path) as excel_file:
            sheets = excel_file.sheet_names
            for idx, sheet in enumerate(sheets):
                if sheet == p_sheet_name:
                    return idx, sheets
            # 如果没找到指定名称的工作表，返回第一个工作表
            if sheets:
                logging.warning(f"☆ 未找到工作表 '{p_sheet_name}'，使用第一个工作表: {sheets[0]}")
                return 0, sheets
            return None, None
    except Exception as e:
        logging.error(f"☆ 读取 Excel 文件时出错: {e}\n")
        return None, None


def find_excel_file_V2(folder=IMPORT_EXCEL_FOLDER):
    """列出指定文件夹中的所有Excel文件"""
    if not os.path.exists(folder):
        logging.error(f"☆ 未找到文件夹 '{folder}'，请确保文件夹路径正确并存在。\n")
        return None
    excel_files = [file for file in os.listdir(folder) if file.endswith(('.xls', '.xlsx', '.csv', '.CSV'))]
    if not excel_files:
        logging.error(f"☆ 在文件夹 '{folder}' 中未找到任何 Excel 或 CSV 文件，请确认文件是否存在且格式正确。\n")
        return None
    return excel_files

def save_dataframes_to_excel(dataframes, filename, folder=OUTPUT_EXCEL_FOLDER):
    """将多个DataFrame保存到同一个Excel文件的不同工作表中"""
    # 检查输出文件夹是否存在，如果不存在则创建
    if not os.path.exists(folder):
        os.makedirs(folder)
        logging.info(f"☆ 创建输出文件夹: {folder}")
    file_path = os.path.join(folder, filename)
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info(f"☆ 数据成功保存到文件: {file_path}")
    except Exception as e:
        logging.error(f"☆ 保存Excel文件时出错: {e}")

def save_query_to_excel_paginated(connection, base_query, filename, page_size=100000, folder=OUTPUT_EXCEL_FOLDER):
    """
    Executes a SQL query in paginated chunks and saves the results to a single Excel file, with a serial number.

    :param connection: Active database connection.
    :param base_query: The user's SQL query without LIMIT/OFFSET.
    :param filename: The name of the output Excel file.
    :param page_size: The number of rows per page.
    :param folder: The output folder.
    :return: The total number of rows written.
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
        logging.info(f"☆ 创建输出文件夹: {folder}")

    file_path = os.path.join(folder, filename)
    total_rows_written = 0
    is_first_page = True

    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            page_num = 0
            while True:
                offset = page_num * page_size
                paginated_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
                logging.info(f"Executing paginated query (page {page_num + 1}): {paginated_query}")

                # Dynamically import to avoid circular dependency at module level
                from migration_tools.dao import directSqlQueryToExcelDao
                cursor, column_names = directSqlQueryToExcelDao.execute_query_stream(connection, paginated_query)

                if not cursor:
                    logging.error("Paginated query execution failed.")
                    break

                rows = cursor.fetchall()
                cursor.close()

                if not rows:
                    logging.info("No more data to fetch. Pagination complete.")
                    break

                df_page = pd.DataFrame(rows, columns=column_names)

                # Add serial number column, starting from 1
                df_page.insert(0, '序号', range(total_rows_written + 1, total_rows_written + 1 + len(df_page)))

                # For the first page, write with header. For subsequent pages, append without header.
                if is_first_page:
                    df_page.to_excel(writer, sheet_name='Sheet1', index=False, header=True)
                else:
                    df_page.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

                rows_in_page = len(df_page)
                total_rows_written += rows_in_page
                logging.info(f"已写入 {rows_in_page} 行 (总计: {total_rows_written})...")

                is_first_page = False
                page_num += 1

                if rows_in_page < page_size:
                    logging.info("Last page fetched. Ending pagination.")
                    break
    except Exception as e:
        logging.error(f"☆ 保存Excel文件时出错: {e}")
        return 0

    return total_rows_written