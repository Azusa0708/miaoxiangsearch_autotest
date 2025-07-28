import requests
import json
import pandas as pd
import time
import os
import threading
import csv

# 用于保护文件写入操作的锁
file_lock = threading.Lock()


def process_query(query_data, api_url, headers, base_payload, output_filepath, retry_delay_seconds, connect_timeout,
                  read_timeout, column_names):
    """
    处理单个查询，发送请求并写入结果到 CSV 文件。
    这是一个会被多个线程调用的函数。

    Args:
        query_data (dict): 包含 'query' 的字典。
        api_url (str): 接口的 URL。
        headers (dict): 请求头。
        base_payload (dict): 基础请求体。
        output_filepath (str): 输出 CSV 文件的路径。
        retry_delay_seconds (float): 每次重试之间的等待秒数。
        connect_timeout (float or tuple): 连接超时时间。
        read_timeout (float or tuple): 读取超时时间。
        column_names (list): CSV 文件的列名列表。
    """
    query = query_data.get("query")
    if not query:
        print(f"警告：跳过缺少 'query' 字段的条目：{query_data}")
        return

    current_payload = base_payload.copy()
    current_payload["query"] = query

    response_json = None
    attempt = 0
    while response_json is None:
        attempt += 1
        print(f"线程 {threading.current_thread().name}: 正在发送请求 (Query: '{query}', 尝试次数: {attempt})...")
        try:
            response = requests.post(
                api_url,
                headers=headers,
                json=current_payload,
                timeout=(connect_timeout, read_timeout)
            )
            response.raise_for_status()
            response_json = response.json()

        except requests.exceptions.Timeout as e:
            print(f"线程 {threading.current_thread().name}: 请求超时 for query '{query}': {e}. 正在重试...")
            time.sleep(retry_delay_seconds)
        except requests.exceptions.ConnectionError as e:
            print(f"线程 {threading.current_thread().name}: 连接错误 for query '{query}': {e}. 正在重试...")
            time.sleep(retry_delay_seconds)
        except requests.exceptions.RequestException as e:
            print(f"线程 {threading.current_thread().name}: 请求失败 for query '{query}': {e}. 正在重试...")
            time.sleep(retry_delay_seconds)
        except json.JSONDecodeError:
            print(
                f"线程 {threading.current_thread().name}: 错误：无法解析响应 for query '{query}'，非 JSON 格式或空响应。正在重试...")
            time.sleep(retry_delay_seconds)
        except Exception as e:
            print(f"线程 {threading.current_thread().name}: 发生未知错误 for query '{query}': {e}. 正在重试...")
            time.sleep(retry_delay_seconds)

    trace_id = response_json.get("traceId")
    extra_infos = response_json.get("extraInfos", {})
    cache_trace_id = extra_infos.get("cacheTraceId")
    is_cache = extra_infos.get("isCache")
    decomposed_queries = extra_infos.get("decomposedQueries")

    cache_status_message = "否"
    if is_cache is True:
        cache_status_message = "是"
    elif is_cache is False:
        cache_status_message = "否"

    print(f"线程 {threading.current_thread().name}: Query '{query}' 完成。是否触发缓存: {cache_status_message}")

    if isinstance(decomposed_queries, list):
        decomposed_queries_str = "; ".join(decomposed_queries)
    else:
        decomposed_queries_str = str(decomposed_queries)

    # 按照 column_names 的顺序构建数据行
    row_data = [
        query,
        trace_id,
        cache_trace_id,
        is_cache,  # 直接将布尔值写入 CSV，Excel 会识别
        decomposed_queries_str
    ]

    # 线程安全的立即写入 CSV
    with file_lock:
        try:
            with open(output_filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row_data)

            print(f"线程 {threading.current_thread().name}: Query '{query}' 的结果已写入 CSV。")

        except Exception as e:
            print(f"线程 {threading.current_thread().name}: 错误：无法写入 CSV 文件中的行 for query '{query}': {e}")


def run_api_tests_multithreaded(input_json_filepath, output_filepath, api_url, headers, base_payload,
                                retry_delay_seconds=0.2, connect_timeout=5, read_timeout=10):
    """
    使用多线程并发处理请求，并实时写入 CSV。
    支持明确设置连接超时和读取超时。
    在开始处理请求前，确保 CSV 文件存在且第一行有表头。

    Args:
        input_json_filepath (str): 包含查询的 JSON 文件路径。
        output_filepath (str): 输出 CSV 文件的路径。
        api_url (str): 接口的 URL。
        headers (dict): 请求头。
        base_payload (dict): 基础请求体，'query' 参数将被替换。
        retry_delay_seconds (float): 每次重试之间的等待秒数。
        connect_timeout (float): 连接超时时间（秒）。
        read_timeout (float): 读取超时时间（秒）。
    """
    try:
        with open(input_json_filepath, 'r', encoding='utf-8') as f:
            queries_data = json.load(f)
    except FileNotFoundError:
        print(f"错误：输入文件 '{input_json_filepath}' 未找到。")
        return
    except json.JSONDecodeError:
        print(f"错误：无法解析文件 '{input_json_filepath}'，请检查 JSON 格式。")
        return

    # 定义 CSV 文件的列名
    column_names = ["Query", "TraceId", "CacheTraceId", "IsCache", "DecomposedQueries"]

    # 在开始处理请求前，预先创建或检查表头
    if not os.path.exists(output_filepath) or os.path.getsize(output_filepath) == 0:
        print(f"文件 '{output_filepath}' 不存在或为空，正在创建并写入表头...")
        try:
            with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(column_names)  #写入表头
            print("表头已成功写入。")
        except Exception as e:
            print(f"错误：无法写入 CSV 文件表头: {e}")
            return

    print(f"开始使用多线程处理请求，结果将写入 '{output_filepath}'...")
    print(f"超时设置：连接超时 {connect_timeout}秒, 读取超时 {read_timeout}秒")

    query_iterator = iter(queries_data)

    def worker():
        while True:
            try:
                with threading.Lock():
                    query_item = next(query_iterator)
            except StopIteration:
                break

            process_query(query_item, api_url, headers, base_payload,
                          output_filepath, retry_delay_seconds, connect_timeout, read_timeout, column_names)

    threads = []

    #设置线程数
    for i in range(3):
        thread = threading.Thread(target=worker, name=f"Worker-{i + 1}")
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"\n所有请求处理完毕，最终结果已保存到 '{output_filepath}'。")


if __name__ == "__main__":
    API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/coreApp/modelV2"
    HEADERS = {'Content-Type': 'application/json'}
    BASE_PAYLOAD = {
        "timeSupSize": 3,
        "decomposedFlag": True,
        "decomposedSize": 3,
        "size": 12,
        "useNewsSearch": True
    }
    INPUT_JSON_FILE = "output.json"
    OUTPUT_CSV_FILE = "api_test_results_multithreaded.csv"

    RETRY_DELAY_SECONDS = 0.2
    CONNECT_TIMEOUT = 5
    READ_TIMEOUT = 20

    run_api_tests_multithreaded(INPUT_JSON_FILE, OUTPUT_CSV_FILE, API_URL, HEADERS, BASE_PAYLOAD,
                                RETRY_DELAY_SECONDS, CONNECT_TIMEOUT, READ_TIMEOUT)