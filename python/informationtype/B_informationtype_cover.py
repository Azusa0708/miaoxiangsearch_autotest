import csv
import json
import requests
from collections import Counter
import time
import os

# --- 配置区 ---

API_URL = 'http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/modelV2'

HEADERS = {
    'Content-Type': 'application/json'
}

INPUT_CSV_FILE = 'query.csv'
OUTPUT_CSV_FILE = 'B_results.csv'


def read_existing_results(filepath: str) -> Counter:
    """
    如果结果文件已存在，则读取其中的计数值。
    这使得脚本可以从上次停止的地方继续运行。
    """
    if not os.path.exists(filepath):
        # 如果文件不存在，返回一个空的计数器
        return Counter()

    try:
        with open(filepath, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            # 跳过表头
            next(reader, None)
            # 读取所有行并创建计数器
            # { 'NEWS': 10, 'REPORT': 5 }
            existing_counts = Counter({row[0]: int(row[1]) for row in reader if row})
            print(f"成功读取已有的统计结果: {dict(existing_counts)}")
            return existing_counts
    except (IOError, IndexError, ValueError, StopIteration) as e:
        # 如果文件格式不正确或为空，则从零开始
        print(f"警告: 无法解析已有的结果文件 '{filepath}'。将从零开始统计。错误: {e}")
        return Counter()

def write_totals_to_csv(filepath: str, counter: Counter):
    """
    将总计数值完整地写入CSV文件，覆盖旧内容。
    """
    try:
        with open(filepath, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)
            # 写入表头
            writer.writerow(['InformationType', 'Count'])
            if not counter:
                writer.writerow(['N/A', 0])
            else:
                # 按字母顺序写入，保持文件整洁
                for info_type, count in sorted(counter.items()):
                    writer.writerow([info_type, count])
    except IOError as e:
        print(f"  -> 错误: 无法将中间结果写入文件 '{filepath}'. Error: {e}")

# --- 主逻辑 ---

def process_queries_incrementally():
    """
    主函数，处理查询并为每个查询实时更新结果文件。
    """
    # 1. 启动时，首先加载已有的统计结果
    total_counter = read_existing_results(OUTPUT_CSV_FILE)

    print(f"\n开始处理 '{INPUT_CSV_FILE}' 中的查询...")

    try:
        # 先读取所有查询，以便显示总进度
        with open(INPUT_CSV_FILE, mode='r', encoding='utf-8') as infile:
            queries = list(csv.reader(infile))
            # 过滤掉可能的空行
            queries = [row for row in queries if row]

    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_CSV_FILE}' 未找到。")
        print("请在脚本同目录下创建一个 query.csv 文件，每行包含一个查询问题。")
        return

    # 2. 遍历每一个查询
    for i, row in enumerate(queries):
        query_text = row[0]
        print(f"\n--- [进度: {i+1}/{len(queries)}] 正在处理查询: '{query_text}' ---")

        # 构建请求载荷
        payload = {
            "query": query_text,
            "timeSupSize": 3,
            "decomposedFlag": False,
            "decomposedSize": 3,
            "size": 12,
            "useNewsSearch": True
  
}

        try:
            # 发送请求
            response = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            response_data = response.json()

            # 提取数据
            chunks = response_data.get('data', [])
            if not isinstance(chunks, list):
                print(f"  -> 注意: 查询 '{query_text}' 的响应中 'data' 字段不是列表。跳过。")
                continue

            # 统计本次请求返回的类型
            current_call_counter = Counter(chunk.get('informationType') for chunk in chunks if chunk.get('informationType'))

            if not current_call_counter:
                print("  -> 本次查询未返回任何新的 InformationType。文件内容不变。")
                continue

            # 3. 更新总计数器，并立即写入文件
            print(f"  -> 本次查询结果: {dict(current_call_counter)}")
            total_counter.update(current_call_counter)
            
            write_totals_to_csv(OUTPUT_CSV_FILE, total_counter)
            print(f"  -> 已将最新累计结果更新到 '{OUTPUT_CSV_FILE}'。")
            print(f"  -> 当前总计: {dict(sorted(total_counter.items()))}")

        except requests.exceptions.RequestException as e:
            print(f"  -> 错误: 请求失败 for query '{query_text}'. Error: {e}")
        except json.JSONDecodeError:
            print(f"  -> 错误: 解析JSON响应失败 for query '{query_text}'. Response: {response.text}")
        except Exception as e:
            print(f"  -> 发生未知错误 for query '{query_text}'. Error: {e}")
        
        # 友好等待
        time.sleep(0.5)

    print("\n--- 所有查询处理完毕 ---")
    print(f"最终结果已保存在 '{OUTPUT_CSV_FILE}'。")

if __name__ == '__main__':
    process_queries_incrementally()