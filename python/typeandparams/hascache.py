# -*- coding: utf-8 -*-
import csv
import json
import os
import time
import requests
from datetime import datetime
from collections import Counter

# --- 1. 全局配置与常量 ---

# B接口和C接口的配置信息
CONFIG = {
    "B": {
        "name": "接口B",
        "api_url": "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/modelV2",
    },
    "C": {
        "name": "接口C",
        "api_url": "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/coreApp/modelV2",
    }
}

# 定义统一的输入和输出文件
QUERY_FILE = "query.csv"
VALIDATION_OUTPUT_FILE = "validation_results.csv" # 验证结果文件
COVERAGE_OUTPUT_FILE = "coverage_results.csv"     # 覆盖率统计文件

# API请求和字段验证规则的共享常量
HEADERS = {'Content-Type': 'application/json'}
CHECK_SOURCE_TYPES = ["NEWS", "CFH", "LAW", "BOND", "WECHAT", "INTERACTION", "HOT_NEWS"]
ID_PREFIX_RULES = {
    "NEWS": "NW", "REPORT": "AP", "NOTICE": "AN", "LAW": "LA",
    "BOND": "BOND", "INTERACTION": "PS", "CFH": ""
}


# --- 2. 核心处理函数 ---

def is_empty(value):
    """检查值是否为None或空字符串"""
    return value is None or value == ""


def check_id_prefix(item_id, info_type):
    """检查ID是否符合前缀规则"""
    if info_type not in ID_PREFIX_RULES:
        return None

    expected_prefix = ID_PREFIX_RULES[info_type]

    if info_type == "CFH":
        if any(item_id.startswith(p) for p in ["NW", "AP", "AN", "LA", "BOND", "PS"]):
            return f"ID不应有前缀但实际为: {item_id[:2] if len(item_id) >= 2 else item_id}"
        return None

    if not item_id.startswith(expected_prefix):
        return f"ID前缀应为{expected_prefix}但实际为: {item_id[:len(expected_prefix)] if len(item_id) >= len(expected_prefix) else item_id}"

    return None


def process_item_for_validation(item):
    """对单个返回结果进行所有字段验证，返回错误原因"""
    save_reasons = []
    required_fields = ["title", "showTime", "informationType"]
    for field in required_fields:
        if is_empty(item.get(field)):
            save_reasons.append(f"{field}为空(null或'')")

    if item.get("informationType") in CHECK_SOURCE_TYPES and is_empty(item.get("source")):
        save_reasons.append("source为空(null或'')但informationType需要")

    if item.get("informationType") in ["WECHAT", "HOT_NEWS", "INV_NEWS"] and is_empty(item.get("jumpUrl")):
        save_reasons.append("jumpUrl为空(null或'')但informationType需要")

    if item.get("informationType") in ID_PREFIX_RULES and not is_empty(item.get("id")):
        prefix_reason = check_id_prefix(item["id"], item["informationType"])
        if prefix_reason:
            save_reasons.append(prefix_reason)

    return "; ".join(save_reasons) if save_reasons else None


def read_existing_coverage_data(filepath):
    """
    读取现有的覆盖率报告数据并返回一个 Counter 结构。
    """
    existing_counters = {
        'B': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()},
        'C': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()}
    }

    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        print(f"  -> 未找到现有覆盖率报告 '{filepath}' 或其为空。将从零开始统计。")
        return existing_counters

    try:
        with open(filepath, mode='r', encoding='utf-8-sig', newline='') as infile:
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                print(f"  -> 现有覆盖率报告 '{filepath}' 为空文件或无表头，将从零开始统计。")
                return existing_counters

            # Map column names to internal counter keys
            # Example: 'Count_接口B_CacheHit' -> ('B', 'cache_hit')
            col_map = {}
            for field in reader.fieldnames:
                if field.startswith('Count_'):
                    parts = field.split('_')
                    if len(parts) >= 3:
                        endpoint_name = parts[1] # "接口B" or "接口C"
                        status_part = parts[-1] # "CacheHit", "CacheMiss", "NoCacheInfo"
                        
                        endpoint_key = None
                        for k, v in CONFIG.items():
                            if v['name'] == endpoint_name:
                                endpoint_key = k
                                break
                        
                        status_key = None
                        if status_part == 'CacheHit':
                            status_key = 'cache_hit'
                        elif status_part == 'CacheMiss':
                            status_key = 'cache_miss'
                        elif status_part == 'NoCacheInfo':
                            status_key = 'no_cache_info'

                        if endpoint_key and status_key:
                            col_map[field] = (endpoint_key, status_key)

            for row in reader:
                info_type = row.get('InformationType')
                if not info_type:
                    continue

                for col_name, (endpoint_key, status_key) in col_map.items():
                    try:
                        count = int(row.get(col_name, 0))
                        existing_counters[endpoint_key][status_key][info_type] += count
                    except ValueError:
                        print(f"  -> 警告: 现有覆盖率报告中 '{col_name}' 列的 '{info_type}' 值无法转换为数字: '{row.get(col_name)}'。将跳过此值。")
                        pass # Skip invalid numbers

        print(f"  -> 成功读取现有覆盖率报告 '{filepath}' 中的数据。")
    except Exception as e:
        print(f"  -> 错误：读取现有覆盖率报告 '{filepath}' 时发生错误: {e}。将从零开始统计。")
        return {
            'B': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()},
            'C': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()}
        }
    return existing_counters


def write_final_coverage_report(all_coverage_counters, filepath):
    """
    将两个接口的资讯类型覆盖统计结果（区分isCache状态）写入同一个CSV文件。
    all_coverage_counters 结构:
    {
        'B': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()},
        'C': {'cache_hit': Counter(), 'cache_miss': Counter(), 'no_cache_info': Counter()}
    }
    """
    print(f"\n正在生成最终的资讯类型覆盖率报告: {filepath}")

    # 收集所有接口的所有资讯类型
    all_types = set()
    for endpoint_key in CONFIG.keys():
        for status_key in all_coverage_counters[endpoint_key].keys():
            all_types.update(all_coverage_counters[endpoint_key][status_key].keys())
    all_types = sorted(list(all_types))

    try:
        # Note: We still use 'w' mode here to overwrite with the *accumulated* data.
        # This is the desired behavior for accumulation - you read, add, then write the new total.
        with open(filepath, mode='w', encoding='utf-8-sig', newline='') as outfile:
            writer = csv.writer(outfile)
            
            # 构建表头
            header = ['InformationType']
            for endpoint_key in sorted(CONFIG.keys()): # 保证B和C的顺序
                header.append(f'Count_{CONFIG[endpoint_key]["name"]}_CacheHit')
                header.append(f'Count_{CONFIG[endpoint_key]["name"]}_CacheMiss')
                header.append(f'Count_{CONFIG[endpoint_key]["name"]}_NoCacheInfo')
            writer.writerow(header)

            # 写入数据
            for info_type in all_types:
                row_data = [info_type]
                for endpoint_key in sorted(CONFIG.keys()):
                    row_data.append(all_coverage_counters[endpoint_key]['cache_hit'].get(info_type, 0))
                    row_data.append(all_coverage_counters[endpoint_key]['cache_miss'].get(info_type, 0))
                    row_data.append(all_coverage_counters[endpoint_key]['no_cache_info'].get(info_type, 0))
                writer.writerow(row_data)
        print("覆盖率报告生成成功。")
    except IOError as e:
        print(f"错误：无法写入覆盖率报告。 {e}")


# --- 3. 主执行逻辑 ---

def main():
    """
    主函数，负责读取问句，并对每个问句调用B和C接口进行处理，
    最后生成统一的验证结果和覆盖率报告。
    """
    print("--- 开始执行API自动化测试 ---")
    print(f"将从 '{QUERY_FILE}' 读取问句...")

    try:
        with open(QUERY_FILE, "r", encoding="utf-8") as f:
            queries = [line.strip() for line in f if line.strip()]
        if not queries:
            print(f"错误: '{QUERY_FILE}' 为空或不存在。")
            return
    except FileNotFoundError:
        print(f"错误: 查询文件 '{QUERY_FILE}' 未找到。请创建该文件后重试。")
        return

    # --- 关键修改：读取现有覆盖率数据 ---
    all_coverage_counters = read_existing_coverage_data(COVERAGE_OUTPUT_FILE)


    # 准备验证结果的CSV文件 (这个文件仍然是每次运行时覆盖，只记录当前运行中的错误)
    validation_fieldnames = [
        "endpoint", "id", "title", "showTime", "source", "informationType",
        "jumpUrl", "currentQuery", "originalQuery", "inputQuery",
        "isCache_present", "isCache_value",
        "invalid_reasons", "process_time"
    ]

    with open(VALIDATION_OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as validation_csvfile:
        validation_writer = csv.DictWriter(validation_csvfile, fieldnames=validation_fieldnames)
        validation_writer.writeheader()

        # 遍历每一个问句
        total_queries = len(queries)
        for i, query in enumerate(queries):
            print(f"\n--- [进度: {i + 1}/{total_queries}] 正在处理问句: '{query}' ---")

            # 对当前问句，依次调用B和C接口
            for endpoint_key, endpoint_info in CONFIG.items():
                api_name = endpoint_info['name']
                api_url = endpoint_info['api_url']
                print(f"  -> 正在调用 [{api_name}]...")

                body = {
                    "query": query, "timeSupSize": 3, "decomposedFlag": True,
                    "decomposedSize": 3, "size": 12, "useNewsSearch": True,
                    "searchStrategyType": "mergeAllQueryRank"
                }

                try:
                    response = requests.post(api_url, json=body, timeout=30)
                    response.raise_for_status()
                    response_json = response.json()

                    extra_infos = response_json.get("extraInfos", {})
                    is_cache_present = "isCache" in extra_infos
                    is_cache_value = extra_infos.get("isCache")

                    response_data = response_json.get("data", [])

                    # 统一处理 isCache 的打印信息
                    cache_status_message = "未知"
                    if is_cache_present:
                        if is_cache_value is True:
                            cache_status_message = "是"
                        elif is_cache_value is False:
                            cache_status_message = "否"
                    print(f"  -> [{api_name}] 响应。isCache 存在: {is_cache_present}, 值: {cache_status_message}")


                    # 如果响应的 'data' 字段不是列表，则记录为错误
                    if not isinstance(response_data, list):
                        print(f"  -> 注意: [{api_name}] 的响应中 'data' 字段不是列表，记录为错误。")
                        validation_writer.writerow({
                            "endpoint": api_name,
                            "inputQuery": query,
                            "invalid_reasons": "响应中的'data'字段不是列表",
                            "isCache_present": is_cache_present,
                            "isCache_value": is_cache_value,
                            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        continue # 跳过后续的字段验证和统计，处理下一个接口或问句

                    # 遍历返回数据，只记录有错误的项到 validation_results.csv
                    current_call_info_types = [] # 用于统计覆盖率
                    for item in response_data:
                        reasons = process_item_for_validation(item)
                        if reasons: # 只在有错误时写入 validation_results.csv
                            print(f"  -> [{api_name}] 发现不合规项: {reasons}")
                            row = {
                                "endpoint": api_name,
                                "id": item.get("id", ""),
                                "title": item.get("title", ""),
                                "showTime": item.get("showTime", ""),
                                "source": item.get("source", ""),
                                "informationType": item.get("informationType", ""),
                                "jumpUrl": item.get("jumpUrl", ""),
                                "currentQuery": item.get("currentQuery", ""),
                                "originalQuery": item.get("originalQuery", ""),
                                "inputQuery": query,
                                "isCache_present": is_cache_present,
                                "isCache_value": is_cache_value,
                                "invalid_reasons": reasons,
                                "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            validation_writer.writerow(row)
                        
                        # 无论是否有错误，都收集 informationType 用于覆盖率统计
                        info_type = item.get('informationType')
                        if info_type:
                            current_call_info_types.append(info_type)


                    # 统计资讯类型覆盖（根据isCache状态）
                    current_call_counter = Counter(current_call_info_types)
                    if endpoint_key == 'B':
                        if is_cache_present:
                            if is_cache_value is True:
                                all_coverage_counters['B']['cache_hit'].update(current_call_counter)
                            else: # is_cache_value is False
                                all_coverage_counters['B']['cache_miss'].update(current_call_counter)
                        else: # isCache_present is False
                            all_coverage_counters['B']['no_cache_info'].update(current_call_counter)
                    else: # 'C'
                        if is_cache_present:
                            if is_cache_value is True:
                                all_coverage_counters['C']['cache_hit'].update(current_call_counter)
                            else: # is_cache_value is False
                                all_coverage_counters['C']['cache_miss'].update(current_call_counter)
                        else: # isCache_present is False
                            all_coverage_counters['C']['no_cache_info'].update(current_call_counter)

                except Exception as e:
                    print(f"  -> 错误: 调用 [{api_name}] 处理问句 '{query}' 时出错: {str(e)}")
                    # 记录接口请求失败本身就是一种错误
                    validation_writer.writerow({
                        "endpoint": api_name,
                        "id": "ERROR",
                        "title": "ERROR",
                        "inputQuery": query,
                        "invalid_reasons": f"接口请求失败: {str(e)}",
                        "isCache_present": False, # 请求失败时 isCache 可能无法获取
                        "isCache_value": None,
                        "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
            time.sleep(0.5)  # 友好等待

    # 所有问句处理完毕后，生成最终的覆盖率报告
    write_final_coverage_report(all_coverage_counters, COVERAGE_OUTPUT_FILE)

    print("\n--- 所有测试任务执行完毕 ---")
    print(f"字段验证结果已保存至: {VALIDATION_OUTPUT_FILE}")
    print(f"资讯类型覆盖率统计已保存至: {COVERAGE_OUTPUT_FILE}")


if __name__ == "__main__":
    main()