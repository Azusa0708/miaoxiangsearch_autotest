import csv
import requests
from datetime import datetime

# 配置参数
API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/modelV2"
RESULT_FILE = "invalid_items_B.csv"
QUERY_FILE = "b_query.csv"

# 定义需要检查source的信息类型
CHECK_SOURCE_TYPES = ["NEWS", "CFH", "LAW", "BOND", "WECHAT", "INTERACTION", "HOT_NEWS"]

# 定义ID前缀规则
ID_PREFIX_RULES = {
    "NEWS": "NW",
    "REPORT": "AP",
    "NOTICE": "AN",
    "LAW": "LA",
    "BOND": "BOND",
    "INTERACTION": "PS",
    "CFH": ""  # 不带前缀
}

def is_empty(value):
    """检查值是否为null或空字符串"""
    return value is None or value == ""

def check_id_prefix(item_id, info_type):
    """检查ID是否符合前缀规则"""
    if info_type not in ID_PREFIX_RULES:
        return None  # 不检查未定义的类型
    
    expected_prefix = ID_PREFIX_RULES[info_type]
    
    # CFH类型不需要前缀
    if info_type == "CFH":
        if item_id.startswith(("NW", "AP", "AN", "LA", "BOND", "PS")):
            return f"ID不应有前缀但实际为: {item_id[:2] if len(item_id) >= 2 else item_id}"
        return None
    
    # 其他类型需要检查前缀
    if not item_id.startswith(expected_prefix):
        return f"ID前缀应为{expected_prefix}但实际为: {item_id[:len(expected_prefix)] if len(item_id) >= len(expected_prefix) else item_id}"
    
    return None

def process_chunk(chunk):
    """处理单个数据块，返回是否需要保存及原因"""
    save_reasons = []
    
    # 检查必填字段是否为空（同时检查null和空字符串）
    required_fields = ["title", "showTime", "informationType"]
    for field in required_fields:
        if is_empty(chunk.get(field)):
            save_reasons.append(f"{field}为空(null或'')")
    
    # 检查特定informationType的source
    if chunk.get("informationType") in CHECK_SOURCE_TYPES:
        if is_empty(chunk.get("source")):
            save_reasons.append("source为空(null或'')但informationType需要")
    
    # 检查特定informationType的jumpUrl
    if chunk.get("informationType") in ["WECHAT", "HOT_NEWS", "INV_NEWS"]:
        if is_empty(chunk.get("jumpUrl")):
            save_reasons.append("jumpUrl为空(null或'')但informationType需要")
    
    # 检查ID前缀（先确保id不为空）
    if chunk.get("informationType") in ID_PREFIX_RULES and not is_empty(chunk.get("id")):
        prefix_reason = check_id_prefix(chunk["id"], chunk["informationType"])
        if prefix_reason:
            save_reasons.append(prefix_reason)
    
    return "; ".join(save_reasons) if save_reasons else None

def process_query(query, csv_writer):
    """处理单个查询并直接写入CSV"""
    # 构造请求体（其他参数固定）
    body = {
        "query": query.strip(),
        "timeSupSize": 3,
        "decomposedFlag": True,
        "decomposedSize": 3,
        "size": 12,
        "useNewsSearch": True
    }
    
    try:
        response = requests.post(API_URL, json=body)
        response.raise_for_status()
        data = response.json()
        
        for item in data.get("data", []):
            reasons = process_chunk(item)
            if reasons:
                # 准备CSV行数据
                row = {
                    "id": item.get("id", ""),
                    "title": item.get("title", ""),
                    "showTime": item.get("showTime", ""),
                    "source": item.get("source", ""),
                    "informationType": item.get("informationType", ""),
                    "jumpUrl": item.get("jumpUrl", ""),
                    "currentQuery": item.get("currentQuery", ""),
                    "originalQuery": item.get("originalQuery", ""),
                    "inputQuery": query.strip(),
                    "invalid_reasons": reasons,
                    "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                csv_writer.writerow(row)
                print(f"发现无效项: {reasons} | 查询: {query.strip()}")
    
    except Exception as e:
        print(f"处理查询'{query.strip()}'时出错: {str(e)}")
        # 记录错误查询到CSV
        row = {
            "id": "ERROR",
            "title": "ERROR",
            "showTime": "",
            "source": "",
            "informationType": "",
            "jumpUrl": "",
            "currentQuery": "",
            "originalQuery": "",
            "inputQuery": query.strip(),
            "invalid_reasons": f"接口请求失败: {str(e)}",
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        csv_writer.writerow(row)

def main():
    # 准备CSV文件并写入表头
    with open(RESULT_FILE, "w", newline="", encoding="utf-8-sig") as csvfile:
        fieldnames = [
            "id", "title", "showTime", "source", "informationType", 
            "jumpUrl", "currentQuery", "originalQuery", "inputQuery",
            "invalid_reasons", "process_time"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # 读取查询文件
        with open(QUERY_FILE, "r", encoding="utf-8") as f:
            queries = f.readlines()
        
        # 处理每个查询
        for query in queries:
            if not query.strip():
                continue
                
            print(f"正在处理查询: {query.strip()}")
            process_query(query, writer)
    
    print(f"处理完成，结果已保存到{RESULT_FILE}")

if __name__ == "__main__":
    main()
