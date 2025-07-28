import requests
import csv
import time
from typing import Dict, List, Optional, Tuple, Any
import uuid
from pathlib import Path

#API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/coreApp/modelV2"
API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/modelV2"
COMMON_HEADERS = {"Content-Type": "application/json"}

# 10种资讯类型
INFORMATION_TYPES = ["NEWS", "REPORT", "NOTICE", "CFH", "LAW", "BOND", 
                    "WECHAT", "INTERACTION", "INV_NEWS", "HOT_NEWS"]

API_PARAMS = {
    "timeSupSize": 3,
    "decomposedFlag": True,
    "decomposedSize": 3,
    "size": 12,
    "useNewsSearch": True
}

def call_api_with_retry(url: str, question: str, params: Dict, information_type: str) -> Tuple[Dict, str]:
    """无重试次数限制的API调用"""
    traceid = str(uuid.uuid4())
    payload = {
        **params,
        "query": question.strip(),
        "traceid": traceid,
        "childSearchType": information_type
    }
    
    while True:  # 无限重试直到成功
        try:
            response = requests.post(
                url,
                headers=COMMON_HEADERS,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return response.json(), traceid
        except Exception as e:
            print(f"API调用失败: {str(e)} | TraceID: {traceid}")
            time.sleep(0.5)


def extract_information_types(response: Dict) -> List[str]:
    """从API响应中提取informationType列表"""
    return [
        item["informationType"] 
        for item in response.get("data", []) 
        if "informationType" in item
    ]

def process_questions(input_csv: Path, output_csv: Path) -> None:
    """
    处理CSV中的每个问题，记录不匹配的结果和空响应
    :param input_csv: 输入CSV文件路径
    :param output_csv: 输出CSV文件路径
    """
    with open(input_csv, 'r', encoding='utf-8') as f_in, \
         open(output_csv, 'w', newline='', encoding='utf-8') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        # 添加新列标记是否为空响应
        writer.writerow(["Question", "RequestedType", "ActualType", "TraceID", "IsEmptyResponse"])
        
        for row in reader:
            question = row[0] if row else ""
            if not question:
                continue
                
            for requested_type in INFORMATION_TYPES:
                try:
                    # 调用API
                    response, traceid = call_api_with_retry(
                        API_URL, question, API_PARAMS, requested_type
                    )
                    
                    # 情况1：空响应记录
                    if not response or not response.get('data'):
                        writer.writerow([
                            question,
                            requested_type,
                            "EMPTY_RESPONSE",  # 特殊标记
                            traceid,
                            "YES"  # 空响应标记
                        ])
                        print(f"空响应记录: {question[:20]}... | "
                             f"请求: {requested_type} | "
                             f"TraceID: {traceid}")
                        continue
                    
                    # 情况2：正常响应但类型不匹配
                    actual_types = extract_information_types(response)
                    for actual_type in actual_types:
                        if actual_type != requested_type:
                            writer.writerow([
                                question,
                                requested_type,
                                actual_type,
                                traceid,
                                "NO"  # 非空响应
                            ])
                            print(f"不匹配记录: {question[:20]}... | "
                                 f"请求: {requested_type} | "
                                 f"实际: {actual_type} | "
                                 f"TraceID: {traceid}")
                            
                except Exception as e:
                    # 情况3：异常情况记录
                    traceid = str(uuid.uuid4())  # 生成新traceid用于异常记录
                    writer.writerow([
                        question,
                        requested_type,
                        f"ERROR: {str(e)}",
                        traceid,
                        "ERROR"
                    ])
                    print(f"异常记录: {question[:20]}... | "
                         f"请求: {requested_type} | "
                         f"错误: {str(e)} | "
                         f"TraceID: {traceid}")
                    continue


if __name__ == "__main__":
    input_file = Path("c_query.csv")  # 输入CSV文件
    output_file = Path("mismatch_records.csv")  # 输出CSV文件
    
    process_questions(input_file, output_file)
    print(f"处理完成，不匹配记录已保存到: {output_file}")
