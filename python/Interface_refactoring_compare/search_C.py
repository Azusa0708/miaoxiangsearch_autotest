import requests
import csv
import time
from typing import Dict, List, Optional, Tuple, Union
import uuid
from itertools import product

# 接口配置
OLD_API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/coreApp/modelV2"
NEW_API_URL = "http://llm-platform-cid.bdeastmoney.net/llm-platform-search-api/search/coreApp/modelV2"

COMMON_HEADERS = {"Content-Type": "application/json"}

# 接口参数
OLD_API_PARAMS = {
    "timeSupSize": 3,
    "decomposedFlag": True,
    "decomposedSize": 3,
    "size": 12,
    "useNewsSearch": False
}

NEW_API_PARAMS = {
    "timeSupSize": 3,
    "decomposedFlag": True,
    "decomposedSize": 3,
    "size": 12,
    "useNewsSearch": True
}

def call_api_with_retry(url: str, question: str, params: Dict) -> Tuple[Union[Dict, List], str]:
    """无限重试的API调用函数（保持0.5秒间隔）"""
    while True:
        traceid = str(uuid.uuid4())
        payload = {**params, "query": question.strip(), "traceid": traceid}
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
            continue

def extract_ids(response: Dict) -> List[str]:
    """从响应中提取id列表（保持原始顺序）"""
    return [item["id"] for item in response.get("data", []) if "id" in item]

def compare_results(old_ids: List[str], new_ids: List[str]) -> Dict:
    """
    增强版比较函数，返回包含完整差异分析的结果
    返回结构:
    {
        "set_diff": bool,         # 集合差异标志
        "order_diff": bool,       # 顺序差异标志
        "only_in_old": List[str], # 仅旧接口独有的ID
        "only_in_new": List[str], # 仅新接口独有的ID
        "order_changed": List[Tuple[int, str, str]], # 顺序变化详情
        "total_diff_count": int,  # 总差异数
        "set_diff_count": int,    # 集合差异数
        "order_diff_count": int   # 顺序差异数
    }
    """
    # 集合比较
    old_set = set(old_ids)
    new_set = set(new_ids)
    set_diff = old_set != new_set
    only_in_old = list(old_set - new_set)
    only_in_new = list(new_set - old_set)
    set_diff_count = len(only_in_old) + len(only_in_new)
    
    # 顺序比较
    order_diff = False
    order_changed = []
    order_diff_count = 0
    
    if not set_diff:  # 仅当无集合差异时检查顺序
        min_len = min(len(old_ids), len(new_ids))
        for i in range(min_len):
            if old_ids[i] != new_ids[i]:
                order_diff = True
                order_changed.append((i, old_ids[i], new_ids[i]))
                order_diff_count += 1
        
        # 处理长度差异
        if len(old_ids) != len(new_ids):
            order_diff = True
            diff_len = abs(len(old_ids) - len(new_ids))
            order_diff_count += diff_len
            for i in range(min_len, max(len(old_ids), len(new_ids))):
                old_id = old_ids[i] if i < len(old_ids) else ""
                new_id = new_ids[i] if i < len(new_ids) else ""
                order_changed.append((i, old_id, new_id))
    
    return {
        "set_diff": set_diff,
        "order_diff": order_diff,
        "only_in_old": only_in_old,
        "only_in_new": only_in_new,
        "order_changed": order_changed,
        "total_diff_count": set_diff_count + order_diff_count,
        "set_diff_count": set_diff_count,
        "order_diff_count": order_diff_count
    }

def process_question(question: str) -> Optional[Tuple[List[Dict], int]]:
    """处理单个问题，执行n次旧接口和n次新接口调用，比较9种组合"""
    # 执行n次旧接口和n次新接口调用
    old_results = []
    new_results = []
    
    for _ in range(3):
        try:
            # 旧接口调用
            old_res, old_traceid = call_api_with_retry(OLD_API_URL, question, OLD_API_PARAMS)
            old_ids = extract_ids(old_res if isinstance(old_res, dict) else {})
            old_results.append({
                "ids": old_ids,
                "traceid": old_traceid,
                "source": f"old_{len(old_results)+1}"
            })
            
            # 新接口调用
            new_res, new_traceid = call_api_with_retry(NEW_API_URL, question, NEW_API_PARAMS)
            new_ids = extract_ids(new_res if isinstance(new_res, dict) else {})
            new_results.append({
                "ids": new_ids,
                "traceid": new_traceid,
                "source": f"new_{len(new_results)+1}"
            })
            
            time.sleep(0.2)  # 避免请求过于密集
        except Exception as e:
            print(f"请求失败: {str(e)}")
            continue
    
    if len(old_results) < 3 or len(new_results) < 3:
        print(f"警告: 未能完成3次完整调用 (旧接口:{len(old_results)}次, 新接口:{len(new_results)}次)")
        if not old_results or not new_results:
            return None
    
    # 打印调试信息
    print(f"\n处理问题: {question[:60]}...")
    for i, res in enumerate(old_results, 1):
        print(f"  旧接口{i}: {len(res['ids'])}个ID | TraceID: {res['traceid']}")
    for i, res in enumerate(new_results, 1):
        print(f"  新接口{i}: {len(res['ids'])}个ID | TraceID: {res['traceid']}")
    
    # 生成所有可能的比较组合
    all_combinations = list(product(old_results, new_results))
    print(f"  生成{len(all_combinations)}种比较组合...")
    
    # 比较所有组合并记录差异
    compared_results = []
    for old_data, new_data in all_combinations:
        diff_result = compare_results(old_data["ids"], new_data["ids"])
        compared_results.append({
            "diff_analysis": diff_result,
            "old_traceid": old_data["traceid"],
            "new_traceid": new_data["traceid"],
            "old_source": old_data["source"],
            "new_source": new_data["source"],
            "old_ids": old_data["ids"],
            "new_ids": new_data["ids"]
        })
    
    # 找出差异最小的组合
    min_diff = min(compared_results, key=lambda x: x["diff_analysis"]["total_diff_count"])
    best_diff = min_diff["diff_analysis"]
    
    # 如果没有差异则返回None
    if best_diff["total_diff_count"] == 0:
        print(f"  最佳组合: {min_diff['old_source']}×{min_diff['new_source']} - 无差异")
        return None
    
    # 生成差异记录
    diff_records = []
    
    # 处理集合差异
    for id in best_diff["only_in_old"]:
        diff_records.append({
            "question": question,
            "old_id": id,
            "new_id": "",
            "diff_type": "only_in_old",
            "old_traceid": min_diff["old_traceid"],
            "new_traceid": "",
            "position": "",
            "source_combo": f"{min_diff['old_source']}×{min_diff['new_source']}"
        })
        
    for id in best_diff["only_in_new"]:
        diff_records.append({
            "question": question,
            "old_id": "",
            "new_id": id,
            "diff_type": "only_in_new",
            "old_traceid": "",
            "new_traceid": min_diff["new_traceid"],
            "position": "",
            "source_combo": f"{min_diff['old_source']}×{min_diff['new_source']}"
        })
    
    # 处理顺序差异
    if best_diff["order_diff"]:
        for pos, old_id, new_id in best_diff["order_changed"]:
            diff_records.append({
                "question": question,
                "old_id": old_id,
                "new_id": new_id,
                "diff_type": "order_diff",
                "old_traceid": min_diff["old_traceid"],
                "new_traceid": min_diff["new_traceid"],
                "position": pos,
                "source_combo": f"{min_diff['old_source']}×{min_diff['new_source']}"
            })
    
    print(f"  最佳组合: {min_diff['old_source']}×{min_diff['new_source']} - 发现{best_diff['total_diff_count']}处差异")
    return diff_records, best_diff["total_diff_count"]

def main(input_csv: str, output_csv: str):
    """主处理流程"""
    with open(input_csv, 'r', encoding='utf-8-sig') as readcsvfile:
        questions = [q.strip() for q in readcsvfile if q.strip()]
    
    total_processed = 0
    total_diffs_found = 0
    total_diff_items = 0

    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as writecsvfile:
        fieldnames = [
            "question", "old_id", "new_id", "diff_type",
            "timestamp", "old_traceid", "new_traceid", 
            "position", "total_diff_count", "source_combo"
        ]
        writer = csv.DictWriter(writecsvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for q in questions:
            try:
                result = process_question(q)
                total_processed += 1
                
                if result:
                    diff_records, total_diff = result
                    total_diffs_found += 1
                    total_diff_items += total_diff

                    for record in diff_records:
                        record.update({
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "total_diff_count": total_diff
                        })
                        writer.writerow(record)
                        writecsvfile.flush()
                    
                    print(f"[进度C] 已处理: {total_processed}/{len(questions)} | 差异问题: {total_diffs_found} | 当前差异数: {total_diff}")
                else:
                    print(f"[进度C] 已处理: {total_processed}/{len(questions)} | 差异问题: {total_diffs_found} | 验证通过")
                
                time.sleep(0.3)  # 间隔避免服务器压力
            except Exception as e:
                print(f"处理问题'{q[:30]}...'时出错: {str(e)}")
                continue

        # 最终汇总报告
        print("\n" + "="*60)
        print(f"{' 处理完成 ':^60}")
        print("="*60)
        print(f"总问题数: {len(questions)}")
        print(f"成功处理: {total_processed} (成功率: {total_processed/len(questions):.1%})")
        print(f"发现差异的问题数: {total_diffs_found} (占比: {total_diffs_found/total_processed:.1%})")
        print(f"总差异条目数: {total_diff_items}")
        print("="*60)

if __name__ == "__main__":
    main("c_query.csv", "id_diff_report_C.csv")
