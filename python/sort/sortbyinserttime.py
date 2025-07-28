import json

def process_json_file(input_filepath, output_filepath):
    """
    读取 JSON 文件，按 'insertTime' 排序，去除 'decomposedQueries' 字段，
    并写入新的 JSON 文件。

    Args:
        input_filepath (str): 输入 JSON 文件的路径。
        output_filepath (str): 输出 JSON 文件的路径。
    """
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"错误：文件 '{input_filepath}' 未找到。")
        return
    except json.JSONDecodeError:
        print(f"错误：无法解析文件 '{input_filepath}'，请检查 JSON 格式。")
        return

    # 按 'insertTime' 从小到大排序
    sorted_data = sorted(data, key=lambda x: x.get('insertTime', 0))

    # 去除 'decomposedQueries' 字段
    processed_data = []
    for item in sorted_data:
        new_item = {k: v for k, v in item.items() if k != 'decomposedQueries'}
        processed_data.append(new_item)

    # 写入新的 JSON 文件
    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        print(f"数据已成功处理并写入到 '{output_filepath}'。")
    except IOError:
        print(f"错误：无法写入文件 '{output_filepath}'。")

if __name__ == "__main__":
    # 替换成你的输入和输出文件路径
    input_json_file = "modelV2CoreQuery.json"  # 假设你的原始 JSON 文件名为 input.json
    output_json_file = "output.json" # 将处理后的数据写入 output.json

    process_json_file(input_json_file, output_json_file)