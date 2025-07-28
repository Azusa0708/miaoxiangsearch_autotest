import pandas as pd

def deduplicate_first_column(input_file, output_file):
    """
    对CSV文件的第一列进行去重，并将结果保存到新文件
    
    参数:
        input_file (str): 输入CSV文件路径
        output_file (str): 输出CSV文件路径
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(input_file)
        
        # 检查数据框是否为空
        if df.empty:
            print("警告: 输入文件为空!")
            return
        
        # 获取第一列列名
        first_column = df.columns[0]
        
        # 按第一列去重，保留第一次出现的行
        deduplicated_df = df.drop_duplicates(subset=[first_column], keep='first')
        
        # 保存到新CSV文件
        deduplicated_df.to_csv(output_file, index=False)
        
        print(f"去重完成! 原始记录数: {len(df)}，去重后记录数: {len(deduplicated_df)}")
        print(f"结果已保存到: {output_file}")
        
    except FileNotFoundError:
        print(f"错误: 文件 {input_file} 未找到!")
    except Exception as e:
        print(f"发生错误: {str(e)}")

# 使用示例
input_csv = "id_diff_report_C.csv"  # 输入CSV文件路径
output_csv = "retryc.csv"  # 输出CSV文件路径

deduplicate_first_column(input_csv, output_csv)
