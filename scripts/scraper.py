import requests
import re
import pandas as pd
from datetime import datetime
import os

def fetch_full_history():
    url = "https://bitinfocharts.com/zh/bitcoin-distribution-history.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    print("正在连接网站获取数据，请稍候...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html_content = response.text
        
        pattern = r'\[new Date\("(.*?)"\),(.*?)\]'
        matches = re.findall(pattern, html_content)
        
        if not matches:
            print("未能提取到历史数据，网页结构可能发生了变化。")
            return
            
        data = []
        for match in matches:
            raw_date = match[0]
            clean_date = raw_date.replace('/', '-')
            numbers = match[1].split(',')
            row = [clean_date] + numbers
            data.append(row)
            
        columns = [
            'Date', '0-0.1', '0.1-1', '1-10', '10-100', '100-1,000', 
            '1,000-10,000', '10,000-100,000', '100,000-1,000,000'
        ]
        
        new_df = pd.DataFrame(data, columns=columns)
        
        # --- 数据保存逻辑修改开始 ---
        # 确保 data 目录存在
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            
        # 定义输出路径
        csv_filename = f"{data_dir}/btc_history_full.csv"
        json_filename = f"{data_dir}/data.json" # 给前端展示用的 JSON
        
        if os.path.exists(csv_filename):
            existing_df = pd.read_csv(csv_filename)
            print(f"发现现有数据，包含 {len(existing_df)} 条记录")
            
            latest_date = existing_df['Date'].max()
            print(f"现有数据最新日期: {latest_date}")
            
            new_data = new_df[new_df['Date'] > latest_date]
            
            if len(new_data) > 0:
                combined_df = pd.concat([existing_df, new_data], ignore_index=True)
                combined_df.sort_values('Date', inplace=True)
                print(f"新增 {len(new_data)} 条记录，总共 {len(combined_df)} 条记录")
                
                # 同步保存 CSV 和 JSON
                combined_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                combined_df.to_json(json_filename, orient='records', force_ascii=False)
                print(f"数据已更新并保存至: {csv_filename} 和 {json_filename}")
            else:
                print("没有新的数据需要添加")
                return existing_df
        else:
            # 首次运行，同步保存 CSV 和 JSON
            new_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            new_df.to_json(json_filename, orient='records', force_ascii=False)
            print(f"首次运行，数据已保存至: {csv_filename} 和 {json_filename}")
        
        final_df = pd.read_csv(csv_filename)
        print(f"数据更新完成！当前总计 {len(final_df)} 天的数据")
        print(f"数据起始日期: {final_df['Date'].iloc[0]}")
        print(f"数据截止日期: {final_df['Date'].iloc[-1]}")
        
        return final_df
        # --- 数据保存逻辑修改结束 ---

    except requests.exceptions.RequestException as e:
        print(f"网络请求错误: {e}")
        return None
    except Exception as e:
        print(f"抓取过程中出现错误: {e}")
        return None

def test_local():
    print("开始本地测试...")
    result = fetch_full_history()
    if result is not None:
        print("测试成功！数据已更新。")
        print(f"数据形状: {result.shape}")
        print("最近几条数据预览:")
        print(result.tail())
    else:
        print("测试失败，请检查网络连接和依赖项。")

if __name__ == "__main__":
    test_local()
