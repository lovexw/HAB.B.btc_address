import requests
import re
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime

def fetch_full_history():
    url = "https://bitinfocharts.com/zh/bitcoin-distribution-history.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    print("正在连接网站获取 100% 准确数据...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # --- 步骤 1: 解析网页顶部的实时数据表格 ---
        # 我们要拿到每个区间的准确 Coins 和 USD 数值
        realtime_map = {}
        table = soup.find('table', {'class': 'table'})
        if table:
            rows = table.find_all('tr')[1:] # 跳过表头
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    # 转换区间名称，例如 "[0.1 - 1)" 转换为 "0.1-1"
                    raw_label = cols[0].get_text(strip=True)
                    label = raw_label.replace('[', '').replace(')', '').replace(' ', '')
                    
                    # 提取 Coins (第4列) 和 USD (第5列)
                    # 去掉 "BTC"、逗号和美元符号
                    coins_val = cols[3].get_text(strip=True).split(' ')[0].replace(',', '')
                    usd_val = cols[4].get_text(strip=True).replace(',', '').replace('$', '')
                    
                    realtime_map[label] = {
                        "coins": coins_val,
                        "usd": usd_val
                    }
        else:
            print("警告：未找到实时数据表格，请检查网页结构")

        # --- 步骤 2: 解析历史地址数序列 ---
        pattern = r'\[new Date\("(.*?)"\),(.*?)\]'
        matches = re.findall(pattern, html_content)
        
        if not matches:
            print("未能提取到历史趋势数据。")
            return
            
        data_list = []
        # 定义标准的 8 个持币区间（与网页 labels 对应）
        tier_names = [
            '0-0.1', '0.1-1', '1-10', '10-100', '100-1,000', 
            '1,000-10,000', '10,000-100,000', '100,000-1,000,000'
        ]
        
        for i, match in enumerate(matches):
            raw_date = match[0].replace('/', '-')
            numbers = match[1].split(',')
            
            row = {'Date': raw_date}
            # 填充每个区间的地址数
            for idx, tier in enumerate(tier_names):
                # 网页数据有时会多出一两列，我们只取前 8 个区间
                val = int(numbers[idx]) if idx < len(numbers) else 0
                row[tier] = val
                
                # 关键：只有最新的一条数据（数组最后一位）才填入真实的 Coins 和 USD
                # 历史数据的 Coins 网站未提供序列，填入 0
                if i == len(matches) - 1:
                    row[f"{tier}_coins"] = realtime_map.get(tier, {}).get("coins", "0")
                    row[f"{tier}_usd"] = realtime_map.get(tier, {}).get("usd", "0")
                else:
                    row[f"{tier}_coins"] = "0"
                    row[f"{tier}_usd"] = "0"
            
            data_list.append(row)
            
        # --- 步骤 3: 保存数据 ---
        new_df = pd.DataFrame(data_list)
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            
        csv_path = f"{data_dir}/btc_history_full.csv"
        json_path = f"{data_dir}/data.json"

        # 保存为 CSV 和 JSON
        new_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        new_df.to_json(json_path, orient='records', force_ascii=False)
        
        print(f"数据抓取成功！已保存 {len(new_df)} 条记录。")
        print(f"今日数据校验：100-1000 区间持币量为 {realtime_map.get('100-1,000', {}).get('coins')} BTC")
        return new_df

    except Exception as e:
        print(f"发生错误: {e}")
        return None

if __name__ == "__main__":
    fetch_full_history()
