import requests
import re
import pandas as pd
import os
from bs4 import BeautifulSoup

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

        # --- 核心修改：精准抓取网页顶部的 Coins 数据表格 ---
        realtime_data = {}
        table = soup.find('table', {'class': 'table'})
        if table:
            rows = table.find_all('tr')[1:] # 跳过表头
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    # 提取区间，例如 [0.1 - 1) 转换为 0.1-1
                    label = cols[0].get_text(strip=True).replace('[', '').replace(')', '').replace(' ', '')
                    # 提取 Coins 列 (第4列)，去掉逗号和 "BTC" 后缀
                    coins_val = cols[3].get_text(strip=True).split(' ')[0].replace(',', '')
                    # 提取 USD 列 (第5列)
                    usd_val = cols[4].get_text(strip=True).replace(',', '').replace('$', '')
                    
                    realtime_data[label] = {
                        "coins": coins_val,
                        "usd": usd_val
                    }

        # --- 抓取历史地址数序列 ---
        pattern = r'\[new Date\("(.*?)"\),(.*?)\]'
        matches = re.findall(pattern, html_content)
        
        if not matches:
            print("未能提取到数据，请检查网页结构。")
            return
            
        data = []
        # 定义标准列名
        columns = ['0-0.1', '0.1-1', '1-10', '10-100', '100-1,000', '1,000-10,000', '10,000-100,000', '100,000-1,000,000']
        
        for match in matches:
            raw_date = match[0].replace('/', '-')
            numbers = match[1].split(',')
            
            row = {'Date': raw_date}
            for i, col in enumerate(columns):
                row[col] = int(numbers[i])
                # 只在最后一条数据（今日）注入真实的 Coins 和 USD
                if match == matches[-1]:
                    row[f"{col}_coins"] = realtime_data.get(col, {}).get("coins", "0")
                    row[f"{col}_usd"] = realtime_data.get(col, {}).get("usd", "0")
                else:
                    # 历史数据中没有 Coins 序列，设为空或 0，前端会据此判断是否展示
                    row[f"{col}_coins"] = "0"
                    row[f"{col}_usd"] = "0"
            data.append(row)
            
        new_df = pd.DataFrame(data)
        
        data_dir = "data"
        if not os.path.exists(data_dir): os.makedirs(data_dir)
        
        csv_filename = f"{data_dir}/btc_history_full.csv"
        json_filename = f"{data_dir}/data.json"

        new_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        new_df.to_json(json_filename, orient='records', force_ascii=False)
        
        print(f"数据同步成功！已获取 {len(realtime_data)} 个区间的真实持仓数据。")
        return new_df

    except Exception as e:
        print(f"抓取过程中出现错误: {e}")
        return None

if __name__ == "__main__":
    fetch_full_history()
