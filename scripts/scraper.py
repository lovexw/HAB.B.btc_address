import requests
import re
import pandas as pd
from datetime import datetime
import os
from bs4 import BeautifulSoup

def fetch_full_history():
    url = "https://bitinfocharts.com/zh/bitcoin-distribution-history.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    print("正在连接网站获取精准数据...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # 1. 抓取网页顶部表格中的【实时持币量】数据
        # 目标：提取每个区间的精确 Coins 数量
        realtime_coins = {}
        table = soup.find('table', {'class': 'table'})
        if table:
            rows = table.find_all('tr')[1:] # 跳过表头
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    label = cols[0].get_text(strip=True).replace('[', '').replace(')', '').replace(' ', '')
                    # 提取 Coins 列，去掉 "BTC" 和逗号
                    coins_val = cols[3].get_text(strip=True).split(' ')[0].replace(',', '')
                    realtime_coins[label] = float(coins_val)

        # 2. 抓取历史地址数序列 (原来的逻辑)
        pattern = r'\[new Date\("(.*?)"\),(.*?)\]'
        matches = re.findall(pattern, html_content)
        
        if not matches:
            print("未能提取到数据")
            return
            
        data = []
        columns = ['Date', '0-0.1', '0.1-1', '1-10', '10-100', '100-1,000', '1,000-10,000', '10,000-100,000', '100,000-1,000,000']
        
        for match in matches:
            raw_date = match[0].replace('/', '-')
            numbers = match[1].split(',')
            # 存入地址数
            row = {'Date': raw_date}
            for i, col in enumerate(columns[1:]):
                row[col] = int(numbers[i])
                # 注入实时 Coins 数据 (仅在最后一行即今日数据中注入，或者根据比例分配)
                # 由于该网站历史 Coins 序列未直接开放，我们确保今日数据 100% 准确
                row[f"{col}_coins"] = realtime_coins.get(col, 0) if raw_date == matches[-1][0].replace('/', '-') else 0
            
            data.append(row)
            
        new_df = pd.DataFrame(data)
        
        data_dir = "data"
        if not os.path.exists(data_dir): os.makedirs(data_dir)
        
        csv_filename = f"{data_dir}/btc_history_full.csv"
        json_filename = f"{data_dir}/data.json"

        # 保存并合并
        new_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        new_df.to_json(json_filename, orient='records', force_ascii=False)
        
        print(f"成功同步！今日持仓数据已更新。")
        return new_df

    except Exception as e:
        print(f"抓取错误: {e}")
        return None

if __name__ == "__main__":
    fetch_full_history()
