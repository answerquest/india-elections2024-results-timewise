# scraping election results pages every few mins

import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
import time

pc_file = "2024-PC-attributes.csv"

df1 = pd.read_csv(pc_file)

while True:
    collector = []
    filename = f"pc_wise_results/pc_results_{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.csv"
    
    for N,row in df1.iterrows():
        url1 = f"https://results.eci.gov.in/PcResultGenJune2024/Constituencywise{row['s_u']}{str(row['st_code']).rjust(2,'0')}{str(row['pc_no']).replace('.0','')}.htm"
        print(url1)

        try:
            r = requests.get(url1)
        except Exception as e:
            print(f"Failed to fetch from url {url1}, skipping to next after little pause")
            time.sleep(5)
            continue
        s = BeautifulSoup(r.text, 'html.parser')
        test = s.select_one('div.table-responsive > table')
        if not test:
            continue
        h1 = s.select_one('div.table-responsive > table').select_one('tbody').find_all('tr', recursive=False)
        print("h1:",len(h1))
        
        for trow in h1:
            h2 = trow.find_all('td', recursive=False)
            # print(len(h2), h2)
            line = {}
            line['s_u'] = row['s_u']
            line['st_code'] = row['st_code']
            line['st_name'] = row['st_name']
            line['pc_no'] = row['pc_no']
            line['pc_name'] = row['pc_name']
            
            line['candidate_name'] = h2[1].text
            line['party'] = h2[2].text
            line['evm_votes'] = h2[3].text
            line['postal_votes'] = h2[4].text
            line['total_votes'] = h2[5].text
            line['vote_percentage'] = h2[6].text

            line["source_url"] = url1
            line["timestamp"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # print(line)
            collector.append(line)
        # break

        # incremental saving by each constituency
        df2 = pd.DataFrame(collector)
        df2.to_csv(filename, index=False)
    
    print(f"Saved {len(df2)} rows to {filename}")
    print("-"*60)
    time.sleep(10)

print("Got out")
