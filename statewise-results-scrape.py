# scraping election results pages every few mins

import pandas as pd
from bs4 import BeautifulSoup
import requests
from math import ceil
import datetime
import time

pc_file = "2024-PC-attributes.csv"

df1 = pd.read_csv(pc_file)

df_states = df1.drop_duplicates(['s_u', 'st_code'])[['s_u', 'st_code', 'st_name']].copy().reset_index(drop=True)

while True:
    collector = []
    filename = f"state_wise_results/results_{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.csv"
    for sN, srow in df_states.iterrows():
        
        padded_st_code = str(srow['st_code']).rjust(2,'0')
        num_pcs = len(df1.query(f""" st_code == {srow['st_code']} """))
        pages = ceil(num_pcs / 20 )
        print(f"{srow['s_u']=} {srow['st_code']=} {num_pcs=} {pages=}")
        
        for page_num in range(1,pages+1):
            url1 = f"https://results.eci.gov.in/PcResultGenJune2024/statewise{srow['s_u']}{padded_st_code}{page_num}.htm"
            print(url1)

            try:
                r = requests.get(url1)
            except Exception as e:
                print(f"Failed to fetch from url {url1}, skipping to next after little pause")
                time.sleep(10)
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
                row = {}
                row["s_u"] = srow['s_u']
                row["st_code"] = srow['st_code']
                row["st_name"] = srow['st_name']
                row["pc_name"] = h2[0].text
                row["pc_no"] = h2[1].text
                row["leading_candidate"] = h2[2].text
                row["leading_party"] = leading_party = h2[3].select_one('tbody').select_one('td').text
                row["trailing_candidate"] = h2[4].text
                row["trailing_party"] = h2[5].select_one('tbody').select_one('td').text
                row["margin"] = h2[6].text
                row["result_status"] = h2[7].text
                row["source_url"] = url1
                row["timestamp"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                collector.append(row)
            
            # incremental saving
            df2 = pd.DataFrame(collector)
            df2.to_csv(filename, index=False)

    df2 = pd.DataFrame(collector)
    df2.to_csv(filename, index=False)

    print(f"Saved {len(df2)} rows to {filename}")
    print("-"*60)
    time.sleep(5*60)

print("Got out")
