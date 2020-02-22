import xml.etree.ElementTree as ET
import sqlite3
import time
from dateutil import rrule
import datetime
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np


def url_response(url, wait_time):
    while True:
        try:
            r = requests.get(url)
        except:
            print('Unable to connect to ' + url)
            print(f'Waiting {wait_time} sec')
            time.sleep(wait_time)
            pass
        else:
            return r


def get_root(path):
    try:
        root = ET.fromstring(path)
    except:
        print('Ошибка при парсинге xml')
    else:
        return root


def get_text(root):
    try:
        text = root.text
    except:
        return 'н/д'
    else:
        return text


class Parser_torgi_gov:
    def __init__(self, ns, scheme_path, usage_path, db_path):
        self.db_path = db_path
        self.ns = ns
        self.data = pd.read_csv(scheme_path, sep=';', encoding='cp1251')
        self.data = self.data.query('to_copy == 1')[['column_name', 'xpath']].reset_index(drop=True)
        to_add = pd.DataFrame([['bidMember_count', np.nan], ['odDetailedHref', np.nan]], columns=self.data.columns)
        self.data = pd.concat([self.data, to_add], ignore_index=True)
        self.usage_list = pd.read_csv(usage_path, sep=';', encoding='cp1251').query('to_copy == 1')['groundUsage_name']

    def create_db(self):
        conn = sqlite3.connect(self.db_path)
        pd.DataFrame(columns=self.data['column_name']).to_sql('lots', con=conn)
        conn.commit()
        conn.close()

    def insert_to_db(self, df):
        conn = sqlite3.connect(self.db_path)
        df.set_index('column_name').T.to_sql('lots', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()

    def check_agri(self, root):
        groundType = get_text(root.find(f'./{self.ns}groundType/{self.ns}name'))
        groundUsage = get_text(root.find(f'./{self.ns}groundUsage/{self.ns}name'))
        mission = get_text(root.find(f'./{self.ns}mission'))
        if groundType not in ['Земли сельскохозяйственного назначения', 'Земли населенных пунктов', 'н/д']:
            return False
        elif groundUsage not in list(self.usage_list):
            return False
        elif (groundUsage == 'н/д') and (
                'ельско' not in mission and 'астениевод' not in mission and 'выращ' not in mission):
            return False
        else:
            return True

    def get_info(self, content, url):
        root = get_root(content)
        lots = root.findall(f'./{self.ns}notification/{self.ns}lot')
        if len(lots) == 0:
            return None
        for lot in lots:
            if not self.check_agri(lot):
                continue
            df = self.data.copy()
            lot_info = []
            for path in self.data['xpath'].dropna():
                if 'notification' in path:
                    lot_info.append(get_text(root.find(path)))
                else:
                    lot_info.append(get_text(lot.find(path)))
            # количество участников
            lot_info.append(len(lot.findall(f'./{self.ns}results/{self.ns}bidMember')))
            # ссылка на xml с извещением
            lot_info.append(url)
            df['values'] = pd.Series(lot_info)
            self.insert_to_db(df[['column_name', 'values']])

    def dllots(self, publishDateFrom, publishDateTo):
        url = f'https://torgi.gov.ru/opendata/7710349494-torgi/\
                                    data.xml?bidKind=2&publishDateFrom={publishDateFrom}&publishDateTo={publishDateTo}'
        r = url_response(url, 10)
        contents = r.text
        root = get_root(contents)
        for child in tqdm(root, desc='Month loop', leave=False):
            if hasattr(child.find(f'{self.ns}odDetailedHref'), 'text'):
                url = child.find(f'{self.ns}odDetailedHref').text
                r = url_response(url, 10)
                contents = r.text
                self.get_info(contents, url)
