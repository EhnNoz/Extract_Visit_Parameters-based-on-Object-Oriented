import ast
import json
import re
from itertools import chain
import requests
import pandas as pd
from sqlalchemy import create_engine


class GetAPI:
    def __init__(self, url, key, start_time, end_time):
        self.url = url
        self.key = key
        self.time_set = ",\"DTStart\":\"{}\",\"DTEnd\":\"{}\"".format(start_time, end_time)

    def post_api(self, sid):
        # extract channel_name
        with open('channel_name.txt', 'r', encoding='utf-8') as f:
            read_lines = f.read()
            code_and_channel = ast.literal_eval(read_lines)
        channel_name = code_and_channel.get(sid)
        channel_code = sid
        # prepare payload to request
        sid_set = "\"SID_Network\":{}".format(sid)
        set_load = '{' + str(sid_set) + str(self.time_set) + '}'
        payload = {"JsonData": set_load, "Key": "{}".format(self.key)}
        data = requests.post(self.url, json=payload)
        data_content = data.content
        data_content = data_content.decode('utf-8')
        json_acceptable_string = data_content.replace("'", "\"")
        json_load = json.loads(json_acceptable_string)
        get_data = json_load.get('JsonData')
        try:
            get_data = ast.literal_eval(get_data)
            get_data = [{'channel_name': channel_name, 'channel_code': channel_code, **item} for item in get_data]
            return get_data
        except ValueError:
            return False

    @staticmethod
    def split_date_time(input_time):
        year = re.split(' ', input_time)[0]
        hour = re.split(' ', input_time)[1]
        return year, hour


call_get_api = GetAPI('https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork', "EPG99f06e12YHNbgtrfvCDEolmnbvc",
                      '04/11/2022', '04/11/2022')
epg_data = list(map(lambda x: call_get_api.post_api(x), list(range(30, 40))))
pure_epg_data = list(filter(lambda x: x, epg_data))

split_list = []
for channel_epg in pure_epg_data:
    add_year_hour = list(map(lambda x: dict(x, start_year=GetAPI.split_date_time(x.get('Time_Play'))[0],
                                            start_hour=GetAPI.split_date_time(x.get('Time_Play'))[1],
                                            end_year=GetAPI.split_date_time(x.get('EP'))[0],
                                            end_hour=GetAPI.split_date_time(x.get('EP'))[1]
                                            ), channel_epg))
    split_list.append(add_year_hour)

flat_split_list = list(chain.from_iterable(split_list))
df = pd.DataFrame.from_records(flat_split_list)
db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')
df.to_sql('Re_EpgRec', db_engine.connect(), if_exists='replace', index=False)
df.to_excel('epg_pro.xlsx', index=False )