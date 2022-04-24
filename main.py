from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
import jdatetime
from sqlalchemy import create_engine
from Re_GetDuration import ExtractData, CompareData, SendData
from Re_Get_EPG import GetAPI
import pandas as pd


def get_epg():
    call_get_api = GetAPI('https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork',
                          "EPG99f06e12YHNbgtrfvCDEolmnbvc",
                          '04/18/2022', '04/18/2022')
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
    flat_split_list = list(
        map(lambda x: dict(x, Time_Play_x=(datetime.strptime(x.get('Time_Play'), '%m/%d/%Y %I:%M:%S %p') -
                                           timedelta(hours=4, minutes=30))), flat_split_list))
    flat_split_list = list(
        map(lambda x: dict(x, Time_Play_x=(datetime.strftime(x.get('Time_Play_x'), '%Y-%m-%dT%H:%M:%S'))),
            flat_split_list))
    JTime = call_get_api.start_time
    df = pd.DataFrame.from_records(flat_split_list)
    df['j_Time_Play'] = jdatetime.date.fromgregorian(day=int(JTime.split('/')[1]),
                                                     month=int(JTime.split('/')[0]),
                                                     year=int(JTime.split('/')[2]))
    df = df.astype(str)
    db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak', pool_size=20, max_overflow=100)
    df.to_sql('Re_EpgRec', db_engine.connect(), if_exists='replace', index=False)
    # df.to_excel('epg_pro1.xlsx', index=False)


def claculation_visit_duration():
    # connect to rabbitmq (must be check CompareData.calc_sessions)
    call_send_data = SendData('192.168.143.39', 'admin', 'R@bbitMQ1!')

    # Registration of specifications of start and end time of logs and elastic search
    call_extract_data = ExtractData('2022-04-18T23:00:01Z', '2022-04-18T23:59:59Z', 4, 22,
                                    "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
                                    '1m', 10000)
    # extract logs
    data_output = call_extract_data.get_data()
    # GET dataframe of EPG and customize field
    db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')
    epg = pd.read_sql_query('SELECT * FROM public."Re_EpgRec"', con=db_engine.connect())
    # df = pd.read_excel('epg_pro1.xlsx', index_col=False)
    cr_epg_lst = ExtractData.extract_epg(epg)

    # extract set of session id
    session_set = set(data_output['session_id'])

    # calc sum duration and visit of sessions
    # final = CompareData.calc_sessions(session_set, [], [])

    # convert logs data to multi chunks
    session_lst = list(session_set)
    chunks = [session_lst[s_id:s_id + 50] for s_id in range(0, len(session_lst), 50)]
    # start of calculations
    for chunk in chunks:
        t1 = Thread(target=CompareData.calc_sessions, args=[chunk, data_output, cr_epg_lst, [], []])
        t1.start()
    # call_send_data.close_connection_rabbit()


get_epg()
claculation_visit_duration()
