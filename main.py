"""
Main file to process log and performs the following tasks:
1) Get API
2) Calc Duration and visit each log
3) Calc Unique Visit

"""
import time
from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
import jdatetime
from sqlalchemy import create_engine
from Re_GetDuration import ExtractData, CompareData, SendData
from Re_Get_EPG import GetAPI
from Manual_EPG import ManualEPG
import pandas as pd
import pause


def get_epg(epg_start_time, epg_end_time, epg_hour_dif):
    # Convert time to desire format
    epg_start_time = ExtractData.convert_time(epg_start_time)
    epg_end_time = ExtractData.convert_time(epg_end_time)
    # Call API
    call_get_api = GetAPI('https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork',
                          "EPG99f06e12YHNbgtrfvCDEolmnbvc",
                          epg_start_time, epg_end_time)
    # Determining the data code
    epg_data = list(map(lambda x: call_get_api.post_api(x), list(range(30, 40))))
    print(epg_data)
    # Remove None Value
    pure_epg_data = list(filter(lambda x: x, epg_data))

    print(pure_epg_data)
    # Split Date and Time
    split_list = []
    for channel_epg in pure_epg_data:
        add_year_hour = list(map(lambda x: dict(x, start_year=GetAPI.split_date_time(x.get('Time_Play'))[0],
                                                start_hour=GetAPI.split_date_time(x.get('Time_Play'))[1],
                                                end_year=GetAPI.split_date_time(x.get('EP'))[0],
                                                end_hour=GetAPI.split_date_time(x.get('EP'))[1]
                                                ), channel_epg))
        split_list.append(add_year_hour)

    # Convert to list
    flat_split_list = list(chain.from_iterable(split_list))
    # flat_split_list = []
    flag = 0
    if not flat_split_list:
        flag = 1
        manual_df = pd.read_excel(r'manual_epg.xlsx', index_col=False)
        call_manual_epg = ManualEPG(manual_df, epg_start_time)
        manual_df['Time_Play'] = manual_df['ID_Day_Item'].apply(
            lambda input_x: call_manual_epg.insert_start_time(input_x))
        manual_df['EP'] = manual_df['ID_Day_Item'].apply(lambda input_x: call_manual_epg.insert_end_time(input_x))
        flat_split_list = manual_df.to_dict('records')
    print(flat_split_list)
    # Add Catchup Data
    # if flag == 0:
    #     manual_df = pd.read_excel(r'manual_epg.xlsx', index_col=False)
    #     call_manual_epg = ManualEPG(manual_df, epg_start_time)
    #     manual_df = manual_df[manual_df['channel_name'] == 'کچاپ']
    #     manual_df['Time_Play'] = manual_df['ID_Day_Item'].apply(
    #         lambda input_x: call_manual_epg.insert_start_time(input_x))
    #     manual_df['EP'] = manual_df['ID_Day_Item'].apply(lambda input_x: call_manual_epg.insert_end_time(input_x))
    #     manual_df_list = manual_df.to_dict('records')
    #     flat_split_list.append(manual_df_list)
    # Convert time to Georgian
    flat_split_list = list(
        map(lambda x: dict(x, Time_Play_x=(datetime.strptime(x.get('Time_Play'), '%m/%d/%Y %I:%M:%S %p') -
                                           timedelta(hours=epg_hour_dif, minutes=30))), flat_split_list))
    flat_split_list = list(
        map(lambda x: dict(x, Time_Play_x=(datetime.strftime(x.get('Time_Play_x'), '%Y-%m-%dT%H:%M:%S'))),
            flat_split_list))
    JTime = call_get_api.start_time
    df = pd.DataFrame.from_records(flat_split_list)
    # Add Jalali Date
    df['j_Time_Play'] = jdatetime.date.fromgregorian(day=int(JTime.split('/')[1]),
                                                     month=int(JTime.split('/')[0]),
                                                     year=int(JTime.split('/')[2]))
    # Insert to data base
    df = df.astype(str)
    db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak', pool_size=20, max_overflow=100)
    df.to_sql('Re_EpgRec2', db_engine.connect(), if_exists='replace', index=False)
    # df.to_excel('epg_pro1.xlsx', index=False)


def claculation_visit_duration(data_start_time, data_end_time, log_hour_dif):
    # connect to rabbitmq (must be check CompareData.calc_sessions)
    call_send_data = SendData('192.168.143.39', 'admin', 'R@bbitMQ1!')

    # Registration of specifications of start and end time of logs and elastic search
    call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
                                    "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
                                    '1m', 10000)
    # extract logs
    data_output = call_extract_data.get_data()
    data_output = data_output[
        ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
         'content_name', 'channel_name', 'content_type_id', 'action_id']]
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
        t1 = Thread(target=CompareData.calc_sessions,
                    args=[0, chunk, data_output, cr_epg_lst, 'durvisit', 'durvisit', [], []])
        # t2 = Thread(target=CompareData.calc_sessions,
        #             args=[36, chunk, data_output, cr_epg_lst, 'durvisit', 'durvisit', [], []])
        t1.start()
        # t2.start()
    # call_send_data.close_connection_rabbit()


def unique_visit(data_start_time, data_end_time, log_hour_dif):
    # ٍExtract session log
    call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
                                    "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
                                    '1m', 10000)
    session_output = call_extract_data.get_data()
    # Extract user log
    call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
                                    "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-login', 'time_stamp',
                                    '1m', 10000)
    user_output = call_extract_data.get_data()
    # Merge session and user based on session_id
    user_behave = pd.merge(session_output, user_output, on='session_id')
    # Drop duplicate logs
    user_behave = user_behave.drop_duplicates(['user_id', 'content_name', 'channel_name'])
    # Add date column
    user_behave['date'] = ''
    # Keep requirements columns
    user_behave = user_behave[['content_name', 'channel_name',
                               'user_id', 'time_stamp_x', 'sys_id_y',
                               'user_agent', 'referer', 'xReferer', 'date']]
    # Connect to the db and get EPG table
    db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')
    epg = pd.read_sql_query('SELECT * FROM public."Re_EpgRec"', con=db_engine.connect())
    cr_epg_lst = ExtractData.extract_epg(epg)
    # Convert df to dict
    user_behave_dict = user_behave.to_dict('records')
    # Convert time-stamp to desired format
    user_behave_dict = list(
        map(lambda x: dict(x, time_stamp_x=datetime.strptime(x.get('time_stamp_x'), '%Y-%m-%dT%H:%M:%SZ')),
            user_behave_dict))
    # Matching logs to epg
    user_behave_list = []
    for item in user_behave_dict:
        log_time = item.get('time_stamp_x')
        log_channel = item.get('channel_name')
        com_output = list(filter(lambda x: (x.get('Time_Play') < log_time < x.get('EP')) and
                                           (log_channel == x.get('channel_name')), cr_epg_lst))
        try:
            item.update(com_output[0])
            user_behave_list.append(item)
        except IndexError:
            pass
    # Convert some keys for desired parameters
    user_behave_list = list(map(lambda x: dict(x, time_stamp_y_new=x.get('Time_Play_x')), user_behave_list))
    user_behave_list = list(
        map(lambda x: dict(x, Time_Play_x=x.get('time_stamp_x') - timedelta(hours=4, minutes=30)), user_behave_list))
    user_behave_list = list(map(lambda x: dict(x, channel=x.get('channel_name')), user_behave_list))
    # Convert list of dict to df
    user_behave_df = pd.DataFrame(user_behave_list)
    user_behave_df = user_behave_df.astype(str)
    user_behave_df = user_behave_df[['Name_Item', 'channel',
                                     'user_id', 'Time_Play_x', 'sys_id_y',
                                     'user_agent', 'referer', 'xReferer', 'time_stamp_y_new', 'j_Time_Play']]
    # To insert data in rabbitmq, convert time to str is necessary
    user_behave_df["Time_Play_x"] = pd.to_datetime(user_behave_df["Time_Play_x"])
    user_behave_df["time_stamp_y_new"] = pd.to_datetime(user_behave_df["time_stamp_y_new"])
    user_behave_df['Time_Play_x'] = user_behave_df['Time_Play_x'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
    user_behave_df['time_stamp_y_new'] = user_behave_df['time_stamp_y_new'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
    # Send Data to DataFrame
    print(len(user_behave_df))
    user_behave_dict = user_behave_df.to_dict('records')
    call_send_data = SendData('192.168.143.39', 'admin', 'R@bbitMQ1!')
    for msg in user_behave_dict:
        # msg = q
        call_send_data.send_to_rabbit(msg, 'uniqueuser', 'uniqueuser')

    # return user_behave


count = 0
for day in range(0, 365):
    # if count == 0:
    #     count = 1
    #     print('1st')
    #     time.sleep(32400)
    # else:
    #     time.sleep(60)

    add_rec_start_time = datetime(2022, 5, 8, 00, 00, 1) + timedelta(days=day)
    add_rec_end_time = datetime(2022, 5, 8, 23, 59, 59) + timedelta(days=day)
    rec_start_time = datetime.strftime(add_rec_start_time, '%Y-%m-%dT%H:%M:%SZ')
    rec_end_time = datetime.strftime(add_rec_end_time, '%Y-%m-%dT%H:%M:%SZ')
    rec_hour_dif = 4
    get_epg(rec_start_time, rec_end_time, rec_hour_dif)
    print('2nd')
    time.sleep(30)
    print(datetime.now())
    claculation_visit_duration(rec_start_time, rec_end_time, rec_hour_dif)
    time.sleep(60)
    print(datetime.now())
    print('3rd')
    unique_visit(rec_start_time, rec_end_time, rec_hour_dif)
    print('finish')
    pause.until(add_rec_end_time + timedelta(days=1))

# use AP scheduler https://coderslegacy.com/python/apscheduler-tutorial-advanced-scheduler/
