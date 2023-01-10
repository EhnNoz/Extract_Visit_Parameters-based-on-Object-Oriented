"""
Main file to process log and performs the following tasks:
1) Get API
2) Calc Duration and visit each log
3) Calc Unique Visit

"""
import itertools
import time
from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
import jdatetime
from elasticsearch import Elasticsearch, helpers
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
    # # Determining the data code
    # epg_data = list(map(lambda x: call_get_api.post_api(x), list(range(21, 230))))
    # print(epg_data)
    # # Remove None Value
    # pure_epg_data = list(filter(lambda x: x, epg_data))
    #
    # print(pure_epg_data)
    # # Split Date and Time
    # split_list = []
    # for channel_epg in pure_epg_data:
    #     add_year_hour = list(map(lambda x: dict(x, start_year=GetAPI.split_date_time(x.get('Time_Play'))[0],
    #                                             start_hour=GetAPI.split_date_time(x.get('Time_Play'))[1],
    #                                             end_year=GetAPI.split_date_time(x.get('EP'))[0],
    #                                             end_hour=GetAPI.split_date_time(x.get('EP'))[1]
    #                                             ), channel_epg))
    #     split_list.append(add_year_hour)
    #
    # # Convert to list
    # flat_split_list = list(chain.from_iterable(split_list))
    # print(flat_split_list)
    flat_split_list = []
    if not flat_split_list:
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
    df.to_sql('Re_EpgRec', db_engine.connect(), if_exists='replace', index=False)
    df.to_sql('Save_EpgRec', db_engine.connect(), if_exists='append', index=False)

    # df.to_excel('epg_pro1.xlsx', index=False)


def extract_action_elastic(data_start_time, data_end_time, log_hour_dif):
    call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
                                    "https://norozzadeh:SepehR!6$@192.168.143.35:9200", 'live-action', 'time_stamp',
                                    '1m', 10000)
    # extract logs
    data_output = call_extract_data.get_data()
    print(data_output)
    data_output = data_output[
        ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
         'content_name', 'channel_name', 'content_type_id', 'action_id']]
    # data_output.to_excel('test_data_out.xlsx')
    return data_output

def extract_login_elastic(data_start_time, data_end_time, log_hour_dif):
    call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
                                    "https://norozzadeh:SepehR!6$@192.168.143.35:9200", 'live-login', 'time_stamp',
                                    '1m', 10000)
    user_output = call_extract_data.get_data()
    # user_output.to_excel('test_user_out.xlsx')
    return user_output
    # data_output.to_csv('test_data.csv')


def claculation_visit_duration(in_data):
    # connect to rabbitmq (must be check CompareData.calc_sessions)
    call_send_data = SendData('10.32.141.52', 'admin', 'R@bbitMQ1!')

    # # Registration of specifications of start and end time of logs and elastic search
    # call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
    #                                 "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
    #                                 '1m', 10000)
    # # extract logs
    # data_output = call_extract_data.get_data()
    # print(data_output)
    data_output = in_data[
        ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
         'content_name', 'channel_name', 'content_type_id', 'action_id']]
    # data_output.to_csv('test_data.csv')
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
        # t1 = Thread(target=CompareData.calc_sessions,
        #             args=[0, chunk, data_output, cr_epg_lst, 'test_dur', 'test_dur', [], []])
        # t2 = Thread(target=CompareData.calc_sessions,
        #             args=[36, chunk, data_output, cr_epg_lst, 'durvisit', 'durvisit', [], []])
        t1.start()
        # break
        # t2.start()
    # call_send_data.close_connection_rabbit()


def calculation_catchup(in_data):
    sessions = set(in_data['session_id'])
    calc_list = []

    for session in sessions:
            # print(session)
            session_data_output = in_data[in_data['session_id'] == session]
            call_comparedata = CompareData(session_data_output, [])
            calced_log = call_comparedata.log_thread('09', '3', '1', '3', '1', 10)
            calc_list.append(calced_log)
    flat_calc_list = list(itertools.chain.from_iterable(calc_list))

    # df1 = pd.DataFrame().from_dict(flat_calc_list)
    # # df1 = pd.DataFrame()
    # # df1['check2'] = str(calced_log)
    # df1.to_excel('df1.xlsx')
    calced_log = list(
        map(lambda x: dict(x, Time_Play_x=x.get('time_stamp') - timedelta(hours=4, minutes=30)), flat_calc_list))
    # df1['check3'] = str(calced_log)
    calced_log = list(map(lambda x: dict(x, channel=x.get('channel_name')), calced_log))
    calced_log = list(map(lambda x: dict(x, Name_Item=x.get('content_name')), calced_log))
    calced_log = [{key: val for key, val in sub.items() if key != 'channel_name'} for sub in calced_log]
    calced_log = [{key: val for key, val in sub.items() if key != 'content_name'} for sub in calced_log]
    calced_log = [{key: val for key, val in sub.items() if key != '@timestamp'} for sub in calced_log]
    calced_log = list(map(lambda x: dict(x, j_Time_Play=jdatetime.date.fromgregorian(day=x.get('time_stamp').day,
                                                                                     month=x.get('time_stamp').month,
                                                                                     year=x.get('time_stamp').year)),
                          calced_log))

    # calced_log = list(map(lambda x: dict(x, dur=(x.get('diff').total_seconds() + 60) / 60), calced_log))
    calced_log = list(map(lambda x: dict(x, dur=(x.get('diff').total_seconds() ) / 60), calced_log))

    log_df = pd.DataFrame.from_records(calced_log)

    log_df['Time_Play_x'] = log_df['Time_Play_x'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))

    log_df['time_stamp'] = log_df['time_stamp'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))

    log_df = log_df.astype(str)
    log_dct = log_df.to_dict('records')

    call_send_data = SendData('10.32.141.52', 'admin', 'R@bbitMQ1!')

    for msg in log_dct:
        call_send_data.send_to_rabbit(msg, 'catchup_session', 'catchup_session')


def unique_visit(in_data, out_data):
    # es = Elasticsearch(hosts="http://norozzadeh:Kibana@110$%^@192.168.143.34:9200")

    # ٍExtract session log
    # call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
    #                                 "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
    #                                 '1m', 10000)
    # session_output = call_extract_data.get_data()
    session_output = in_data
    sesssion_output = session_output[session_output['action_id'] != 2]
    sesssion_output = session_output[session_output['action_id'] != 7]
    sesssion_output_dct = sesssion_output.to_dict('records')
    session_output = pd.DataFrame.from_dict(sesssion_output_dct)
    # Extract user log
    # call_extract_data = ExtractData(data_start_time, data_end_time, log_hour_dif, 22,
    #                                 "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-login', 'time_stamp',
    #                                 '1m', 10000)
    # user_output = call_extract_data.get_data()
    user_output = out_data
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
    # es = Elasticsearch(hosts=self.engine_elastic)
    #
    # doc = [{'_id': 1, 'price': 10, 'productID': 'XHDK-A-1293-#fJ3'},
    #        {'_id': 2, "price": 20, "productID": "KDKE-B-9947-#kL5"},
    #        {'_id': 3, "price": 30, "productID": "JODL-X-1937-#pV7"},
    #        {'_id': 4, "price": 30, "productID": "QQPX-R-3956-#aD8"}]
    #
    call_send_data = SendData('10.32.141.52', 'admin', 'R@bbitMQ1!')
    count = 0
    for msg in user_behave_dict:
        # tmp_msg = {'salam':'salam'}
        # count +=1
        # msg = q
        call_send_data.send_to_rabbit(msg, 'uniqueuser', 'uniqueuser')
        # if count == 10:
        #     break
        # call_send_data.send_to_rabbit(msg, 'iframe', 'iframe')

    # return user_behave


count = 0
for day in range(0, 365):
    # if count == 0:
    #     count = 1
    #     print('1st')
    #     time.sleep(30000)
    # else:
    #     time.sleep(60)

    add_rec_start_time = datetime(2023, 1, 7, 0, 0, 1) + timedelta(days=day)
    add_rec_end_time = datetime(2023, 1, 7, 23, 59, 59) + timedelta(days=day)
    rec_start_time = datetime.strftime(add_rec_start_time, '%Y-%m-%dT%H:%M:%SZ')
    rec_end_time = datetime.strftime(add_rec_end_time, '%Y-%m-%dT%H:%M:%SZ')
    rec_hour_dif = 4
    get_epg(rec_start_time, rec_end_time, rec_hour_dif)
    print('2nd')
    time.sleep(1)
    print(datetime.now())
    action_log = extract_action_elastic(rec_start_time, rec_end_time, rec_hour_dif)
    login_log = extract_login_elastic(rec_start_time, rec_end_time, rec_hour_dif)
    # print('outttttttttt')
    # time.sleep(20)
    print(datetime.now())
    try:
        calculation_catchup(action_log)
    except KeyError:
        pass
    print(datetime.now())
    claculation_visit_duration(action_log)
    time.sleep(10)
    print(datetime.now())
    print('3rd')
    unique_visit(action_log, login_log)
    print('finish')
    # pause.until(add_rec_end_time + timedelta(days=1))

# use AP scheduler https://coderslegacy.com/python/apscheduler-tutorial-advanced-scheduler/
