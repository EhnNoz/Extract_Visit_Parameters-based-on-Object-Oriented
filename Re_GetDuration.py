"""
This code is written to extract the:
 1) Visit
 2) Time Duration
 based on client side logs

"""
import json
from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
import pandas as pd
import pika
from elasticsearch import Elasticsearch


# from sqlalchemy import create_engine

# db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak', pool_size=20, max_overflow=100)
# db_connected = db_engine.connect()


class ExtractData:
    """
    This class is writen to:
     1) extract data of Elastic Search database (get_data)
     2) customize epg field (extract_epg)
    """

    def __init__(self, start_time, end_time, hour_dif, minutes_dif, engine_elastic, table_name,
                 field_name, scroll, size):
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        start_time = start_time - timedelta(hours=hour_dif, minutes=minutes_dif)
        start_time = datetime.strftime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
        end_time = end_time - timedelta(hours=hour_dif, minutes=minutes_dif)
        end_time = datetime.strftime(end_time, '%Y-%m-%dT%H:%M:%SZ')
        self.start_time = start_time
        self.end_time = end_time
        self.hour_dif = hour_dif
        self.minutes_dif = minutes_dif
        self.engine_elastic = engine_elastic
        self.table_name = table_name
        self.field_name = field_name
        self.scroll_value = scroll
        self.size_value = size

    def get_data(self):
        """
        this func is written to extract data based on time stamp
        output func is dataframe
        """
        es = Elasticsearch(hosts=self.engine_elastic)
        body = {'query': {'bool': {'must': [{'match_all': {}},
                                            {'range': {self.field_name: {'gte': '{}'.format(self.start_time),
                                                                         'lt': '{}'.format(self.end_time)}}}]}}}

        def process_hits(hits):
            sec_df = pd.DataFrame()
            for item in hits:
                init_df = pd.DataFrame(item['_source'], columns=item['_source'].keys(), index=[0])
                sec_df = sec_df.append(init_df, ignore_index=True)
            return sec_df

        data = es.search(
            index=self.table_name,
            scroll=self.scroll_value,
            size=self.size_value,
            body=body
        )

        sid = data['_scroll_id']
        summ = 0
        scroll_size = len(data['hits']['hits'])
        read_file = pd.DataFrame()
        while scroll_size > 0:
            process_output = process_hits(data['hits']['hits'])

            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

            read_file = read_file.append(process_output, ignore_index=True)
            # print(process_output.columns)
            # read_file = read_file[
            #     ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
            #      'content_name', 'channel_name', 'content_type_id', 'action_id']]
            read_file = read_file.astype(str)
            summ = scroll_size + summ
            # print(summ)
        read_file['time_stamp'] = read_file['time_stamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        read_file['time_stamp'] = read_file['time_stamp'].apply(
            lambda x: x + timedelta(hours=self.hour_dif, minutes=self.minutes_dif))
        read_file['time_stamp'] = read_file['time_stamp'].apply(lambda x: datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
        return read_file

    @staticmethod
    def extract_epg(data_frame):
        """
        this func is written to customize epg field and 'Time_Play' & 'EP' convert to time type
        :param data_frame:
        :return: list of EPG details
        """
        data_frame = data_frame.drop(
            columns=['ID_Program', 'end_hour', 'end_year', 'start_hour', 'start_year', 'ID_Kind', 'channel_code'])
        epg_lst = data_frame.to_dict(orient='records')
        epg_lst = list(map(lambda x: dict(x, duration_sum=0), epg_lst))
        epg_lst = list(map(lambda x: dict(x, visit_sum=0), epg_lst))
        for new_id, dct in enumerate(epg_lst, start=1):
            dct['id'] = new_id
        epg_lst = list(
            map(lambda x: dict(x, Length=(
                    datetime.strptime(x.get('Length'), "%H:%M:%S") - datetime(1900, 1, 1)).total_seconds()),
                epg_lst))
        cr_epg_lst = list(
            map(lambda x: dict(x, Time_Play=datetime.strptime(x.get('Time_Play'), '%m/%d/%Y %I:%M:%S %p')),
                epg_lst))
        cr_epg_lst = list(map(lambda x: dict(x, EP=datetime.strptime(x.get('EP'), '%m/%d/%Y %I:%M:%S %p')), cr_epg_lst))
        return cr_epg_lst

    @staticmethod
    def convert_time(desire_time):
        """
        :param desire_time: eg: 2022-04-18T23:00:01Z
        :return: str(time) eg: 05/18/2022
        """
        desire_time = datetime.strptime(desire_time, '%Y-%m-%dT%H:%M:%SZ')
        desire_time = datetime.strftime(desire_time, '%m/%d/%Y')
        return desire_time


class CompareData:
    """
    This class is written to:
    1) filter log based (log_thread)
    2) calc duration and visit each log (process_log)
    3) calc sum duration and visit each session and compliance with EPG
        and send to rabbitmq (calc_sessions)
    """

    def __init__(self, log_data_frame, epg_lst):
        self.log_lst = log_data_frame.to_dict(orient='records')
        # self.log_lst = map(lambda x:  dict(x, channel_name='کچاپ') if x.get('null') else dict(x, channel_name=x.get('channel_name')), log_lst)
        self.epg_lst = epg_lst

    def log_thread(self, sys_id, service_id, action_id, con_type_id, close_session):
        """
        This def is writen to filter log based on follow:
        :param sys_id:
        :param service_id:
        :param action_id:
        :param con_type_id:
        :return: list of dict of filtered log
        """
        sorted_log_lst = sorted(self.log_lst, key=lambda d: d['time_stamp'])
        time_stamp_list = list(map(lambda x: x.get('time_stamp'), sorted_log_lst))
        time_stamp_list.pop(0)
        time_stamp_list.append(time_stamp_list[-1])
        cr_log_lst = list(map(lambda x: dict(x[0], end_time=x[1]), zip(sorted_log_lst, time_stamp_list)))
        cr_log_lst = list(
            map(lambda x: dict(x, end_time=datetime.strptime(x.get('end_time'), '%Y-%m-%dT%H:%M:%SZ')), cr_log_lst))
        cr_log_lst = list(
            map(lambda x: dict(x, time_stamp=datetime.strptime(x.get('time_stamp'), '%Y-%m-%dT%H:%M:%SZ')), cr_log_lst))
        # Add end_time of last log
        cr_log_lst[-1].update({'end_time': cr_log_lst[-1].get('time_stamp') + timedelta(minutes=close_session)})
        last_dct = cr_log_lst[-1]
        cr_log_lst.pop(-1)
        cr_log_lst.append(last_dct)
        # Calc differential of start and end time log
        cr_log_lst = list(map(lambda x: dict(x, diff=x.get('end_time') - x.get('time_stamp')), cr_log_lst))
        cr_log_lst = list(
            map(lambda x: dict(x, end_time=x.get('time_stamp') + timedelta(minutes=close_session)) if x.get(
                'diff').seconds > close_session * 60 else dict(x, end_time=x.get('end_time')), cr_log_lst))

        sys_id_cr_log_lst = list(filter(lambda x: x.get('sys_id') == sys_id, cr_log_lst))
        act_id_cr_log_lst = list(filter(lambda x: x.get('action_id') == action_id, sys_id_cr_log_lst))
        con_type_id_cr_log_lst = list(filter(lambda x: x.get('content_type_id') == con_type_id, act_id_cr_log_lst))
        service_id_cr_log_lst = list(filter(lambda x: x.get('service_id') == service_id, con_type_id_cr_log_lst))
        return service_id_cr_log_lst

    def process_log(self, log):
        """
        This def is writen to calc duration and visit each log
        """

        cr_epg_lst = self.epg_lst
        start_time = log.get('time_stamp')
        # print(f'start:{start_time}')
        end_time = log.get('end_time')
        # print(f'end:{end_time}')
        log_channel_name = log.get('channel_name')
        # print(f'log_channel_name:{log_channel_name}')
        flt_channel = list(filter(lambda x: x.get('channel_name') == log_channel_name, cr_epg_lst))
        # print(f'len_flt_channel:{len(flt_channel)}')
        btw_pro_lst = list(filter(lambda x: x.get('Time_Play') >= start_time and x.get('EP') <= end_time, flt_channel))
        # print(f'btw_pro_lst:{btw_pro_lst}')
        if btw_pro_lst:
            btw_pro_lst = list(
                map(lambda x: dict(x, duration_sum=x.get('duration_sum') + x.get('Length')), btw_pro_lst))
            # print(f'btw_pro_lst2:{btw_pro_lst}')
        st_end_pro_lst = list(
            filter(lambda x: x.get('Time_Play') < (start_time and end_time) < x.get('EP'), flt_channel))
        # print(f'st_end_pro_lst:{st_end_pro_lst}')
        if st_end_pro_lst:
            st_end_pro_lst = list(
                map(lambda x: dict(x, duration_sum=x.get('duration_sum') + ((end_time - start_time).total_seconds())),
                    st_end_pro_lst))
            btw_pro_lst.insert(0, st_end_pro_lst[0])
            # print(f'btw_pro_lst3:{btw_pro_lst}')
        else:
            # print(f'else:{btw_pro_lst}')
            st_pro_lst = list(filter(lambda x: x.get('Time_Play') < start_time < x.get('EP'), flt_channel))
            if st_pro_lst:
                st_pro_lst = list(
                    map(lambda x: dict(x,
                                       duration_sum=x.get('duration_sum') + ((end_time - start_time).total_seconds())),
                        st_pro_lst))
                btw_pro_lst.insert(0, st_pro_lst[0])
            end_pro_lst = list(filter(lambda x: x.get('Time_Play') < end_time < x.get('EP'), flt_channel))
            if end_pro_lst:
                end_pro_lst = list(
                    map(lambda x: dict(x, duration_sum=x.get('duration_sum') + (
                            (end_time - x.get('Time_Play')) - datetime(1900, 1, 1)).total_seconds()),
                        end_pro_lst))
                btw_pro_lst.append(end_pro_lst)

        # sum_pro_dur_lst = list(chain.from_iterable(btw_pro_lst))
        sum_pro_dur_lst = btw_pro_lst

        return sum_pro_dur_lst

    @staticmethod
    def calc_sessions(iframe, session_list, output_logs, customize_epg, exchange_name, routing_key_name, final_list=[],
                      fn_lst=[]):
        """
        This def is writen to calc sum duration and visit each session and compliance with EPG
        and send to rabbitmq

        """
        for session in session_list:
            if len(session) == iframe:
                continue
            session_data_output = output_logs[output_logs['session_id'] == session]
            call_class = CompareData(session_data_output, customize_epg)
            try:
                log_act_filter = call_class.log_thread('09', '1', '1', '1', 15)
            except IndexError:
                continue
            mnr_process_log_output = list(map(lambda x: call_class.process_log(x), log_act_filter))
            mnr_process_log_output = list(filter(None, mnr_process_log_output))
            mnr_process_log_output = list(chain.from_iterable(mnr_process_log_output))
            if mnr_process_log_output:
                final_list.append(mnr_process_log_output)

        for item in final_list:
            result = [e.get('id') for e in item]
            result = set(result)
            # print(result)
            sum_lst = []
            unique_lst = []
            for id_num in result:
                flt_lst_id = list(filter(lambda x: x.get('id') == id_num, item))
                sum_result = sum(d.get('duration_sum', 0) for d in flt_lst_id)
                sum_lst.append(sum_result)
                flt_lst_id[0].update({'duration_sum': sum_result / 60})
                flt_lst_id[0].update({'visit_sum': 1})
                unique_lst.append(flt_lst_id[0])
            # print(sum_lst)
            fn_lst.append(unique_lst)

        # print(session)
        # print(final_list)

        fn_lst = list(chain.from_iterable(fn_lst))
        if fn_lst:
            fn_df = pd.DataFrame(fn_lst)
            fn_df.rename(
                columns={'channel_name': 'channel', 'visit_sum': 'visit', 'duration_sum': 'dur', 'id': 'u_visit'},
                inplace=True)
            fn_df = fn_df.astype(str)
            fn_dct = fn_df.to_dict('records')
            # if iframe == 36:
            #     fn_dct = list(map(lambda x: dict(x, Dec_Summary=str(iframe)), fn_dct))

            for q in fn_dct:
                msg = q
                try:
                    call_send_data.send_to_rabbit(msg, exchange_name, routing_key_name)

                except:
                    call_send_data = SendData('192.168.143.39', 'admin', 'R@bbitMQ1!')
                    call_send_data.send_to_rabbit(msg, exchange_name, routing_key_name)

        return fn_lst


class SendData:
    """
    This class is written to Send Data
    """

    def __init__(self, host, username, password):
        credentials = pika.PlainCredentials(username=username, password=password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=5672, credentials=credentials))
        self.connection = connection
        self.channel = self.connection.channel()

    def send_to_rabbit(self, msg, exchange_name, routing_key_name):
        """
        :param msg: log of EPG with Duration and visit
        :param exchange_name: exchange of Rabbitmq
        :param routing_key_name: routing_key of Rabbitmq

        """
        self.channel.basic_publish(exchange=exchange_name, routing_key=routing_key_name,
                                   properties=pika.BasicProperties(
                                       content_type='application/json'),
                                   body=json.dumps(msg, ensure_ascii=False))

    def close_connection_rabbit(self):
        self.connection.close()

# # connect to rabbitmq
# call_send_data = SendData('192.168.143.39', 'admin', 'R@bbitMQ1!')
#
# # Registration of specifications of start and end time of logs and elastic search
# call_extract_data = ExtractData('2022-04-18T23:00:01Z', '2022-04-18T23:59:59Z', 4, 22,
#                                 "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', 'time_stamp',
#                                 '1m', 10000)
# # extract logs
# data_output = call_extract_data.get_data()
# data_output = data_output[
#     ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
#      'content_name', 'channel_name', 'content_type_id', 'action_id']]
#
# # GET dataframe of EPG and customize field
# df = pd.read_excel('epg_pro1.xlsx', index_col=False)
# cr_epg_lst = ExtractData.extract_epg(df)
#
# # extract set of session id
# session_set = set(data_output['session_id'])
#
# # calc sum duration and visit of sessions
# # final = CompareData.calc_sessions(session_set, [], [])
#
# # convert logs data to multi chunks
# session_lst = list(session_set)
# chunks = [session_lst[s_id:s_id + 50] for s_id in range(0, len(session_lst), 50)]
# # start of calculations
# for chunk in chunks:
#     t1 = Thread(target=CompareData.calc_sessions, args=[chunk, data_output, cr_epg_lst, [], []])
#     t1.start()
# # call_send_data.close_connection_rabbit()
