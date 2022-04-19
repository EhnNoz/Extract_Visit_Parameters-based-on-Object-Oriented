from datetime import datetime, timedelta
from itertools import chain
from threading import Thread
import pandas as pd
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine

db_engine = create_engine('postgresql://postgres:nrz1371@localhost/samak', pool_size=20, max_overflow=100)
db_connected = db_engine.connect()


class ExtractData:
    def __init__(self, start_time, end_time, engine_elastic, table_name, field_name, scroll, size):
        self.start_time = start_time
        self.end_time = end_time
        self.engine_elastic = engine_elastic
        self.table_name = table_name
        self.field_name = field_name
        self.scroll_value = scroll
        self.size_value = size

    def get_data(self):
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
            read_file = read_file[
                ['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
                 'content_name', 'channel_name', 'content_type_id', 'action_id']]
            read_file = read_file.astype(str)
            summ = scroll_size + summ
            print(summ)
        return read_file

    @staticmethod
    def extract_epg(data_frame):
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


class CompareData:
    def __init__(self, log_data_frame, epg_lst):
        self.log_lst = log_data_frame.to_dict(orient='records')
        self.epg_lst = epg_lst

    def log_thread(self, sys_id, service_id, action_id, con_type_id):
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
        cr_log_lst[-1].update({'end_time': cr_log_lst[-1].get('time_stamp') + timedelta(minutes=30)})
        last_dct = cr_log_lst[-1]
        cr_log_lst.pop(-1)
        cr_log_lst.append(last_dct)
        # Calc differential of start and end time log
        cr_log_lst = list(map(lambda x: dict(x, diff=x.get('end_time') - x.get('time_stamp')), cr_log_lst))
        cr_log_lst = list(
            map(lambda x: dict(x, end_time=x.get('time_stamp') + timedelta(minutes=30)) if x.get(
                'diff').seconds > 1800 else dict(x, end_time=x.get('end_time')), cr_log_lst))

        sys_id_cr_log_lst = list(filter(lambda x: x.get('sys_id') == sys_id, cr_log_lst))
        act_id_cr_log_lst = list(filter(lambda x: x.get('action_id') == action_id, sys_id_cr_log_lst))
        con_type_id_cr_log_lst = list(filter(lambda x: x.get('content_type_id') == con_type_id, act_id_cr_log_lst))
        service_id_cr_log_lst = list(filter(lambda x: x.get('service_id') == service_id, con_type_id_cr_log_lst))
        return service_id_cr_log_lst

    def process_log(self, log):
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
    def calc_sessions(session_list, final_list=[], fn_lst=[]):
        for session in session_list:
            session_data_output = data_output[data_output['session_id'] == session]
            call_class = CompareData(session_data_output, cr_epg_lst)
            try:
                log_act_filter = call_class.log_thread('09', '1', '1', '1')
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
                flt_lst_id[0].update({'duration_sum': sum_result})
                flt_lst_id[0].update({'visit_sum': 1})
                unique_lst.append(flt_lst_id[0])
            # print(sum_lst)
            fn_lst.append(unique_lst)

        # print(session)
        # print(final_list)

        fn_lst = list(chain.from_iterable(fn_lst))
        if fn_lst:
            fn_df = pd.DataFrame(fn_lst)
            fn_df.to_sql('Re_DurVis', db_connected, if_exists='append', index=False)

        print(fn_lst)
        return fn_lst


call_extract_data = ExtractData('2022-04-18T00:00:01Z', '2022-04-18T23:59:59Z',
                                "http://norozzadeh:Kibana@110$%^@192.168.143.34:9200", 'live-action', '@timestamp',
                                '1m', 10000)
df = pd.read_excel('epg_pro1.xlsx', index_col=False)
cr_epg_lst = ExtractData.extract_epg(df)

data_output = call_extract_data.get_data()
session_set = set(data_output['session_id'])
final = CompareData.calc_sessions(session_set, [], [])
session_lst = list(session_set)
chunks = [session_lst[s_id:s_id + 50] for s_id in range(0, len(session_lst), 50)]
for chunk in chunks:
    t1 = Thread(target=CompareData.calc_sessions, args=[chunk, [], []])
    t1.start()

#
# final_list =[]
# for session in session_set:
#     if session =='f670be01-221c-4ebb-8eb2-72ce837e7cb8':
#         session_data_output = data_output[data_output['session_id'] == session]
#         call_class = CompareData(session_data_output, cr_epg_lst)
#         try:
#             log_act_filter = call_class.log_thread('09', '1', '1', '1')
#         except IndexError:
#             continue
#         mnr_process_log_output = list(map(lambda x: call_class.process_log(x), log_act_filter))
#         mnr_process_log_output = list(filter(None, mnr_process_log_output))
#         mnr_process_log_output = list(chain.from_iterable(mnr_process_log_output))
#         if mnr_process_log_output:
#             final_list.append(mnr_process_log_output)
#
# f_lst = []
# for init_item in final_list:
#     item = init_item.copy()
#     print(len(item))
#     result = [e.get('id') for e in item]
#     result = set(result)
#     print(f'result:{result}')
#     sum_lst = []
#     unique_lst = []
#     for id_num in result:
#         # print(f'result:{result}')
#         flt_lst_id = list(filter(lambda x: x.get('id') == id_num, item))
#         print(f'flt_lst_id:{flt_lst_id}')
#         sum_result = [d.get('duration_sum') for d in flt_lst_id]
#         print(sum_result)
#         x_p = sum(sum_result)
#         print(x_p)
#     #     sum_lst.append(sum_result)
#         flt_lst_id[0].update({'duration_sum':x_p})
#     #     flt_lst_id[0].update({'visit_sum': 1})
#         unique_lst.append(flt_lst_id[0])
#     # print(sum_lst)
#     f_lst.append(unique_lst)
#     print(final_list)
#
#
# 95406a90-053d-46f1-b7ed-c202c90599f7  03:47