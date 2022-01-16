import csv
import os
from datetime import datetime

import pandas as pd

from utils.basic import verified_input


def _calc_rank(b, s, v):
    if 'asin' in b:
        if not v['avg_rank']: v['avg_rank'] = sum(s) / len(s)
        if not v['min_rank']: v['min_rank'] = min(s)
        if not v['max_rank']: v['max_rank'] = max(s)


class SearchEngine:
    ST_DICT = {}

    BA_ATTRS = [
        'Department',
        'Search Term',
        'Search Frequency Rank',
        'Date',
        'Order',
        'Clicked ASIN',
        'Product Title',
        'Click Share',
        'Conversion Share'
    ]
    AVAILABLE_BY = [
        'search term',
        'search frequency rank',
        'search term asin',
        'search frequency rank asin',
        'asin detail',
        'search term detail',
        'search frequency rank detail',
    ]

    AVAILABLE_MODE = ['loose', 'exact']

    AVAILABLE_ENGINE = ['pandas', 'python']

    def __init__(self, engine=None):
        self.by = None
        self.params = None
        self.param_str = None
        self.mode = None
        if engine is None:
            self.engine = 'python'
        else:
            self.engine = engine
        self.st_data = None
        self.bind_df = None

    def _get_columns_index(self, kw, columns):
        for i, c in enumerate(columns):
            if kw.lower() == c.lower():
                return i
        return None

    def _get_time_prefix(self):
        return datetime.now().strftime('%Y%m%d-%H%M%S')

    def _get_abs_files_data(self, dirpath):
        files_dict = {'csv': None, 'hdf': None, 'json': None}
        item_list = os.listdir(dirpath)
        abs_file_list = []
        hdf_flag = False
        json_flag = False
        for item in item_list:
            item_abs_path = os.path.join(dirpath, item)
            fn, ext = os.path.splitext(item)
            if ext.lower() in ['.csv']:
                abs_file_list.append(item_abs_path)
            elif not hdf_flag and ext.lower() in ['.hdf']:
                files_dict['hdf'] = item_abs_path
                hdf_flag = True
            elif not json_flag and ext.lower() in ['.json']:
                files_dict['json'] = item_abs_path
                json_flag = True
        files_dict['csv'] = abs_file_list
        return files_dict

    def _load_st_data_basic_mode(self, engine, reader, fp, file, st_data, date):
        if engine in ['pandas']:
            df_el = pd.read_csv(fp, engine='c', dtype=str, na_filter=False, usecols=range(3))
            df_el.insert(loc=3, column='date', value=date)
            st_data = st_data.append(df_el)
        else:
            next(reader)
            i = 0
            for row in reader:
                i += 1
                st = row[1]
                asin_data_list = []
                date_data = {
                    'filepath': file,
                    'search frequency rank': i,
                    'asin_data': asin_data_list
                }
                if st not in st_data.keys():
                    site = row[0]
                    st_data[st] = {
                        'site': site,
                        'search_term': st,
                        'avg_rank': None,
                        'min_rank': None,
                        'max_rank': None,
                    }
                    st_data[st]['data'] = {
                        date: date_data,
                    }
                else:
                    st_data[st]['data'][date] = date_data
        return st_data

    def _load_st_data_asin_mode(self, engine, reader, fp, file, st_data, date):
        if engine in ['pandas']:
            df_el = pd.read_csv(fp, engine='c', dtype=str, na_filter=False, usecols=None)
            df_el.insert(loc=3, column='date', value=date)
            st_data = st_data.append(df_el)
        else:
            next(reader)
            i = 0
            for row in reader:
                i += 1
                st = row[1]
                asin_data_list = {}
                # this below is difference.
                for r in range(3):
                    asin = row[3 + r * 4]
                    asin_data_list[asin] = {
                        'order': r + 1,
                        'clicked asin': asin,
                        'product title': row[4 + r * 4],
                        'click share': row[5 + r * 4],
                        'conversion share': row[6 + r * 4],
                    }
                # ............................
                date_data = {
                    'filepath': file,
                    'search frequency rank': i,
                    'asin_data': asin_data_list
                }
                if st not in st_data.keys():
                    site = row[0]
                    st_data[st] = {
                        'site': site,
                        'search_term': st,
                        'avg_rank': None,
                        'min_rank': None,
                        'max_rank': None,
                    }
                    st_data[st]['data'] = {
                        date: date_data,
                    }
                else:
                    st_data[st]['data'][date] = date_data
        return st_data

    def _load_st_data_detail_mode(self, engine, reader, fp, file, st_data, date):
        return self._load_st_data_asin_mode(engine, reader, fp, file, st_data, date)

    def _load_st_data(self, by, engine, reader, fp, file, st_data, date):
        if by in ['search term', 'search frequency rank']:
            st_data = self._load_st_data_basic_mode(engine, reader, fp, file, st_data, date)
        else:
            st_data = self._load_st_data_asin_mode(engine, reader, fp, file, st_data, date)

        return self._after_load_st_data(by, st_data)

    def _after_load_st_data(self, by, st_data):
        # if self.engine == 'python':
        #     if by in ['search term', 'search frequency rank']:
        #         for st, val in st_data.items():
        #             val['min_rank'] = min([v['search frequency rank'] for v in val['data'].values()])
        return st_data

    def set_search_term_data(self, by, engine, abs_file_data, prev_data=None):
        def prepare_st_data(p_data):
            if p_data:
                return p_data
            else:
                if engine == 'python':
                    p_data = self.ST_DICT
                    if p_data is None:
                        p_data = {}
                    return p_data
                else:
                    return pd.DataFrame()

        hdf_file = abs_file_data['hdf']
        json_file = abs_file_data['json']

        if engine in ['pandas'] and hdf_file:
            print(f'loading hdf file: {hdf_file}')
            st_data = pd.read_hdf(hdf_file, mode='r')
        elif engine in ['python'] and json_file:
            st_data = None
        else:
            abs_file_list = abs_file_data['csv']
            lenfile = len(abs_file_list)
            st_data = prepare_st_data(prev_data)
            for index, file in enumerate(abs_file_list):
                print(f'processing {index + 1}/{lenfile}')
                with open(file, 'r', encoding='UTF-8') as fp:
                    reader = csv.reader(fp, delimiter=',')
                    date = next(reader)[4].replace('Viewing=[', '').replace(']', '')
                    st_data = self._load_st_data(by, engine, reader, fp, file, st_data, date)
            if engine in ['pandas']:
                # st_data.columns = [
                #     'department',
                #     'search term',
                #     'search frequency rank',
                #     'date',
                #     'clicked asin 1',
                #     'product title 1',
                #     'click share 1',
                #     'conversion share 1',
                #     'clicked asin 2',
                #     'product title 2',
                #     'click share 2',
                #     'conversion share 2',
                #     'clicked asin 3',
                #     'product title 3',
                #     'click share 3',
                #     'conversion share 3',
                # ]
                st_data.iloc[:, 2] = pd.to_numeric(st_data.iloc[:, 2].str.replace(',', ''))
                st_data.columns = [col.lower() for col in st_data.columns]
                # df_new['click share'] = pd.to_numeric(df_new['click share'].str.rstrip('%'),errors='coerce') / 100.0
                # df_new['conversion share'] = pd.to_numeric(df_new['conversion share'].str.rstrip('%'),errors='coerce') / 100.0
        self.st_data = st_data
        return st_data

    def _parse_search_list(self, by, searched_list):
        """
        转为可以转为df的字典
        :param searched_list:
        :return:
        """
        binded_dict_list = []
        if by in ['search term', 'search frequency rank']:
            for sd in searched_list:
                bind_dict = {
                    'site': sd['site'],
                    'search_term': sd['search_term'],
                    'min_rank': sd['min_rank']
                }
                date_value_dict = {}
                for date, value in sd['data'].items():
                    date_value_dict[date] = value['search frequency rank']
                # merge dict
                bind_dict = {**bind_dict, **date_value_dict}
                binded_dict_list.append(bind_dict)
            return binded_dict_list
        elif by in ['search term asin', 'search frequency rank asin']:
            df_st2 = pd.DataFrame()
            asin_len = 3
            for sd in searched_list:

                bind_list_dict = {
                    'site': [sd['site']] * asin_len,
                    'search_term': [sd['search_term']] * asin_len,
                    'min_rank': [sd['min_rank']] * asin_len,
                    'avg_rank': [sd['avg_rank']] * asin_len,
                    'max_rank': [sd['max_rank']] * asin_len,
                    'order': [i + 1 for i in range(asin_len)],
                }
                # date_value_dict = {}
                for date, value in sd['data'].items():
                    asin_list = list(value['asin_data'].keys())
                    # print(asin_list)
                    # asin_list.append([]*(asin_len-len(asin_list)))
                    asin_list_len = len(asin_list)
                    if asin_list_len < asin_len:
                        asin_list += [None] * (asin_len - asin_list_len)
                    bind_list_dict[date] = asin_list
                # for k,v in bind_list_dict.items():
                #     print(f'{k}: {len(v)}')
                df_temp = pd.DataFrame(bind_list_dict)
                df_st2 = df_st2.append(df_temp, ignore_index=True)
                # merge dict
                # bind_dict = {**bind_dict, **date_value_dict}
                # bind_dict_list.append(bind_dict)
            df_st2.loc[:, 'site':'max_rank'] = df_st2.loc[:, 'site':'max_rank'].ffill()
            return df_st2
        else:
            binded_dict_list = []
            for sd in searched_list:
                bind_dict = {
                    'site': sd['site'],
                    'search_term': sd['search_term'],
                    'min_rank': sd['min_rank'],
                    'avg_rank': sd['avg_rank'],
                    'max_rank': sd['max_rank'],
                }
                asin_dict = {}
                for date, date_data in sd['data'].items():
                    for asin, val in date_data['asin_data'].items():
                        for k, v in val.items():
                            asin_dict[k] = v
                            asin_dict['date'] = date
                        binded_dict = {**bind_dict, **asin_dict}
                        binded_dict_list.append(binded_dict)
            return binded_dict_list

    def search_dict_mode(self, by, param, mode, st_dict):
        bind_dict_list = []
        self.param_str = param
        condition = {
            'loose': lambda x, y, z: x in z,
            'exact': lambda x, y, z: x == y,
            'mean': lambda x, y, z: x in z,
        }
        assert isinstance(st_dict, dict) and len(st_dict) > 0, 'wrong data structure.'
        searched_list = []
        if by in ['search term', 'search term asin', 'search term detail']:
            if isinstance(param, str):
                param = [param]
            self.param_str = param[0] + '++'
            for par in param:
                for st, value in st_dict.items():
                    if condition[mode](par.lower(), st, st):
                        sfr = [v['search frequency rank'] for v in value['data'].values()]
                        _calc_rank(by, sfr, value)
                        searched_list.append(value)
            bind_dict_list = self._parse_search_list(by, searched_list)
        elif by in ['search frequency rank', 'search frequency rank asin', 'search frequency rank detail']:
            if isinstance(param, str) or isinstance(param, int):
                param = [param]
            self.param_str = str(param[0])
            param = param[0]
            if isinstance(param, str) and param.isalnum():
                param = int(param)
            elif isinstance(param, int):
                pass
            else:
                exit('param不是数字形式。')

            if mode == 'loose':
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    min_rank = min(sfr)
                    value['min_rank'] = min_rank
                    if min_rank <= param:
                        _calc_rank(by, sfr, value)
                        searched_list.append(value)
            elif mode == 'exact':
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    max_rank = max(sfr)
                    value['max_rank'] = max_rank
                    if max_rank <= param:
                        _calc_rank(by, sfr, value)
                        searched_list.append(value)
            elif mode == 'mean':
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    avg_rank = sum(sfr) / len(sfr)
                    value['avg_rank'] = avg_rank
                    if avg_rank <= param:
                        _calc_rank(by, sfr, value)
                        searched_list.append(value)
            bind_dict_list = self._parse_search_list(by, searched_list)
        elif by in ['asin detail']:
            if isinstance(param, str):
                param = [param]
            self.param_str = param[0] + '++'
            for st, value in st_dict.items():
                for date, date_data in value['data'].items():
                    asin_list = date_data['asin_data'].keys()
                    for k, asin_data in date_data['asin_data'].items():
                        for par in param:
                            if condition[mode](par, k, asin_list):
                                bind_dict = {
                                    'site': value['site'],
                                    'search_term': value['search_term'],
                                    'search frequency rank': date_data['search frequency rank'],
                                    'date': date,
                                }
                                bind_dict = {**bind_dict, **asin_data}
                                # searched_list.append(value)
                                bind_dict_list.append(bind_dict)
                                break
        bind_df = self.bind_list_to_df(bind_dict_list)
        self.bind_df = bind_df
        return bind_df

    def df_row_to_st_dict_basic(self, row, st_dict, append_dict=None):
        st = row[2]
        asin_data_list = []
        date = row[4]
        date_data = {
            'filepath': None,
            'search frequency rank': row[3],
            'asin_data': asin_data_list
        }
        if st not in st_dict.keys():
            site = row[1]
            st_dict[st] = {
                'site': site,
                'search_term': st,
                'avg_rank': None,
                'min_rank': None,
                'max_rank': None,
            }
            st_dict[st]['data'] = {
                date: date_data,
            }
            if append_dict:
                for k, v in append_dict.items():
                    st_dict[st][k] = row[v]
        else:
            st_dict[st]['data'][date] = date_data

    def df_row_to_st_dict_extended(self, row, st_dict, append_dict=None):
        st = row[2]
        sfr = row[3]
        date = row[4]
        order = row[5]
        asin_len = 3

        def _render_asin_data(row):
            asin_data_list = {}
            for i in range(1, asin_len + 1):
                asin_index = 1 + i * 4
                asin = row[asin_index]
                asin_info = {
                    'order': order,
                    'clicked asin': asin,
                    'product title': row[asin_index + 1],
                    'click share': row[asin_index + 2],
                    'conversion share': row[asin_index + 3],
                }
                asin_data_list[asin] = asin_info
            return asin_data_list

        if st not in st_dict.keys():
            site = row[1]
            st_dict[st] = {
                'site': site,
                'search_term': st,
                'avg_rank': None,
                'min_rank': None,
                'max_rank': None,
            }
            # this below is difference.
            # ............................
            date_data = {
                'filepath': None,
                'search frequency rank': sfr,
                'asin_data': _render_asin_data(row)
            }
            st_dict[st]['data'] = {
                date: date_data,
            }
            if append_dict:
                for k, v in append_dict.items():
                    st_dict[st][k] = row[v]
        else:
            date_data = {
                'filepath': None,
                'search frequency rank': sfr,
                'asin_data': _render_asin_data(row)
            }
            st_dict[st]['data'][date] = date_data
        return st_dict

    def st_df_to_sedt_list(self, by, st_df_filtered, st_dict=None, append_dict=None):
        """
        filtered st_df directly convert to searched list, which requires much less memory.
        :param by:
        :param st_df_filtered:
        :return:
        """
        if st_dict is None:
            st_dict = {}
        if by in ['search term', 'search frequency rank']:
            for row in st_df_filtered.itertuples():
                self.df_row_to_st_dict_basic(row, st_dict, append_dict)
        else:
            for row in st_df_filtered.itertuples():
                self.df_row_to_st_dict_extended(row, st_dict, append_dict)
        return st_dict

    def search_dataframe_mode(self, by, param, mode, st_df):
        searched_list, bind_dict_list = [], []
        condition = {
            'loose': lambda x, y, z: x in z,
            'exact': lambda x, y, z: x == y,
            'mean': lambda x, y, z: x in z,
        }
        if by in ['search term', 'search term asin', 'search term detail']:
            if isinstance(param, str):
                param = [param]
            self.param_str = param[0] + '++'
            st_df_grouped = st_df.groupby('search term')
            st_dict = {}
            for st, group in st_df_grouped:
                for par in param:
                    if condition[mode](par, st, st):
                        st_dict = self.st_df_to_sedt_list(by, group, st_dict)
            for st, value in st_dict.items():
                sfr = [v['search frequency rank'] for v in value['data'].values()]
                _calc_rank(by, sfr, value)
                searched_list.append(value)
            bind_dict_list = self._parse_search_list(by, searched_list)
        elif by in ['search frequency rank', 'search frequency rank asin', 'search frequency rank detail']:

            if isinstance(param, str) or isinstance(param, int):
                param = [param]
            self.param_str = str(param[0])
            param = param[0]
            if isinstance(param, str) and param.isalnum():
                param = int(param)
            elif isinstance(param, int):
                pass
            else:
                exit('param不是数字形式。')
            if mode == 'loose':
                st_df['min_rank'] = st_df.groupby("search term")['search frequency rank'].transform('min')
                st_df_filtered = st_df[st_df['min_rank'] <= param]
                st_dict = self.st_df_to_sedt_list(by, st_df_filtered, append_dict={'min_rank': -1})
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    _calc_rank(by, sfr, value)
                    searched_list.append(value)
                searched_list = st_dict.values()
            elif mode == 'exact':
                st_df_filtered = st_df[st_df['search frequency rank'] <= param]
                st_dict = self.st_df_to_sedt_list(by, st_df_filtered)
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    _calc_rank(by, sfr, value)
                    searched_list.append(value)
                searched_list = st_dict.values()
            elif mode == 'mean':
                st_df['min_rank'] = st_df.groupby("search term")['search frequency rank'].transform('min')
                st_df_filtered = st_df[st_df['min_rank'] <= param]
                st_dict = self.st_df_to_sedt_list(by, st_df_filtered)
                for st, value in st_dict.items():
                    sfr = [v['search frequency rank'] for v in value['data'].values()]
                    _calc_rank(by, sfr, value)
                    searched_list.append(value)
                searched_list = st_dict.values()
            bind_dict_list = self._parse_search_list(by, searched_list)
        elif by in ['asin detail']:
            if isinstance(param, str):
                param = [param]
            self.param_str = param[0] + '++'
            asin_len = 3
            filtered_list = []
            for row in st_df.itertuples():
                asin_dict = {}
                asin_list = []
                for i in range(1, asin_len + 1):
                    asin_index = 1 + i * 4
                    asin = row[asin_index]
                    asin_list.append(asin)
                    asin_dict[asin] = row[1:5] + (i,) + row[asin_index:asin_index + 4]

                for asin in asin_list:
                    for par in param:
                        if condition[mode](par, asin, asin_list):
                            temp_row = asin_dict[asin]
                            filtered_list.append(temp_row)
                            break
            bind_dict_list = pd.DataFrame(filtered_list, columns=[c.lower() for c in self.BA_ATTRS])
        bind_df = self.bind_list_to_df(bind_dict_list)
        self.bind_df = bind_df
        return bind_df

    def bind_list_to_df(self, bind_dict_list):
        if isinstance(bind_dict_list, pd.DataFrame):
            df = bind_dict_list
        else:
            df = pd.DataFrame(bind_dict_list)
        return df

    def search(self, by, param, mode, engine, st_data=None):
        print('start searching.')
        if engine in ['python']:
            bind_df = self.search_dict_mode(by, param, mode, st_data)
        else:
            bind_df = self.search_dataframe_mode(by, param, mode, st_data)
        print(bind_df)
        return bind_df

    def save_search(self, bind_df, by, param_str, mode, engine, dirpath, save_dirpath):
        if save_dirpath is None:
            save_dirpath = dirpath
        nowtime = datetime.now().strftime('%Y%m%d-%H%M%S')
        save_file_path = os.path.join(save_dirpath, f'result-{mode}-{by}-{param_str}-{engine}-{nowtime}.xlsx')
        bind_df.to_excel(save_file_path)
        print(f'保存文件：{save_file_path}')

    def operator_mechine(self, by, param, mode, engine, dirpath, save_dirpath=None):
        abs_file_data = self._get_abs_files_data(dirpath)
        st_data = self.set_search_term_data(by, engine, abs_file_data)
        bind_df = self.search(by, param, mode, engine, st_data)
        st_data = 0
        param_str = self.param_str
        self.save_search(bind_df, by, param_str, mode, engine, dirpath, save_dirpath)


def search(by, param, mode, engine, dirpath, save_dirpath=None):
    st_object = SearchEngine()
    st_object.operator_mechine(by, param, mode, engine, dirpath, save_dirpath)
    st_object = 0


def run2():
    def ver_param():
        def ver_param_path():
            excel_path = input('输入参数表格路径：').strip('\"').strip()
            return excel_path if os.path.exists(excel_path) else False

        excel_path = verified_input(lambda: ver_param_path())
        df = pd.read_excel(excel_path)
        df.columns = [c.lower() for c in df.columns]
        by = df['by'][0]
        print(f'by: {by}')
        param = df['param'].to_list()
        print(f'param: {str(param)}')
        mode = df['mode'][0]
        print(f'mode: {mode}')

        if by in SearchEngine.AVAILABLE_BY and mode in SearchEngine.AVAILABLE_MODE:
            return by, param, mode, excel_path
        else:
            return False

    by, param, mode, excel_path = verified_input(lambda: ver_param(), "表格不正确", '错误的参数表格。')

    def ver_engine():
        engine_id = input('选择处理引擎编号 (1.pandas, 2.python，默认为pandas, python可能更快但需要更多运存): ') or '1'
        if not engine_id.isalnum():
            return False
        if engine_id == '1':
            return 'pandas'
        elif engine_id == '2':
            return 'python'
        else:
            return False

    engine = verified_input(lambda: ver_engine(), "错误设置引擎, 请重试", "错误输入太多，程序退出。")

    def ver_save_dirpath():
        save_dirpath = input('结果保存位置 (默认与参数表格同文件夹)：').strip('\"').strip() or os.path.dirname(excel_path)
        return save_dirpath if os.path.exists(save_dirpath) else False

    save_dirpath = verified_input(lambda: ver_save_dirpath())

    dirpath = input('输入品牌分析文件夹: ').strip('\"').strip() or r'D:\HollyWork\调研\品牌分析\UK ABA'
    search(by, param, mode, engine, dirpath, save_dirpath)
