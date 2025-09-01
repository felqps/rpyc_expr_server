#!/home/shuser/anaconda3/bin/python

import sys

import numpy
import pandas as pd
import time
import datetime
import pickle
import os
from dateutil.parser import parse as dtparse
from factor_return import FactorReturn
from calc_performance import CalcPerformance
from create_report import CreateReport
from default_params import set_default_params
from common_basic import *
from shared_cmn import print_df
from funcs_fld import re_index


class AlphaPerformance:
    #@timeit_m
    def __init__(self, params=None, simple_report=True, input_data=None, auto_calc = True):
        self._factors_data = None
        self._factor_return = None
        self._performance_summary = None
        self._performance_data = None
        self._auto_calc = auto_calc
        #self._simple_report = simple_report
        
        if params != None:
            self._params = params
        else:
            self._params = set_default_params(simple_report) #xxx
            
        self._params['alpha_name']=self._params['alpha_name'].replace('.','_')
        self._input_data=input_data

        if auto_calc:
            self.get_data()
            self.fix_data()
            self.calculate()
            self.create_report()


    def factor_return(self):
        if self._factor_return is None:
            self._factor_return = FactorReturn(self._params, self._input_data)
        return self._factor_return

    def GetPerformanceSummary(self):
        return self._performance_summary

    def GetPerformanceData(self): 
        if not self._auto_calc:
            self.get_data()
            self.fix_data()
            self.calculate()
            self.create_report()            
        return self._performance_data 

    def input_data(self, name):
        return self._input_data[name]
        
    #@timeit_m
    def read_data(self,file_name):
        # if not os.path.exists(file_name):
        #     file_name = file_name.replace('prod1w', 'W')
        #     if not os.path.exists(file_name):
        #         print(f"ERROR: can not find {file_name}", file=sys.stderr)
        #         return None
                
        data = smart_load(file_name, debug=True, title=f"alpha_performance.read_data")
        return data

    @timeit_m_real
    def get_data(self):
        if self._input_data is None:
            datain = {}
            params = self._params

            dirname=params['data_dir']

            if params['factor_data'] is not None: #xxx
                #print(f"DEBUG_2344", print_df(params['factor_data'].tail()))
                datain['factor_data'] = params['factor_data']
            else:
                if params['factor_file'] != "":
                    datain['factor_data'] = self.read_data(params['factor_dir'] + params['factor_file'])

            if 'factor_data' in datain and isinstance(datain['factor_data'],dict):
                params.update(datain['factor_data']['params'])
                datain['factor_data']=datain['factor_data']['factor_data']

            if params['return_data'] is not None: #xxx
                datain['return_data'] =  params['return_data'] 
            else:   
                datain['return_data'] = self.read_data(dirname + params['return_file_name'])

            if params['min_ipo_age']>0:
                datain['ipo_age'] = self.read_data(dirname + params['ipo_age_file_name'])
            if params['exclude_st'] == True:
                datain['st_flag'] = self.read_data(dirname + params['st_flag_file_name'])

            datain['index_return_data'] = None
            datain['beta_HS300'] = None
            datain['beta_ZZ500'] = None
            datain['sector_flag'] = None
            datain['sector_weight'] = None
 

            if 'universe_data' not in params:
                params['universe_data']=[]
                if (params['universe'] is not None) and (params['universe'] != []):
                    for i in range(len(params['universe'])):
                        if params['universe'][i].lower() != 'all':
                            #params['universe_data'].append('univ_'+params['universe'][i])
                
                            datain['univ_'+params['universe'][i]] = self.read_data(params['universe_file_name'][i])
                            params['universe_data'].append(datain['univ_'+params['universe'][i]])
                            #print(i, datain['univ_'+params['universe'][i]].shape)
                        else:
                            params['universe'][i] = 'all'
                            params['universe_data'].append(1)
            else:
                for i in range(len(params['universe'])):
                    if params['universe'][i].lower() != 'all':
                        datain['univ_'+params['universe'][i]] = params['universe_data'][i]
                        #params['universe_data'].append(datain['univ_'+params['universe'][i]])
                        #print(i, datain['univ_'+params['universe'][i]].shape)
                    else:
                        pass
                        #params['universe'][i] = 'all'
                        #params['universe_data'].append(1)
            

            # for x in params['universe_data']:
            #     print("INFO: universe_data", x, x.shape if isinstance(x, pd.DataFrame) else x)
            # print(len(params['universe_data']))

            # XXX
            if params['calcQT_HS300'] == True or \
                params['calcQT_ZZ500'] == True or \
                params['calcQT_beta_HS300'] == True or \
                params['calcQT_beta_ZZ500'] == True:
                if 'index_return_data' in params:
                    datain['index_return_data'] = params['index_return_data']
                else:
                    datain['index_return_data'] = self.read_data(params['index_return_file_name'])
                    params['index_return_data'] = datain['index_return_data']

            if params['calcQT_beta_HS300'] == True:
                if 'beta_HS300' in params:
                    datain['beta_HS300'] = params['beta_HS300']
                else:
                    datain['beta_HS300'] = self.read_data(dirname + params['beta_HS300_file_name'])
                    params['beta_HS300'] = datain['beta_HS300']

            if params['calcQT_beta_ZZ500'] == True:
                if 'beta_ZZ500' in params:
                    datain['beta_ZZ500'] = params['beta_ZZ500']
                else:
                    datain['beta_ZZ500'] = self.read_data(dirname + params['beta_ZZ500_file_name'])
                    params['beta_ZZ500'] = datain['beta_ZZ500']

            if params['calcQT_sector_demean'] == True:
                if 'sector_flag' in params:
                    datain['sector_flag'] = params['sector_flag']
                    datain['sector_weight'] = params['sector_weight']
                else:
                    sector_data = self.read_data(dirname + params['sector_file_name'])
                    if sector_data is None:
                        params['calcQT_sector_demean'] = False
                        print(f"{RED}ERROR: cannot find sector_data {dirname + params['sector_file_name']}{NC}")
                    else:
                        sector_data.fillna(0.0, inplace=True) #xxxb
                        datain['sector_flag'] = sector_data.astype('i')
                        datain['sector_weight'] = 10*(sector_data-datain['sector_flag'])
                        params['sector_flag'] = datain['sector_flag']
                        params['sector_weight'] = datain['sector_weight']

            self._input_data = datain
            
    @timeit_m_real
    def fix_data(self):
        funcn = "AlphaPerformance.fix_data()"
        #print(">>>>>>>>>>>DEBUB: fix_data", self._input_data.keys())
        #print(f"DEBUG_8423: start_date= {self._params['start_date']}, end_date= {self._params['end_date']}")
        for dta in self._input_data.keys():
            if self._input_data[dta] is not None:
                fld_index_drop_tm(self._input_data[dta])
                if self._params['start_date'] is not None:
                    self._input_data[dta] = self._input_data[dta][
                        self._input_data[dta].index >= (dtparse(self._params['start_date']) if type(self._params['start_date']) == type(str()) else self._params['start_date'])
                        ]
                if self._params['end_date'] is not None:
                    self._input_data[dta] = self._input_data[dta][
                        self._input_data[dta].index <= (dtparse(self._params['end_date']) if type(self._params['end_date']) == type(str()) else self._params['end_date'])
                        ]

        for nm in ['factor_data', 'return_data']:
            if nm not in self._input_data:
                continue
            df = self._input_data[nm]
            #print_df(df, title=funcn)
            if type(df) != type(pd.DataFrame()):
                print(f"ERROR: factor_data for nm= {nm} not a DataFrame, skipping...")
                continue

            if df is not None and not df.index.is_unique:
                print(f"DEBUG: fix_data chk duplicate failed for {nm}")
                #print(df.tail())    
                print_df(df.index[df.index.duplicated()], title=f'fix_data duplicated')
   

        if 'factor_data' in self._input_data and 'return_data' in self._input_data:
            re_index(self._input_data['factor_data'], self._input_data['return_data'])
            re_index(self._input_data['return_data'], self._input_data['factor_data'])


        for dta in self._input_data.keys():
            if dta != 'factor_data' and dta != 'return_data' and self._input_data[dta] is not None:
                if dta == 'index_return_data':
                    # pass
                    # XXX 
                    self._input_data[dta] = self._input_data[dta].reindex(index=self._input_data['return_data'].index)

                else:
                    self._input_data[dta] = self._input_data[dta].reindex(index=self._input_data['return_data'].index, columns=self._input_data['return_data'].columns)

        if self._params['calcQT_ma'] == True:
            self._input_data['factor_ma_data'] = self._input_data['factor_data'].rolling(self._params['ma_days']).mean()
            self._input_data['factor_ma_data'].where(~numpy.isnan(self._input_data['return_data']), other=numpy.nan, inplace=True)
        else:
            self._input_data['factor_ma_data'] = None

        self._input_data['factor_data'].where(~numpy.isnan(self._input_data['return_data']), other=numpy.nan, inplace=True)
        self._input_data['return_data'].fillna(0, inplace=True)

        if self._params['min_ipo_age'] > 0:
            self._input_data['factor_data'].where(self._input_data['ipo_age']>self._params['min_ipo_age'] , other=numpy.nan,inplace=True)
        if self._params['exclude_st'] == True:
            self._input_data['factor_data'].where(self._input_data['st_flag'] < 0.5,other=numpy.nan, inplace=True)

        if self._params['start_date'] is  None:
            self._params['start_date']=self._input_data['return_data'].index[0]
        if self._params['end_date'] is None:
            self._params['end_date'] = self._input_data['return_data'].index[-1]

        if self._params['os_end_date'] is None:
            self._params['os_end_date']=self._params['end_date']
        if self._params['os_start_date'] is None:
            self._params['os_start_date']=self._params['os_end_date']
        if self._params['is_start_date'] is None:
            self._params['is_start_date']=self._params['start_date']
        if self._params['is_end_date'] is None:
            self._params['is_end_date']=self._params['os_start_date']
        self._os_days=(self._params['os_end_date']-self._params['os_start_date']).days

    @timeit_m_real
    def calculate(self):
        self._params['os_days'] = self._os_days
        cp = CalcPerformance(self._params, self.factor_return(), self)
        self._performance_data = cp.GetPerformance()

    @timeit_m_real
    def create_report(self):
        cr = CreateReport(self._params, self.factor_return(), self._performance_data, self)
        self._performance_summary=cr.GetPerformanceSummary()


