#!/home/shuser/anaconda3/bin/python

import sys

import numpy
import pandas
from common_basic import *
from shared_cmn import *
from funcs_fld import re_index

class FactorReturn():
    #@timeit_m
    def __init__(self, params, input_data):
        self._input_data = input_data
        self._params=params
        self._factor_data = input_data['factor_data']
        self._return_data = input_data['return_data']
        self._index_return_data = input_data['index_return_data']
        self._factor_ma_data = None
        self._universe = {}
        #xxx rename dt to univ, dt = date

        #for univ in self._params['universe_data']:
            # if univ != 'all':
            #     if univ in input_data:
            #         self._universe[univ] = input_data[univ]

        for i in range(len(params['universe'])):
            name = params['universe'][i]
            dta = params['universe_data'][i]
            self._universe[f'univ_{name}'] = dta
            if params['verbose']>=1:
                print(f"INFO: FactorReturn() universe data name= {name:<8}, dta= {dta if type(dta) != type(pd.DataFrame()) else dta.shape}")


        self._beta_HS300 = input_data['beta_HS300']
        self._beta_ZZ500 = input_data['beta_ZZ500']
        self._sector_flag = input_data['sector_flag']
        self._sector_weight= input_data['sector_weight']
        self.calculate()

    def factor_data(self, univ='all'):
        if univ == 'all': 
            return self._factor_data
        else:
            #print("DEBUG_2398:", univ, self._universe.keys(), file=sys.stderr)

            if univ in self._universe and self._universe[univ] is not None:
                return self._factor_data.where(~numpy.isnan(self._universe[univ]),numpy.nan,inplace=False)
            else:
                return self._factor_data.where(numpy.isnan(self._factor_data),numpy.nan,inplace=False) #if the universe is not available, then everything set to nan

    def data(self, name):
        return self.GetFactorReturns()[name]

    #@timeit_m
    def GetFactorReturns(self):
        self._data={}
        self._data['factor_returns'] = self._factor_returns
        self._data['factor_net_returns'] = self._factor_net_returns
        self._data['factor_trades'] = self._factor_trades
        self._data['factor_weights'] = self._factor_weights
        self._data['factor_returns_group'] = self._factor_returns_QT_group
        return self._data

    @timeit_m_real
    def calculate(self):
        self._factor_returns = {}
        self._factor_net_returns = {}
        self._factor_trades = {}
        self._factor_weights = {}
        self._factor_returns_QT_group=None

        if len(self._params['universe_data'])>1 or self._params['calcQT_sector_demean'] == True:
            self._factor_data_backup = self._factor_data.copy()

        if len(self._params['universe_data'])>0:
            univ = self._params['universe_data'][0]
            if univ != 1:  #1 is 'all'
                self._factor_data.where(self._universe[univ]!=0,numpy.nan,inplace=True)

        if self._params['calcQT'] == True:
            self.calc_alpha_by_name('calcQT')

        if len(self._params['universe_data']) > 1:
            # for i in range(1,len(self._params['universe_data'])):
            #     #univ = self._params['universe_data'][i] #XXXX
            #     univ = self._params['universe'][i] 
            #     self.calc_alpha_by_name(f'calcQT_{univ}')

            #XXX
            if self._params['evaluation_model'] in ['cx_all']:
                for k,v in self._universe.items():
                    if k.find('all')<0:
                        self.calc_alpha_by_name(f'calcQT_{k}')

        if self._params['calcQT_HS300'] == True:
            self.calc_alpha_by_name('calcQT_HS300')

        if self._params['calcQT_ZZ500'] == True:
            self.calc_alpha_by_name('calcQT_ZZ500')

        if self._params['calcQT_beta_HS300'] == True:
            self.calc_alpha_by_name('calcQT_beta_HS300')

        if self._params['calcQT_beta_ZZ500'] == True:
            self.calc_alpha_by_name('calcQT_beta_ZZ500')

        if self._params['calcLS'] == True:
            self.calc_alpha_by_name('calcLS')

        if self._params['calcQT_ma'] == True:
            self.calc_alpha_by_name('calcQT_ma')

        if self._params['calcQT_sector_demean'] == True:
            self.calc_alpha_by_name('calcQT_sector_demean')

        if self._params['calcQT_group'] == True:
            self._factor_returns_QT_group = self.calculate_factor_returns_QT_group()

    #@timeit_m
    def calc_eval(self, alphapar, expr):
        if ctx_verbose(1):
            print(f"INFO: FactorReturn {alphapar:<24}: {expr.replace('self.', 'FactorReturn.')}", file=sys.stderr)
        factor_returns, factor_net_returns, trades, weights = eval(expr)
        self._factor_returns[f'alpha_{alphapar}'] = factor_returns
        self._factor_net_returns[f'alpha_{alphapar}'] = factor_net_returns
        self._factor_trades[f'alpha_{alphapar}'] = trades
        self._factor_weights[f'alpha_{alphapar}'] = weights

    #@timeit_m
    def calc_alpha_by_name(self, alphanm):
        alphapar = alphanm.replace('calc', '')
        if alphapar in ['QT_ZZ500', 'QT_HS300']:
            indexnm = alphapar.split(r'_')[-1]
            self.calc_eval(alphapar, f"self.calculate_factor_returns_QT_Index('{indexnm}')")
        elif alphapar in ['QT_beta_ZZ500', 'QT_beta_HS300']:
            indexnm = alphapar.split(r'_')[-1]
            self.calc_eval(alphapar, f"self.calculate_factor_returns_QT_Beta_Index('{indexnm}')")
        elif alphapar in ['QT_sector_demean']:
            #self.calc_eval(alphapar, f"self.calculate_factor_returns_QT_sector_demean()")
            pass
        elif alphapar in ['QT_ma']:
            self.calc_eval(alphapar, f"self.calculate_factor_returns_QT(self.factor_ma_data())")
        elif alphapar in ['LS']:
            self.calc_eval(alphapar, f"self.calculate_factor_returns_LS()")
        elif alphapar in ['QT']:
            self.calc_eval(alphapar, f"self.calculate_factor_returns_QT(self.factor_data())")
        elif alphapar.find("QT_univ_")>=0:
            univ = alphapar.replace('QT_', '')
            self.calc_eval(alphapar, f"self.calculate_factor_returns_QT(self.factor_data('{univ}'))")

    #@timeit_m
    def factor_ma_data(self):
        if self._factor_ma_data is None:
            self._factor_ma_data = self._input_data['factor_ma_data']
        return self._factor_ma_data

    def calculate_factor_returns_QT(self, factor_data, title="NA"):
        # print(self._factor_data.shape)
        # print_df(self._factor_data)
        # print(self._return_data.shape)
        # print_df(self._return_data)

        #positions = self.calc_positions(factor_data,long_only=False)
        positions = self.calc_positions(factor_data,long_only=True)

        tradeCost = self._params['trading_cost']
        trades = self.calc_trades(positions)

        port_returns = positions * self._return_data
        factor_returns = pandas.Series(port_returns.sum(axis=1))
        net_factor_returns = pandas.Series((port_returns - 0.5 * tradeCost * trades.abs()).sum(axis=1))

        return [factor_returns, net_factor_returns, trades, positions]

    #@timeit_m
    def calculate_factor_returns_QT_Index(self, index_name, hedge_ratio=1.0):
        positions = self.calc_positions(self._factor_data) * (1 - self._params['future_margin']*hedge_ratio)

        tradeCost = self._params['trading_cost']
        trades = self.calc_trades(positions)

        if index_name == 'HS300':
            index_col = '000300.XSHG'
        elif index_name == 'ZZ500':
            index_col = '000905.XSHG'
            
        index_returns = self.index_return_data(index_col) * (1 - self._params['future_margin']*hedge_ratio)

        port_returns = positions * self._return_data
        
        factor_returns = pandas.Series(port_returns.sum(axis=1)) - index_returns*hedge_ratio

        net_factor_returns = pandas.Series((port_returns - 0.5 * tradeCost * trades.abs()).sum(axis=1)) - index_returns*hedge_ratio

        return [factor_returns, net_factor_returns, trades, positions]

    #@timeit_m
    def calculate_factor_returns_QT_Beta_Index(self, index_name):
        positions = self.calc_positions(self._factor_data) * (1 - self._params['future_margin'])

        tradeCost = self._params['trading_cost']
        trades = self.calc_trades(positions)

        if index_name == 'HS300':
            index_col = '000300.XSHG'
            beta_data=self._beta_HS300
        elif index_name == 'ZZ500':
            index_col = '000905.XSHG'
            beta_data = self._beta_ZZ500
        index_returns = self.index_return_data(index_col) * (1 - self._params['future_margin'])
        port_returns = positions * self._return_data
        total_beta = (beta_data*positions).sum(axis=1)
        factor_returns = pandas.Series(port_returns.sum(axis=1)) - index_returns*total_beta
        net_factor_returns = pandas.Series((port_returns - 0.5 * tradeCost * trades.abs()).sum(axis=1)) - index_returns*total_beta

        return [factor_returns, net_factor_returns, trades, positions]

    #@timeit_m
    def calculate_factor_returns_QT_sector_demean(self, title="NA"):
        #print(self._factor_data0.shape)
        #print_df(self._factor_data0)
        #factor_data = self.factor_data().copy()
        factor_data_demean = self.sector_demean(self._factor_data_backup)    
        #print(factor_data.shape)
        #print_df(factor_data)
        return self.calculate_factor_returns_QT(factor_data_demean)

    #@timeit_m
    def sector_demean(self, factor_data):
        #nd, ns = self._sector_weight.shape
        nd = len(factor_data.index)
        ns = len(factor_data.columns)
        #print_df(self._sector_flag, title="sector_flag")
        self._sector_flag.fillna(0.0, inplace=True)  #unique below will treat each nan, and create too many loops if sector data are missing for some stocks
        sectors = numpy.unique(self._sector_flag)
        print(f"sector_demean sectors count: {len(sectors)}", file=sys.stderr)
        for i in sectors:
            #print(f"__sector_demean {i}")
            s = self._sector_weight.where(self._sector_flag  == i, numpy.nan, inplace=False)
            f_mean = (factor_data * s).sum(axis=1) / s.sum(axis=1)
            #print(f"DEBUG: f_mean= {f_mean.shape}, sector_weight= {self._sector_weight.shape}, index= {len(factor_data.index)}, colums= {len(factor_data.columns)}")
            f_mean = pandas.DataFrame(numpy.array([f_mean.values]).T + numpy.zeros((nd, ns)), index=factor_data.index,columns=factor_data.columns)
            f_mean.where(self._sector_flag == i, 0, inplace=True)
            factor_data = factor_data - f_mean
        return factor_data

    #@timeit_m
    def calculate_factor_returns_LS(self, title='NA'):
        # print(f"DEBUG: __calculate_factor_returns_LS {title}")
        # print(self._factor_data.shape, self._factor_data.head())
        # print(self._return_data.shape, self._return_data.head())
        positions = (self._factor_data.sub(self._factor_data.mean(axis=1), axis=0)).div(self._factor_data.abs().sum(axis=1), axis=0)
        tradeCost = self._params['trading_cost']
        trades = self.calc_trades(positions)

        port_returns = positions * self._return_data
        factor_returns = pandas.Series(port_returns.sum(axis=1), index=self._factor_data.index)
        net_factor_returns = pandas.Series((port_returns - 0.5 * tradeCost * trades.abs()).sum(axis=1),index=self._factor_data.index)

        return [factor_returns, net_factor_returns, trades, positions]

    #@timeit_m
    def calc_trades(self,positions):
        positions.fillna(0, inplace=True) #xxx
        trades = positions - positions.shift(1)
        trades.iloc[0, :] = 0
        return trades

    #@timeit_m
    def calc_positions(self,factor_data,long_only=True, quantile_combined_data=False):
        nd, ns = factor_data.shape
        
        qt_threshold = self._params['qt_threshold']
        if self._params['is_binary_factor']:
            qt_threshold = 0.50

        if 'is_quantile_combined_data' in self._params:
            quantile_combined_data = self._params['is_quantile_combined_data']
            if False:
                print_df(factor_data, title=f"is_quantile_combined_data={is_quantile_combined_data}")

        if not quantile_combined_data:
            #print_df(factor_data)
            qt2 = factor_data.quantile(1 - qt_threshold, axis=1) #XXX
            #qt2 = factor_data.quantile(1 - 0.1, axis=1) #XXX
            #print_df(qt2)
            qt2 = numpy.array([qt2]).T + numpy.zeros((nd, ns))
            qt2 = pandas.DataFrame(qt2, index=factor_data.index, columns=factor_data.columns)    
            #longPos = numpy.where(factor_data > qt2, 1, 0)
            ones = factor_data.copy()
            ones[:] = 1
            longPos = ones.where(factor_data > qt2, 0) #using DataFrame where function
        else:
            #longPos = numpy.where(factor_data > TINY, 1, 0) #XXX
            longPos = factor_data.where(factor_data>TINY, 0)

        nLong = longPos.sum(axis=1) + 1e-16
        print(f"INFO: type(longPos)= {type(longPos)}, type(nLong)= {type(nLong)}, type(factor_data)= {type(factor_data)}, nLong=", nLong.tail(5))
        
        constant_bankroll=True
        if constant_bankroll:
            longPos = longPos/600
        else:
            longPos = longPos / numpy.reshape(nLong, (nd, 1))

        print(f"INFO: calc_positions quantile_combined_data= {quantile_combined_data}, long_only= {long_only}, day_count={factor_data.shape[0]}, avg_pos_count= {nLong.sum(axis=0)/factor_data.shape[0]}")

        if long_only == False:
            if not quantile_combined_data:
                qt1 = factor_data.quantile(qt_threshold, axis=1)
                qt1 = numpy.array([qt1]).T + numpy.zeros((nd, ns))
                qt1 = pandas.DataFrame(qt1, index=factor_data.index, columns=factor_data.columns)
                #shortPos = numpy.where(factor_data < qt1, -1, 0)
                negones = factor_data.copy()
                negones[:] = -1
                shortPos = negones.where(factor_data < qt1, 0) #using DataFrame where function
            else:
                #shortPos = numpy.where(factor_data < -TINY, -1, 0)
                shortPos = factor_data.where(factor_data < -TINY, 0)

            nShort = -shortPos.sum(axis=1) + 1e-16
            shortPos = shortPos / numpy.reshape(nShort, (nd, 1)) / 2
            longPos = longPos/2

        if long_only:
            #positions = pandas.DataFrame(longPos, index=self._factor_data.index,columns=self._factor_data.columns)
            return longPos
        else:
            #positions = pandas.DataFrame(longPos + shortPos, index=self._factor_data.index, columns=self._factor_data.columns)
            return(longPos + shortPos)
    
    def rank_stocks_deciles(self, factor_data):
        deciles = factor_data.apply(lambda x: pandas.qcut(x.rank(method='dense'), q=10, labels=range(0,10), duplicates='drop'), axis=1)
        return deciles

    def __calculate_factor_returns_QT_group(self): #This impl generate similar results, but probably not as robust as one below, which was an older version 
        self._factor_data = re_index(self._factor_data, self._return_data)
        deciles = self.rank_stocks_deciles(self._factor_data)
        #print_df(deciles, title = "DEBUG_dsfa")
        (deciles, self._return_data) = align_index(deciles, self._return_data)
        #print(deciles.shape, self._factor_data.shape, self._return_data.shape)

        group_returns = {}
        for grp in range(10):
            group_returns[grp] = (self._return_data * numpy.where(deciles == grp, 1.0, 0)).sum(axis=1)
        
        return(pandas.DataFrame.from_dict(group_returns, orient='columns'))

    #@timeit_m
    def calculate_factor_returns_QT_group(self): #previous impl, same results, but harder to understand
        self._factor_data = re_index(self._factor_data, self._return_data)
        #re_index(self._return_data, self._factor_data)
        self._factor_data = self._factor_data.dropna(how='all', axis=0) #sometimes the last row is all nan
        if False:
            print_df(self._factor_data, title=f"DEBUG_3453: factor_data")
            print_df(self._return_data, title=f"DEBUG_3454: return_data")
        return self._factor_data.apply(lambda ts: self.get_group_return(ts, self._return_data.loc[ts.name], name=ts.name), axis=1)

    #@timeit_m
    def get_group_return(self, factor_ts, return_ts, name=None):
        #print(f"DEBUG_3455: {name}")
        ts = factor_ts.dropna().sort_values(ascending=False)
        ts_list = numpy.array_split(ts, 10)
        return pandas.Series([re_index(return_ts, split_ts).mean() for split_ts in ts_list])
        #return pandas.Series([return_ts.reindex(split_ts.index).mean() for split_ts in ts_list])

    def index_return_data(self, index_col=None):
        funcn = 'factor_return.index_return_data'
        #print(f"DEBUG_2341: {funcn} {index_col}")
        if self._index_return_data is None:
            self._index_return_data = smart_load(self._params['data_dir'] + self._params['index_return_file_name'], debug=True, title=funcn)

        if index_col is None:
            return self._index_return_data
        else:
            return self._index_return_data[index_col]

