#!/home/shuser/anaconda3/bin/python

import os,sys

import numpy
import math
from dateutil.parser import parse as dtparse
from common_basic import *
from common_colors import *
import register_all_functions

class CalcPerformance():
    @timeit_m_real
    def __init__(self, params, factor_return, alpha_performance):
        self.__params=params
        self.__os_days = params['os_days']
        self.__factor_returns = factor_return.data('factor_returns')
        self.__factor_net_returns = factor_return.data('factor_net_returns')
        self.__factor_trades = factor_return.data('factor_trades')
        self.__factor_weights = factor_return.data('factor_weights')

        self.__alpha_performance = alpha_performance
        # self.__return_data = alpha_performance.input_data('return_data')
        # self.__factor_data = alpha_performance.input_data('factor_data')
        self.__calculate()

    @timeit_m_real
    def GetPerformance(self):
        perf_data={}
        perf_data['performance']=self.__factor_performance
        perf_data['net_performance'] = self.__factor_net_performance
        perf_data['ic'] = self.__ic
        perf_data['factor_turnover'] = self.__factor_turnover
        perf_data['decayed_ic'] = self.__decayed_ic
        perf_data['horizon_ic'] = self.__horizon_ic
        perf_data['factor_decay'] = self.__factor_decay
        return perf_data

    @timeit_m_real
    def __calculate(self):
        self.__factor_performance = {}
        self.__factor_net_performance = {}
        for alpha in self.__factor_returns.keys():
            factor_returns = self.__factor_returns[alpha]
            factor_net_returns = self.__factor_net_returns[alpha]
            factor_trades = self.__factor_trades[alpha]
            factor_weights = self.__factor_weights[alpha]
            self.__factor_performance[alpha]=self.__calculate_performance(factor_returns,factor_trades,factor_weights, title=alpha)
            self.__factor_net_performance[alpha]=self.__calculate_performance(factor_net_returns,factor_trades,factor_weights, title=alpha)

        self.__ic = None
        self.__factor_turnover = None
        self.__decayed_ic = None
        self.__horizon_ic = None
        self.__factor_decay = None
        if self.__params['calc_ic'] == True:
            self.__ic = self.__calculate_ic()
        if self.__params['calc_factor_turnover'] == True:
            self.__factor_turnover = self.__calculate_factor_turnover()
        if self.__params['calc_decay'] == True:
            self.__decayed_ic,self.__horizon_ic,self.__factor_decay = self.__calculate_decay()

    #@timeit_m_real
    def __calculate_performance(self, factor_returns, trades=None, weights=None, title="NA"):
        perfs = {}
        perf = self.__calculate_perf(factor_returns, trades, weights, title='all')
        perfs['all'] = perf
        perf_items = list(perf.keys())
        if self.__params['year_bucket'] == None:
            years = list(set(map(lambda x: x.year, factor_returns.index.to_pydatetime())))
        else:
            years = self.__params['year_bucket']
        for y in years:
            factor_return_select = factor_returns.loc[str(y)]
            if trades is not None:
                try:
                    trades_select = trades.loc[str(y)]
                except Exception as e:
                    if False:
                        print(f"Exception: __calculate_performance(title={title}), trades_select missing year", e, file=sys.stderr)
                    continue
            else:
                trades_select = None
            if weights is not None:
                try: 
                    weights_select =  weights.loc[str(y)]
                except Exception as e:
                    if False:
                        print(f"WARNING: __calculate_performance weights {e}", file=sys.stderr)
                    continue
            else:
                weights_select = None
            perfs[str(y)] = self.__calculate_perf(factor_return_select, trades_select, weights_select, title=str(y))

        if self.__os_days > 0.1:
            factor_return_select = factor_returns[factor_returns.index >= dtparse(self.__params['is_start_date'])]
            factor_return_select = factor_returns[factor_return_select.index <= dtparse(self.__params['is_end_date'])]
            perfs['is'] = self.__calculate_perf(factor_return_select, title='is')
            factor_return_select = factor_returns[factor_returns.index >= dtparse(self.__params['os_start_date'])]
            factor_return_select = factor_returns[factor_return_select.index <= dtparse(self.__params['os_end_date'])]
            perfs['os'] = self.__calculate_perf(factor_return_select, title='os')
        else:
            perfs['is'] = perfs['all']
            perfs['os'] = dict.fromkeys(perfs['all'].keys(), numpy.nan)

        return [list(perfs.keys()), perf_items, perfs]

    #@timeit_m_real
    def __calculate_perf(self, factor_returns, trades=None, weights=None, title="NA"):
        avg_ret = float(factor_returns.mean())
        std_ret = float(factor_returns.std())
        if std_ret > 0:
            ir = avg_ret / std_ret
        else:
            ir = 0

        if self.__params['exponential_return'] == True:
            navs = numpy.exp(factor_returns).cumprod()
        else:
            navs = (1 + factor_returns).cumprod()
        drawdown = navs / navs.cummax() - 1
        maxDrawdown = float(drawdown.min())
        maxRecDays = self.__calc_rec_days(navs)

        if trades is None:
            turnover = numpy.nan
            # import sys
            # print(f'DEBUG: missing trades, title={title}', file=sys.stderr)
        else:
            turnover=trades.abs().sum(axis=1)/weights.shift(1).abs().sum(axis=1)
            turnover.where(turnover<1e10, 1, inplace=True)
            turnover=turnover.mean()
            # print(trades)
            # print(weights)
            # print(turnover)   

        ndays = self.__params['trading_days']
        sqrt_ndays = math.sqrt(ndays)
        perf = {}
        perf['Annualized Return'] = ndays * avg_ret
        perf['Annualized Volatility'] = sqrt_ndays * std_ret
        perf['Annualized IR'] = sqrt_ndays * ir
        perf['Max Drawdown'] = maxDrawdown
        perf['Max Recovery Periods'] = maxRecDays
        perf['Turnover'] = float(turnover)
        return perf

    #@timeit_m_real
    def __calc_rec_days(self, navs):
        max_val = navs[0]
        count = 0
        max_count = 0  # 记录最大回撤天数
        for i in range(1, navs.size):
            if navs[i] > max_val:
                max_val = navs[i]
                count = 0
            else:
                count = count + 1
            if count > max_count:
                max_count = count
        return max_count

    #@timeit_m_real
    def __calculate_corr(self, x, y):
        return register_all_functions.calculate_corr(x,y)

    @timeit_m_real
    def __calculate_decay(self):
        decayed_ic = {}
        horizon_ic = {}
        factor_decay = {}
        for i in self.__params['decay_days']:
            x = self.__alpha_performance.input_data('factor_data').shift(i)
            y = self.__alpha_performance.input_data('return_data')
            icmean, icstd, ir = self.__calculate_corr(x, y)
            ic = {}
            ic['Mean Decayed IC'] = icmean
            ic['STD Decayed IC'] = icstd
            ic['IR From Decayed IC'] = ir
            decayed_ic[str(i)] = ic

            x = self.__alpha_performance.input_data('factor_data')
            y = self.__alpha_performance.input_data('return_data').rolling(i).sum()
            icmean, icstd, ir = self.__calculate_corr(x, y)
            ic = {}
            ic['Mean Horizon IC'] = icmean
            ic['STD Horizon IC'] = icstd
            ic['IR From Horizon IC'] = ir
            horizon_ic[str(i)] = ic

            x = self.__alpha_performance.input_data('factor_data').shift(i)
            y = self.__alpha_performance.input_data('factor_data')
            icmean, icstd, ir = self.__calculate_corr(x, y)
            ic = {}
            ic['Mean Factor Decay'] = icmean
            factor_decay[str(i)] = ic

        return [decayed_ic, horizon_ic, factor_decay]

    @timeit_m_real
    def __calculate_factor_turnover(self):
        factor_turnovers = {}
        for i in self.__params['turnover_days']:
            factor_data = self.__alpha_performance.input_data('factor_data').copy()
            factor_data.fillna(0, inplace=True)
            #trades = self.__alpha_performance.input_data('factor_data') - self.__alpha_performance.input_data('factor_data').shift(i)
            trades = factor_data - factor_data.shift(i)
            gross =  factor_data.abs() + factor_data.shift(i).abs()      
            turnover = trades.abs().sum(axis=1) / (gross.sum(axis=1)+MICRO)
            turnover = turnover.mean() * 2.0
            factor_turnovers[str(i)] = turnover
            if False:
                print(f"{RED}DEBUG_4322: ======================= calculate_factor_turnover {i} {turnover}{NC}")
        return factor_turnovers

    @timeit_m_real
    def __calculate_ic(self):
        if self.__params['year_bucket'] == None:
            years = list(set(map(lambda x: x.year, self.__alpha_performance.input_data('factor_data').index.to_pydatetime())))
        else:
            years = self.__params['year_bucket']
        years = ['all'] + years
        ic = {}

        for yr in years:
            ic[yr] = {}
            if yr == 'all':
                x = self.__alpha_performance.input_data('factor_data')
                y = self.__alpha_performance.input_data('return_data')
            else:
                x = self.__alpha_performance.input_data('factor_data').loc[str(yr)]
                y = self.__alpha_performance.input_data('return_data').loc[str(yr)]
            icmean, icstd, ir = self.__calculate_corr(x, y)
            ic[yr]['Mean IC'] = icmean
            ic[yr]['STD IC'] = icstd
            ic[yr]['IR From IC'] = ir

            x_rank = x.rank(axis=1)
            y_rank = y.rank(axis=1)
            icmean, icstd, ir = self.__calculate_corr(x_rank, y_rank)
            ic[yr]['Mean Rank IC'] = icmean
            ic[yr]['STD Rank IC'] = icstd
            ic[yr]['IR From Rank IC'] = ir

        return ic

