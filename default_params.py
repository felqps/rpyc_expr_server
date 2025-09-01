import time

def set_default_params(simple_report):
    params={
        'alpha_name': 'alpha_%d' % time.time(),
        'alpha_id': 'None',
        'alpha_type': 'None',
        'alpha_sub_type': 'None',
        'author': 'None',
        'data_dir': '/Data/input_data/',
        'factor_dir': '/Data/factor_data/',
        'result_dir': '/Data/alpha_result/', 
        'return_file_name': 'returns',
        'index_return_file_name': 'index_returns',
        'beta_HS300_file_name': 'beta_HS300',
        'beta_ZZ500_file_name': 'beta_ZZ500',
        'sector_file_name': 'sector',
        'ipo_age_file_name': 'ipo_age',
        'st_flag_file_name': 'st_flag',
        'min_ipo_age': 120,
        'exclude_st': True,
        'trading_cost': 0.002,
        'future_margin': 0.2,
        'trading_days': 243,
        'trading_frequency': 'D',
        'min_ipo_days': 90,
        'qt_threshold': 0.20,
        'ma_days': 10,
        'n_group': 10,
        'year_bucket': None,
        'turnover_days': [1,2,5,10,20],
        'decay_days': [1,2,5,10,20],
        'exponential_return': False,
        'start_date': None,
        'end_date': None,
        'is_start_date': None,
        'is_end_date': None,
        'os_start_date': None,
        'os_end_date': None,
        'result_file': 'performance_',
        'summary_file': 'summary_',
        'graph_file': 'graph_',
        'pdf_file': 'factor_report_',
        'alpha_return_file': 'alpha_return_',
        'factor_data': None, #xxx
        'return_data': None, #xxx
        'simple_report': simple_report,
        'is_binary_factor': False
    }
    params['factor_file']=params['alpha_name']
    if simple_report==True:
        params['calcQT']=True
        params['calcLS']=False
        params['calcQT_HS300'] = False
        params['calcQT_ZZ500'] = True
        params['calcQT_beta_HS300'] = False
        params['calcQT_beta_ZZ500'] = False
        params['calcQT_sector_demean'] = False
        params['calcQT_ma'] = False
        params['calcQT_group'] = False
        params['universe'] = ['Top1800']
        params['universe_file_name'] = ['Universe_Top1800']
        params['calc_ic'] = False
        params['calc_decay'] = False
        params['calc_factor_turnover'] = True
        params['display_coverage'] = False
        params['create_summary'] = False
        params['display_result'] = True
        params['display_graph'] = True
        params['save_result'] = False
        params['create_pdf'] = False
        params['save_alpha_return'] = False
    else:
        params['calcQT']=True
        params['calcLS']=True
        params['calcQT_HS300'] = True
        params['calcQT_ZZ500'] = True
        params['calcQT_beta_HS300'] = True
        params['calcQT_beta_ZZ500'] = True
        params['calcQT_sector_demean'] = True
        params['calcQT_ma'] = True
        params['calcQT_group'] = True
        params['universe'] = ['Top1800', 'Top1200', 'ZZ800', 'ZZ500', 'HS300']
        params['universe_file_name'] = ['Universe_Top1800', 'Universe_Top1200', 'Universe_CN800','Universe_CN500', 'Universe_CN300']
        params['calc_ic'] = True
        params['calc_decay'] = True
        params['calc_factor_turnover'] = True
        params['display_coverage'] = True
        params['create_summary'] = True
        params['display_result'] = False
        params['display_graph'] = False
        params['save_result'] = True
        params['create_pdf'] = True
        params['save_alpha_return'] = True
    return params
