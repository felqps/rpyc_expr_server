import numpy
import pandas
import pickle
from matplotlib import pyplot as plt
from reportlab.platypus import Paragraph, SimpleDocTemplate, Image, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus.tables import Table, TableStyle
from reportlab.lib.units import mm
from reportlab.lib import colors
from common_basic import *
from math import sqrt

class CreateReport():
    def __init__(self, params, factor_return, performance_data, alpha_performance):
        self.__performance_summary=None
        self.__params=params
        self.__simple_report=params['simple_report']
        self.__factor_performance = performance_data['performance']
        self.__factor_net_performance = performance_data['net_performance']
        self.__ic = performance_data['ic']
        self.__factor_turnover = performance_data['factor_turnover']
        self.__decayed_ic = performance_data['decayed_ic']
        self.__horizon_ic = performance_data['horizon_ic']
        self.__factor_decay = performance_data['factor_decay']
        self.__factor_returns = factor_return.data('factor_returns')
        self.__factor_net_returns = factor_return.data('factor_net_returns')
        self.__factor_weights = factor_return.data('factor_weights')
        self.__factor_returns_group = factor_return.data('factor_returns_group')
        self.__factor_data = alpha_performance.input_data('factor_data')

        self.__create_report()

    def GetPerformanceSummary(self):
        return self.__performance_summary

    @timeit_m_real
    def __create_report(self):
        if self.__params['display_result'] == True and self.__simple_report == True:
            self.__short_display()
        elif self.__params['display_result'] == True and self.__simple_report == False and ctx_verbose(1):
            self.__long_display()

        if self.__params['display_graph'] == True or self.__params['create_pdf'] == True or self.__params['save_result'] == True:
            self.__create_graph()

        if self.__params['create_summary'] == True:
            self.__create_summary()

        if self.__params['save_result'] == True:
            self.__create_performance_file()

        if self.__params['save_alpha_return'] == True:
            self.__create_alpha_return_file()

        if self.__params['create_pdf'] == True:
            self.__create_pdf_file()

    def __create_performance_file(self):
        file_name = self.__params['result_dir'] + "/" + self.__params['result_file'] + self.__params['alpha_name']+'.pkl'
        #file_name = self.__params['result_dir'] + "/" + self.__params['result_file'] + '.pkl'
        save_data = {}
        for alpha in self.__factor_performance.keys():
            years, perf_items, perf_data = self.__factor_performance[alpha]
            save_data[alpha] = perf_data
        if self.__params['calc_ic'] == True:
            save_data['ic'] = self.__ic
        if self.__params['calc_decay'] == True:
            save_data['decayed_ic'] = self.__decayed_ic
            save_data['horizon_ic'] = self.__horizon_ic
            save_data['factor_decay'] = self.__factor_decay
        if self.__params['calc_factor_turnover'] == True:
            save_data['factor_turnover'] = self.__factor_turnover
        # f = open(file_name, 'wb')
        # pickle.dump(save_data, f)
        smart_dump(save_data, file_name)
        # f.close()

        if self.__params['create_summary'] == True:
            file_name = self.__params['result_dir'] + "/" + self.__params['summary_file'] + self.__params['alpha_name']+'.pkl'
            #file_name = self.__params['result_dir'] + "/" + self.__params['summary_file'] + '.pkl'
            # f = open(file_name, 'wb')
            # pickle.dump(self.__performance_summary, f)
            # f.close()
            smart_dump(self.__performance_summary, file_name)

    def __create_alpha_return_file(self, title="create_alpha_return_file"):
        # smart_dump(self.__factor_returns, f"{self.__params['result_dir']}/factor_returns.{self.__params['alpha_name']}.pkl", debug=True, title=title)
        # smart_dump(self.__factor_net_returns, f"{self.__params['result_dir']}/factor_net_returns.{self.__params['alpha_name']}.pkl", debug=True, title=title)
        smart_dump(self.__factor_returns, f"{self.__params['result_dir']}/factor_returns.pkl", debug=True, title=title)
        smart_dump(self.__factor_net_returns, f"{self.__params['result_dir']}/factor_net_returns.pkl", debug=True, title=title)

    def __create_pdf_file(self):
        dfs = self.__create_data_for_pdf()

        stylesheet = getSampleStyleSheet()
        Heading2 = stylesheet['Heading2']
        story = []
        story.append(Paragraph("Alpha Report", stylesheet['Title']))

        for it in dfs.keys():
            df = dfs[it]
            story.append(Paragraph("<seq id='spam'/>. " + it, Heading2))
            if it == 'Basics':
                story.append(self.__get_reportlab_table(df, False))
            else:
                story.append(self.__get_reportlab_table(df))

        story.append(Paragraph("<seq id='spam'/>. Figure", Heading2))
        import glob
        for figfp in glob.glob(self.__params['result_dir'] + '/' + '*.png'):
            # story.append(Image(self.__params['result_dir'] + self.__params['graph_file'] + self.__params['alpha_name'] + '.png',width=500, height=500))
            # story.append(Image(self.__params['result_dir'] +  'alpha_raw.png',width=500, height=300))
            story.append(Image(figfp, width=500, height=300))

        story.append(Spacer(1, 10 * mm))
        comment_text = ParagraphStyle(name='ct', leftIndent=25, fontSize=6, spaceAfter=2) #XXX fontsize
        story.append(Paragraph("Alpha Type", stylesheet['Heading3']))
        story.append(Paragraph("alpha_QT:               long equal weights by quantile (not long/short as previous, as shorts are hard to implement in real trading)", comment_text))
        story.append(Paragraph("alpha_LS:               long short weights by factor values", comment_text))
        story.append(Paragraph("alpha_QT_HS300:         long equal weights by quantile, hedged by HS300", comment_text))
        story.append(Paragraph("alpha_QT_ZZ500:         long equal weights by quantile, hedged by ZZ500", comment_text))
        story.append(Paragraph("alpha_QT_univ_xxx:      long equal weights by quantile, using xxx as universe", comment_text))
        story.append(Paragraph("alpha_QT_beta_HS300:    long equal weights by quantile, hedged by HS300,beta neutral", comment_text))
        story.append(Paragraph("alpha_QT_beta_ZZ500:    long equal weights by quantile, hedged by ZZ500,beta neutral", comment_text))
        story.append(Paragraph("alpha_QT_ma:            long equal weights by quantile, smoothed by MA", comment_text))
        story.append(Paragraph("alpha_QT_sector_demean: long equal weights by quantile, de-meaned in sectors", comment_text))

        if self.__params['pdf_file'].find(".pdf")>=0: #xxx, user specify filename directly
            file_name = self.__params['result_dir'] + "/" + self.__params['pdf_file']
        else:
            file_name = self.__params['result_dir'] + "/" + self.__params['pdf_file'] + self.__params['alpha_name'] + '.pdf'
        print(f"{BROWN}INFO: create_pdf_file {file_name}{NC}")
        doc = SimpleDocTemplate(file_name)
        doc.build(story)

    def __create_data_for_pdf(self):
        dfs = {}
        fields = ['Alpha ID', 'Author', 'Alpha Type', 'Alpha Sub-type', 'Universe', 'IS Start Date', 'IS End Date',
                  'OS Start Date',
                  'OS End Date', 'OS Days', 'Trading Frequency', 'Trading Cost']
        if self.__params['universe'] is not None and self.__params['universe'] != []:
            universe = self.__params['universe'][0]
        else:
            universe='None'
        values = [self.__params['alpha_id'], self.__params['author'], self.__params['alpha_type'],
                  self.__params['alpha_sub_type'],universe,
                  self.__params['is_start_date'].strftime('%Y/%m/%d'), self.__params['is_end_date'].strftime('%Y/%m/%d'),
                  self.__params['os_start_date'].strftime('%Y/%m/%d'),
                  self.__params['os_start_date'].strftime('%Y/%m/%d'), self.__params['os_days'], self.__params['trading_frequency'],
                  self.__params['trading_cost']]
        df = pandas.DataFrame(numpy.array([fields, values]).T, columns=('Alpha Name', self.__params['alpha_name']))
        x=numpy.array([fields, values]).T
        y=numpy.array(['Alpha Name', self.__params['alpha_name']])
        dfs['Basics'] = df

        alphas = list(self.__factor_performance.keys())
        years, perf_items, perf_data = self.__factor_performance[alphas[0]]
        n_alpha = len(alphas)
        n_years = len(years)
        for pit in perf_items:
            values = numpy.full((n_alpha, n_years), numpy.nan)
            for i in range(n_alpha):
                years, perf_items, perf_data = self.__factor_performance[alphas[i]]
                for j in range(n_years):
                    if j < len(years): #XXX need check when some years are missing
                        if years[j] in perf_data:
                            values[i][j] = perf_data[years[j]][pit]
            dfs[pit] = pandas.DataFrame(values.round(4), index=alphas, columns=years)
            if pit != 'Turnover':
                for i in range(n_alpha):
                    years, perf_items, perf_data = self.__factor_net_performance[alphas[i]]
                    for j in range(n_years):
                        if j < len(years):
                            if years[j] in perf_data:
                                values[i][j] = perf_data[years[j]][pit]
                dfs['Net ' + pit] = pandas.DataFrame(values.round(4), index=alphas, columns=years)

        if self.__params['calc_ic'] == True:
            years = list(self.__ic.keys())
            ic_items = list(self.__ic[years[0]].keys())
            n_items = len(ic_items)
            n_years = len(years)
            values = numpy.full((n_items, n_years), numpy.nan)
            for i in range(n_items):
                for j in range(n_years):
                    values[i][j] = self.__ic[years[j]][ic_items[i]]
            dfs['IC'] = pandas.DataFrame(values.round(4), index=ic_items, columns=years)

        if self.__params['calc_decay'] == True:
            days = list(self.__decayed_ic.keys())
            decay_items = list(self.__decayed_ic[days[0]].keys())
            horizon_items = list(self.__horizon_ic[days[0]].keys())
            factor_items = list(self.__factor_decay[days[0]].keys())
            n_decay_items = len(decay_items)
            n_horizon_items = len(horizon_items)
            n_factor_items = len(factor_items)
            n_items = n_decay_items + n_horizon_items + n_factor_items
            n_days = len(days)
            values = numpy.full((n_items, n_days), numpy.nan)
            for i in range(n_decay_items):
                for j in range(n_days):
                    values[i][j] = self.__decayed_ic[days[j]][decay_items[i]]

            for i in range(n_horizon_items):
                for j in range(n_days):
                    values[n_decay_items + i][j] = self.__horizon_ic[days[j]][horizon_items[i]]

            for i in range(n_factor_items):
                for j in range(n_days):
                    values[n_decay_items + n_horizon_items + i][j] = self.__factor_decay[days[j]][factor_items[i]]
            dfs['IC Decay And Horizon'] = pandas.DataFrame(values.round(4),
                                                           index=decay_items + horizon_items + factor_items,
                                                           columns=days)

        if self.__params['calc_factor_turnover'] == True:
            days = list(self.__factor_turnover.keys())
            n_days = len(days)
            values = numpy.full((1, n_days), numpy.nan)
            for i in range(n_days):
                values[0][i] = self.__factor_turnover[days[i]]
            dfs['Factor Turnover'] = pandas.DataFrame(values.round(4), index=['Factor Turnover'], columns=days)

        return dfs

    def __get_reportlab_table(self, df, set_index=True):
        df = df.replace(numpy.nan, "")
        if set_index == True:
            df = df.reset_index()
        data = [list(df.columns)]
        if set_index == True:
            data[0][0] = ''
        data.extend(df.values.tolist())

        table = Table(data)
        table.setStyle(TableStyle([('ALIGN', (0, 0), (-1, -1), 'CENTRE'),
                                   ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                   ('FONTSIZE', (0, 0), (-1, -1), 4), #XXX fontsize
                                   ('GRID', (0, 0), (-1, -1), 0.5, colors.black)]))
        return table

    def __short_display(self):
        print(' ')
        for alpha in self.__factor_performance.keys():
            print(alpha)
            years, perf_items, perf_data = self.__factor_performance[alpha]
            print('%1s' % '-' * (28 + 12 * len(years)))
            print('%28s' % ' ', end='')
            for y in years:
                print('%12s' % y, end='')
            print(' ')
            print('%1s' % '-' * (28 + 12 * len(years)))
            for pit in perf_items:
                print('%28s' % pit, end='')
                for y in years:
                    print('%12.4f' % perf_data[y][pit], end='')
                print(' ')
            print('%1s' % '-' * (28 + 12 * len(years)))

            years, perf_items, perf_data = self.__factor_net_performance[alpha]
            for pit in perf_items:
                print('%28s' % ('Net ' + pit), end='')
                for y in years:
                    print('%12.4f' % perf_data[y][pit], end='')
                print(' ')
            print('%1s' % '-' * (28 + 12 * len(years)))
            print('\n')


    def __long_display(self):
        print(' ')
        alphas = list(self.__factor_performance.keys())
        years, perf_items, perf_data = self.__factor_performance[alphas[0]]
        for pit in perf_items:
            print(pit, '(long_display)')
            print('%1s' % '-' * (28 + 12 * len(years)))
            print('%28s' % ' ', end='')
            for y in years:
                print('%12s' % y, end='')
            print(' ')
            print('%1s' % '-' * (28 + 12 * len(years)))
            for alp in alphas:
                years, perf_items, perf_data = self.__factor_performance[alp]
                print('%28s' % alp, end='')
                for y in years:
                    print('%12.4f' % perf_data[y][pit], end='')
                print(' ')
            print('\n')

            if pit == 'Turnover':
                continue
                
            print('Net ' + pit)
            for alp in alphas:
                years, perf_items, perf_data = self.__factor_net_performance[alp]
                print('%28s' % alp, end='')
                for y in years:
                    print('%12.4f' % perf_data[y][pit], end='')
                print(' ')
            print('\n')

        if self.__params['calc_ic'] == True:
            print('IC')
            years = list(self.__ic.keys())
            print('%1s' % '-' * (28 + 12 * len(years)))
            print('%28s' % ' ', end='')
            for y in years:
                print('%12s' % y, end='')
            print(' ')
            print('%1s' % '-' * (28 + 12 * len(years)))
            ic_items = list(self.__ic[years[0]].keys())
            for ct in ic_items:
                print('%28s' % ct, end='')
                for y in years:
                    ic = self.__ic[y]
                    print('%12.4f' % ic[ct], end='')
                print(' ')
            print('\n')

        if self.__params['calc_decay'] == True:
            print('IC Decay and Horizon')
            days = list(self.__decayed_ic.keys())
            print('%1s' % '-' * (28 + 12 * len(days)))
            print('%28s' % ' ', end='')
            for d in days:
                print('%12s' % d, end='')
            print(' ')
            print('%1s' % '-' * (28 + 12 * len(days)))
            decay_items = self.__decayed_ic[days[0]].keys()
            for dt in decay_items:
                print('%28s' % dt, end='')
                for d in days:
                    ic = self.__decayed_ic[d]
                    print('%12.4f' % ic[dt], end='')
                print(' ')
            horizon_items = self.__horizon_ic[days[0]].keys()
            for dt in horizon_items:
                print('%28s' % dt, end='')
                for d in days:
                    ic = self.__horizon_ic[d]
                    print('%12.4f' % ic[dt], end='')
                print(' ')
            factor_items = self.__factor_decay[days[0]].keys()
            for dt in factor_items:
                print('%28s' % dt, end='')
                for d in days:
                    ic = self.__factor_decay[d]
                    print('%12.4f' % ic[dt], end='')
                print(' ')
            print('\n')
        if self.__params['calc_factor_turnover'] == True:
            print('Factor Turnover')
            days = list(self.__factor_turnover.keys())
            print('%1s' % '-' * (28 + 12 * len(days)))
            print('%28s' % ' ', end='')
            for d in days:
                print('%12s' % d, end='')
            print(' ')
            print('%1s' % '-' * (28 + 12 * len(days)))
            print('%28s' % 'Factor Turnover', end='')
            for d in days:
                print('%12.4f' % self.__factor_turnover[d], end='')
            print('\n')

    def factor_performance_keys_group(self, group_by):
        klist = []
        for alpha in self.__factor_performance.keys():
            if group_by in ['hedged']:
                if alpha.find("univ")>=0 or alpha.find('LS')>=0:
                    continue
            elif group_by in ['nohedge']:
                if alpha.find('LS')>=0 or alpha.find("beta")>=0 or alpha.find("QT_HS") >= 0 or alpha.find("QT_ZZ") >= 0:
                    continue
            elif group_by in ['longshort']:
                if alpha.find('LS')<0:
                    continue
            klist.append(alpha)
        return(klist)

    def plot_alpha_implementation(self, keys, datadict, ax, trading_days=243, title="Title_NA"): #CN=243
        ret_by_name = {}
        for alpha in keys:
            ret = datadict[alpha]
            ir = ret.mean() / (ret.std() + MICRO) * sqrt(trading_days)
            name = f"[IR={round(ir,2):<5}] {alpha}"
            ret_by_name[name] = ret

        for name in sorted(ret_by_name.keys(), reverse=True):
            ax.plot(ret_by_name[name].cumsum(), label=name)
        
        ax.set_title(title)
        ax.legend(fontsize=14,loc=2)

    def plot_alpha_raw(self, group_by='all'):
        fig = plt.figure(figsize=(20,15))
        ax = fig.add_subplot(111)
        self.plot_alpha_implementation(self.factor_performance_keys_group(group_by), self.__factor_returns, ax, title=f"factor raw returns (group_by={group_by})")

        # ret_by_name = {}
        # for alpha in self.factor_performance_keys_group(group_by):
        #     ret = self.__factor_returns[alpha].cumsum()
        #     ir = ret.mean() / ret.std() * sqrt(243)
        #     name = f"[IR={round(ir,2):<5}] {alpha}"
        #     ret_by_name[name] = ret

        # for name in sorted(ret_by_name):
        #     ax.plot(ret_by_name[name], label=name)

        # ax.legend(fontsize=14,loc=2)
        plt.savefig(self.__params['result_dir'] + "/" + f'alpha_raw_{group_by}')
        plt.close()

    def plot_alpha_net(self, group_by='all'):
        fig = plt.figure(figsize=(20,15))
        ax = fig.add_subplot(111)

        self.plot_alpha_implementation(self.factor_performance_keys_group(group_by), self.__factor_net_returns, ax, title=f"factor net returns (group_by={group_by})")

        # for alpha in self.factor_performance_keys_group(group_by):
        #     ax.plot(self.__factor_net_returns[alpha].cumsum(), label=alpha)
        # ax.legend(fontsize=14,loc=2)
        plt.savefig(self.__params['result_dir'] + "/" + f'alpha_net_{group_by}')
        plt.close()

    def plot_qt_groups(self):
        fig = plt.figure(figsize=(20,15))
        ax = fig.add_subplot(111)
        group_cum_returns = self.__factor_returns_group.cumsum()
        for col_name in group_cum_returns.columns:
            ax.plot(group_cum_returns[col_name], label=col_name)
        ax.legend(fontsize=14,loc=2)
        ax.set_title("qt group")
        plt.savefig(self.__params['result_dir'] + "/" + 'qt_group')
        plt.close()

    def plot_coverage(self):
        fig = plt.figure(figsize=(20,15))
        ax = fig.add_subplot(111)
        coverage = (~self.__factor_data.isna()).sum(axis=1)
        long_num_stock = (self.__factor_weights['alpha_QT']>0).sum(axis=1)
        short_num_stock = (self.__factor_weights['alpha_QT']<0).sum(axis=1)
        ax.plot(coverage, label='coverage')
        ax.plot(long_num_stock, color='r',  label='long stock holdings')
        ax.plot(short_num_stock, color='g',  label='short stock holdings')
        ax.legend(fontsize=14,loc=2)
        ax.set_title("stock holdings coverage")
        plt.savefig(self.__params['result_dir'] + "/" + 'coverage')
        plt.close()

    def __create_graph(self):
        for group_by in ["nohedge", "hedged", "longshort", "all"]:
            self.plot_alpha_raw(group_by=group_by)
            self.plot_alpha_net(group_by=group_by)

        if self.__params['calcQT_group'] == True:
            self.plot_qt_groups()
        if self.__params['display_coverage'] == True:
            self.plot_coverage()
        return

        n_graph = 2
        if self.__params['calcQT_group'] == True:
            n_graph += 1
        if self.__params['display_coverage'] == True:
            n_graph += 1

        #fig = plt.figure(figsize=(20, 90))
        if n_graph == 4:
            fig = plt.figure(figsize=(10, 13))
            ax1 = fig.add_subplot(411)
            ax2 = fig.add_subplot(412)
            ax3 = fig.add_subplot(413)
            ax4 = fig.add_subplot(414)
        elif n_graph == 3:
            fig = plt.figure(figsize=(10, 10))
            ax1 = fig.add_subplot(311)
            ax2 = fig.add_subplot(312)
            ax3 = fig.add_subplot(313)
        else:
            fig = plt.figure(figsize=(10, 7))
            ax1 = fig.add_subplot(211)
            ax2 = fig.add_subplot(212)


        for alpha in self.__factor_performance.keys():
            #figNew.plot(self.__factor_returns[alpha].cumsum(), label=alpha)
            ax1.plot(self.__factor_returns[alpha].cumsum(), label=alpha)
            ax1.set_title(f"factor_raw_return({alpha})")
            ax2.plot(self.__factor_net_returns[alpha].cumsum(), label='net ' + alpha)
            ax2.set_title(f"factor_net_return({alpha})")
        ax1.legend(fontsize=5,loc=2)
        ax2.legend(fontsize=5,loc=2)

        if self.__params['calcQT_group'] == True:
            group_cum_returns = self.__factor_returns_group.cumsum()
            for col_name in group_cum_returns.columns:
                ax3.plot(group_cum_returns[col_name], label=col_name)
            ax3.set_title('calcQT_group')
            ax3.legend(fontsize=5,loc=2)

        if self.__params['calcQT_group'] == True and self.__params['display_coverage'] == True:
            coverage = (~self.__factor_data.isna()).sum(axis=1)
            num_stock = (self.__factor_weights['alpha_QT']>0).sum(axis=1)
            ax4.plot(coverage, label='coverage')
            ax4.set_title("coverage")
            ax5 = ax4.twinx()
            ax5.plot(num_stock, color='r',  label='long stock holdings')
            ax5.set_title("num_stock")
            ax4.legend(loc=2)
            ax5.legend(loc=4)

        # if self.__params['calcQT_group'] == False and self.__params['display_coverage'] == True:
        #     coverage = (~self.__factor_data.isna()).sum(axis=1)
        #     num_stock = (self.__factor_weights['alpha_QT']>0).sum(axis=1)
        #     ax3.plot(coverage, label='coverage')
        #     ax4 = ax3.twinx()
        #     ax4.plot(num_stock,color='r', label='long stock holdings')
        #     ax3.legend(loc=2)
        #     ax4.legend(loc=4)

        if self.__params['create_pdf'] == True or self.__params['save_result'] == True:
            plt.savefig(self.__params['result_dir'] + "/" + self.__params['graph_file'] + self.__params['alpha_name'])
        if self.__params['display_graph'] == True:
            plt.show()

        plt.cla()
        plt.close("all")


    def __create_summary(self):
        if self.__simple_report == True:
            self.__create_simple_summary()
        else:
            self.__create_long_summary()

    def __create_simple_summary(self):
        self.__performance_summary = {}
        alpha = 'alpha_QT'
        if alpha in self.__factor_performance:
            years, perf_items, perf_data = self.__factor_performance[alpha]
            self.__performance_summary['Annual Return'] = perf_data['all']['Annualized Return']
            self.__performance_summary['IR'] = perf_data['all']['Annualized IR']
            self.__performance_summary['Max Drawdown'] = perf_data['all']['Max Drawdown']
            self.__performance_summary['Turnover'] = perf_data['all']['Turnover']
            self.__performance_summary['Net Annual Return'] = perf_data['all']['Annualized Return']

    def __create_long_summary(self):
        self.__performance_summary = {}
        self.__performance_summary['Alpha ID'] = self.__params['alpha_id']
        self.__performance_summary['Alpha Name'] = self.__params['alpha_name']
        self.__performance_summary['Alpha Type'] = self.__params['alpha_type']
        self.__performance_summary['Alpha Sub-Type'] = self.__params['alpha_sub_type']
        self.__performance_summary['Author'] = self.__params['author']
        self.__performance_summary['Alpha/Strategy'] = 'alpha'
        self.__performance_summary['Universe'] = self.__params['universe']
        self.__performance_summary['IS Start'] = self.__params['is_start_date']
        self.__performance_summary['IS End'] = self.__params['is_end_date']
        self.__performance_summary['OS Start'] = self.__params['os_start_date']
        self.__performance_summary['OS End'] = self.__params['os_end_date']
        self.__performance_summary['OS Days'] = self.__params['os_days']
        self.__performance_summary['Frequency'] = self.__params['trading_frequency']
        self.__performance_summary['Trading Cost'] = self.__params['trading_cost']
        alpha = 'alpha_QT'
        if alpha in self.__factor_performance:
            years, perf_items, perf_data = self.__factor_performance[alpha]
            self.__performance_summary['Annual Return'] = perf_data['all']['Annualized Return']
            self.__performance_summary['IS Annual Return'] = perf_data['is']['Annualized Return']
            self.__performance_summary['OS Annual Return'] = perf_data['os']['Annualized Return']
            self.__performance_summary['IR'] = perf_data['all']['Annualized IR']
            self.__performance_summary['IS IR'] = perf_data['is']['Annualized IR']
            self.__performance_summary['OS IR'] = perf_data['os']['Annualized IR']
            self.__performance_summary['Max Drawdown'] = perf_data['all']['Max Drawdown']
            self.__performance_summary['IS Max Drawdown'] = perf_data['is']['Max Drawdown']
            self.__performance_summary['OS Max Drawdown'] = perf_data['os']['Max Drawdown']
            self.__performance_summary['Turnover'] = perf_data['all']['Turnover']
            for y in years:
                if y != 'all' and y != 'is' and y != 'os':
                    self.__performance_summary[y + ' Return'] = perf_data[y]['Annualized Return']
            years, perf_items, perf_data = self.__factor_net_performance[alpha]
            self.__performance_summary['Net Annual Return'] = perf_data['all']['Annualized Return']
            self.__performance_summary['IS Net Annual Return'] = perf_data['is']['Annualized Return']
            self.__performance_summary['OS Net Annual Return'] = perf_data['os']['Annualized Return']

        alpha = 'alpha_QT_HS300'
        if alpha in self.__factor_performance:
            years, perf_items, perf_data = self.__factor_performance[alpha]
            self.__performance_summary['Excess Annual Return HS300'] = perf_data['all']['Annualized Return']

        alpha = 'alpha_QT_ZZ500'
        if alpha in self.__factor_performance:
            years, perf_items, perf_data = self.__factor_performance[alpha]
            self.__performance_summary['Excess Annual Return ZZ500'] = perf_data['all']['Annualized Return']




