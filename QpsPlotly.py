import sys,os
import pickle
import math
import glob
import datetime
from optparse import OptionParser
import pandas as pd
import plotly.express as px
import plotly.offline as py
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from pathlib import Path
from InstruCommon import generator_vps_instru_common
from QpsNotebook import *
from QpsLogging import logging
from qdb_options import get_options_plotly

# py.init_notebook_mode(connected=False)

def gen_line_data(df:pd.DataFrame, label):
    rt_df = None
    if type(df) == type(pd.Series()):
        t_df = pd.DataFrame({"x": df.index, "y": df.values, "symbol": label})
        rt_df = pd.concat([rt_df,t_df], axis=0, ignore_index=True)
    else:
        for column in df:
            t_df = df[column]
            t_df = pd.DataFrame({"x": t_df.index, "y": t_df.values, "symbol": column})
            rt_df = pd.concat([rt_df,t_df], axis=0, ignore_index=True)
    return rt_df

def px_line(df, title="Line", label="default", figsize=(18, 8), png_path="", html_path="", notes=""):
    # print(df)
    df = gen_line_data(df, label)
    fig = px.line(
        df,
        x="x",
        y='y',
        title=title,
        color="symbol",
        # markers=True,
        width=figsize[0]*60,
        height=figsize[1]*60,
        symbol="symbol",
        line_shape="linear",  #linear,spline
    )

    fig.update_traces(
        marker = {
            "size": 3,
        },
        line = {
            "width": 1.5
        }
    )

    fig.update_layout(
        showlegend = True,
        legend = {
            "orientation": "h",
            "title": ""
        },
        xaxis={
            "nticks": 12,
        },
        xaxis_title=format_xaxis_title(figsize, notes),
        xaxis_title_standoff=30,
        yaxis_title="",
    )
    if png_path:
        fig.write_image(png_path, engine="kaleido")
    if html_path:
        fig.write_html(html_path)
    fig.show()

def format_xaxis_title(figsize, text):
    l = figsize[0]*7
    arr = [text[i:i+l] for i in range(0, len(text), l)]
    # if len(arr) > 1:
    #     arr[-1] = arr[-1] + "\t"*(len(arr[-2]) - len(arr[-1]))
    #     print(len(arr[-1]),len(arr[-2]), len(arr[-3]))
    return "<br>".join(arr)

def px_subplot_line(dfs, title="Line", cols = 3):
    if len(dfs) < 1:
        print("ERROR: No data to plot.")
        return

    subplot_titles = (f"{k}" for k in dfs)
    rows = int(len(dfs)/cols) + 1
    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=tuple(subplot_titles),
        # vertical_spacing=0.03,
        vertical_spacing=0.6/len(dfs),
    )

    for i,v in enumerate(dfs.values()):
        for col_name in v:
            fig.add_trace(
                go.Scatter(
                    x = v[col_name].keys(), #[1, 2, 3],
                    y = v[col_name], #[4, 5, 6],
                    # x = [1, 2, 3],
                    # y = [4, 5, 6],
                    showlegend=False,
                    # mode='lines+markers',
                    marker={
                        "size": 3,
                    },
                    line = {
                        "width": 1.5
                    }
                ),
                row=int(i/cols)+1,
                col=i%cols + 1,
            )

    width = 1800
    heght = 300 * rows
    fig.update_layout(height=heght, width=width, title_text=title)
    fig.show()


class PxUtil():
    def __init__(self, options):
        self.opt = options
        self.all_instru = self.load_all_instru()
        # print(list(self.all_instru.values())[0])

    def load_all_instru(self):
        all_instru = {}
        if self.opt.sectype == "CS":
            symset='CS_ALL'
        else:
            symset='CF_ALL'
        instrus = generator_vps_instru_common(symset=symset)
        for i in instrus:
            all_instru[i.permid.split(".")[0] if self.opt.sectype == "CS" else i.metaId] = i

        return all_instru

    def get_category(self, sym):
        return "CF_ALL" if self.opt.sectype != "CS" else self.all_instru.get(sym).category

    def get_permid(self, sym):
        return sym if self.opt.sectype != "CS" else self.all_instru.get(sym).permid

    def get_volume_data(self, dir_name, sym):
        volume_data = {}
        for bar in ["1d"]:#, "5m", "1m"]:
            try:
                fn = glob.glob(f"{dir_name}/{bar}/*OHLCV_V_*/{self.get_category(sym)}.db")[0]
                data = pd.DataFrame()
                if os.path.exists(fn):
                    data = pd.DataFrame(pickle.load(open(fn, "rb")))
                volume_data[bar] = data
            except Exception as err:
                logging.exception(err)
                logging.error(f"No such file: {fn}")
                volume_data[bar] = pd.DataFrame()
        return volume_data["1d"]

    def plot_flds(self, sym: str, filter: [], bar="??", root_dir="/qpsuser/che/data_rq.20210810_uptodate", figsize=figsizeHalf, subplot=True):
        if type(filter) == str:
            filter = [filter]

        flds = []
        permid = self.get_permid(sym)
        dir_name = sorted(glob.glob(f"{root_dir}/{self.opt.sectype}/{self.opt.session}/prod1w_????????_????????"))[-1]

        if self.opt.sectype == "CS":
            volume_data = self.get_volume_data(dir_name, sym)
            volume_data = volume_data[permid][volume_data[permid] == 0]
        else:
            volume_data = data = pd.DataFrame()
        # print(volume_data)

        for ftr in filter:
            print((f"{dir_name}/{bar}/*{ftr}*/{self.get_category(sym)}.db"))
            flds.extend(sorted(glob.glob(f"{dir_name}/{bar}/*{ftr}*/{self.get_category(sym)}.db")))

        print(f"Find all flds:")
        print('\n'.join(flds))

        flds_data = {}
        for fld in sorted(flds):
            bar = fld.split("/")[-3]
            fld_data = pd.DataFrame(pickle.load(open(fld, "rb"))[permid])
            if len(volume_data) > 0 and "OHLCV_V_" not in fld:
                for dt in volume_data.index.values:
                    fld_data[permid][pd.to_datetime(str(dt)).strftime("%Y%m%d")] = None

            # print(fld_data)
            flds_data[os.path.basename(os.path.dirname(fld))] = fld_data
        if subplot:
            title = str(self.all_instru.get(sym)).replace("VpsInstru[", "").replace("]", "").replace(";", ";   ")
            px_subplot_line(flds_data, title)
        else:
            for k,v in flds_data.items():
                px_line(v, label=k, title=k, figsize=figsize)

def list_cmds(opt):
    cmds = [
        "python /Fairedge_dev/app_QpsData/QpsPlotly.py --sym 000957 --filter OHLCV --session SN_CS_DAY --sectype CS",
        "python /Fairedge_dev/app_QpsData/QpsPlotly.py --sym A --filter OHLCV --session SN_CF_DAY --sectype CF",
    ]
    print('\n'.join(cmds))


if __name__ == '__main__':
    opt, args = get_options_plotly(list_cmds)
    if len(args) != 0:
        input_files = []
        for x in args:
            input_files.extend(x.split(","))
        plot_data = pd.DataFrame()
        for fn in input_files:
            df = pickle.load(open(fn, "rb"))
            plot_data = pd.concat([plot_data, df])
        plot_data = plot_data.fillna(0)
        px_line(plot_data, title="Line", figsize=(30, 20))
    else:
        PxUtil(opt).plot_flds(opt.sym, opt.filter, subplot=True)


    if 0:
        df = pickle.load(open("/qpsuser/che/data_rq.20100101_uptodate/CS/SN_CS_DAY/prod1w_20210810_20211202/1d/Pred_ohlcv_pre_1d_1/688117.XSHG.db", "rb")).get("688117.XSHG")
        df = df[["open", "high", "low", "close"]]
        px_line(df, title="Line", figsize=(18, 8))

    if 0:
        pd.set_option('display.max_columns',None)
        pd.set_option('display.max_rows',None)
        PxUtil(opt).plot_flds("000957", "OHLCV_", subplot=False)

    if 0:
        PxUtil(opt).plot_flds("000957", "OHLCV", subplot=True)
