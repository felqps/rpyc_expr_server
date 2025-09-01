import pickle
import math
import pandas as pd
from typing import List, Sequence, Union
import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid
from pyecharts.commons.utils import JsCode
from pyecharts.globals import CurrentConfig, NotebookType
CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_NOTEBOOK

def gen_line_data(df:pd.DataFrame):
    df = pd.DataFrame(df)
    df = df.fillna(0)
    x_data = [pd.to_datetime(d).strftime("%Y%m%d %H%M%S") for d in list(df.index.values)]

    y_datas = df.to_dict("list")
    if False:
        print(x_data)
        print(y_datas)
    return x_data,y_datas

def gen_opts_pieces(x_data):
    cnt = 10
    step = math.floor(len(x_data)/cnt)
    pieces = []
    beg = 0
    end = step
    for i in range(cnt+1)[1:]:
        if i == cnt:
            end = len(x_data)
        pieces.append({"gt": beg, "lte": end, "color":"green" if i%2 == 0 else "red"})
        beg += step
        end += step
    
    return pieces

def plot_line(df, title="Line", figsize=(18, 8), grid=False, linewidth=1.5):
    x_data,y_datas = gen_line_data(df)
    ln = Line()
    ln.add_xaxis(xaxis_data=x_data)
    for k,y_data in y_datas.items():
        ln.add_yaxis(
            series_name=k,
            y_axis=y_data,
            # is_smooth=True,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=linewidth),
            # linestyle_opts=opts.LineStyleOpts(width=linewidth, color="#0099ff"),
        )

    ln.set_global_opts(
        title_opts=opts.TitleOpts(title=title, subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        xaxis_opts=opts.AxisOpts(boundary_gap=False),
        yaxis_opts=opts.AxisOpts(
            axislabel_opts=opts.LabelOpts(formatter="{value}"),
            splitline_opts=opts.SplitLineOpts(is_show=grid),
            min_="dataMin",
            max_="dataMax",
        ),
        # visualmap_opts=opts.VisualMapOpts(
        #     is_piecewise=True,
        #     dimension=0,
        #     pieces=gen_opts_pieces(x_data),
        # ),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=False, type_="inside", xaxis_index=[0, 0], range_start=0, range_end=100
            ),
            opts.DataZoomOpts(
                is_show=True, xaxis_index=[0, 1], range_start=0, range_end=100
            ),
        ]
    )
    # ln.set_series_opts(
    #     markarea_opts=opts.MarkAreaOpts(
    #         data=[
    #             opts.MarkAreaItem(name="早高峰", y=(200, 300)),
    #             opts.MarkAreaItem(name="晚高峰", x=("17:30", "21:15")),
    #         ]
    #     )
    # )
    grid_chart = Grid(init_opts=opts.InitOpts(width=f'{figsize[0]*50}px', height=f"{figsize[1]*50}px"))

    grid_chart.add(ln, grid_opts=opts.GridOpts(pos_left="8%", pos_right="1%"))
    grid_chart.add(Line(), grid_opts=opts.GridOpts(pos_left="8%", pos_right="1%"))

    return grid_chart

def gen_kline_data(df:pd.DataFrame):
    data = {}
    df = pd.DataFrame(df)
    df = df.fillna(0)
    data["times"] = [pd.to_datetime(d).strftime("%Y%m%d %H%M%S") for d in list(df.index.values)]

    df_dict = df.to_dict("split")
    data["datas"] = filter_column_ohlcv(df_dict)
    data["vols"] = [data[4] for data in data["datas"]]
    return data

def filter_column_ohlcv(df_dict):
    colums = df_dict["columns"]
    ids = {name:i for i, name in enumerate(colums)}

    data = df_dict["data"]
    r_data = []
    for d in data:
        r_line = []
        for n in ["open", "close", "low", "high", "volume"]:
            r_line.append(d[ids[n]])
        r_data.append(r_line)
    return r_data

def calculate_ma(data, day_count: int):
    result: List[Union[float, str]] = []

    for i in range(len(data["times"])):
        if i < day_count:
            result.append("-")
            continue
        sum_total = 0.0
        for j in range(day_count):
            sum_total += float(data["datas"][i - j][1])
        result.append(abs(float("%.2f" % (sum_total / day_count))))
    return result


def split_data_part(data) -> Sequence:
    mark_line_data = []
    idx = 0
    tag = 0
    vols = 0
    for i in range(len(data["times"])):
        if data["datas"][i][5] != 0 and tag == 0:
            idx = i
            vols = data["datas"][i][4]
            tag = 1
        if tag == 1:
            vols += data["datas"][i][4]
        if data["datas"][i][5] != 0 or tag == 1:
            mark_line_data.append(
                [
                    { 
                        "xAxis": idx,
                        "yAxis": float("%.2f" % data["datas"][idx][3])
                        if data["datas"][idx][1] > data["datas"][idx][0]
                        else float("%.2f" % data["datas"][idx][2]),
                        "value": vols,
                    },
                    {
                        "xAxis": i,
                        "yAxis": float("%.2f" % data["datas"][i][3])
                        if data["datas"][i][1] > data["datas"][i][0]
                        else float("%.2f" % data["datas"][i][2]),
                    },
                ]
            )
            idx = i
            vols = data["datas"][i][4]
            tag = 2
        if tag == 2:
            vols += data["datas"][i][4]
        if data["datas"][i][5] != 0 and tag == 2:
            mark_line_data.append(
                [
                    {
                        "xAxis": idx,
                        "yAxis": float("%.2f" % data["datas"][idx][3])
                        if data["datas"][i][1] > data["datas"][i][0]
                        else float("%.2f" % data["datas"][i][2]),
                        "value": str(float("%.2f" % (vols / (i - idx + 1)))) + " M",
                    },
                    {
                        "xAxis": i,
                        "yAxis": float("%.2f" % data["datas"][i][3])
                        if data["datas"][i][1] > data["datas"][i][0]
                        else float("%.2f" % data["datas"][i][2]),
                    },
                ]
            )
            idx = i
            vols = data["datas"][i][4]
    return mark_line_data


def plot_kline(df, title="KLine", figsize=(30, 20), linewidth=1.5):
    data = gen_kline_data(df)
    kline = (
        Kline()
        .add_xaxis(xaxis_data=data["times"])
        .add_yaxis(
            series_name="",
            y_axis=data["datas"],
            itemstyle_opts=opts.ItemStyleOpts(
                color="#ef232a",
                color0="#14b143",
                border_color="#ef232a",
                border_color0="#14b143",
            ),
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(type_="max", name="最大值"),
                    opts.MarkPointItem(type_="min", name="最小值"),
                ]
            ),
            # markline_opts=opts.MarkLineOpts(
            #     label_opts=opts.LabelOpts(
            #         position="middle", color="blue", font_size=15
            #     ),
            #     data=split_data_part(data),
            #     symbol=["circle", "none"],
            # ),
        )
        # .set_series_opts(
        #     markarea_opts=opts.MarkAreaOpts(is_silent=True, data=split_data_part(data))
        # )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title, pos_left="0"),
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True, splitline_opts=opts.SplitLineOpts(is_show=True)
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="line"),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=False, type_="inside", xaxis_index=[0, 0], range_start=0, range_end=100
                ),
                opts.DataZoomOpts(
                    is_show=True, xaxis_index=[0, 1], pos_top="94%", range_start=0, range_end=100
                ),
            ],
        )
    )

    kline_line = (
        Line()
        .add_xaxis(xaxis_data=data["times"])
        .add_yaxis(
            series_name="MA5",
            y_axis=calculate_ma(data, day_count=5),
            is_smooth=True,
            linestyle_opts=opts.LineStyleOpts(opacity=0.5),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                grid_index=1,
                axislabel_opts=opts.LabelOpts(is_show=False),
            ),
            yaxis_opts=opts.AxisOpts(
                grid_index=1,
                split_number=3,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                axislabel_opts=opts.LabelOpts(is_show=True),
            ),
        )
    )
    overlap_kline_line = kline.overlap(kline_line)

    bar_vol = (
        Bar()
        .add_xaxis(xaxis_data=data["times"])
        .add_yaxis(
            series_name="Volumn",
            y_axis=data["vols"],
            xaxis_index=1,
            yaxis_index=1,
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(
                color=JsCode(
                    """
                function(params) {
                    var colorList;
                    if (barData[params.dataIndex][1] > barData[params.dataIndex][0]) {
                        colorList = '#ef232a';
                    } else {
                        colorList = '#14b143';
                    }
                    return colorList;
                }
                """
                )
            ),
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                grid_index=1,
                axislabel_opts=opts.LabelOpts(is_show=False),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )


    grid_chart = Grid(init_opts=opts.InitOpts(width=f'{figsize[0]*50}px', height=f"{figsize[1]*50}px"))

    grid_chart.add_js_funcs("var barData = {}".format(data["datas"]))

    grid_chart.add(
        overlap_kline_line,
        grid_opts=opts.GridOpts(pos_left="8%", pos_right="1%", height="60%"),
    )

    grid_chart.add(
        bar_vol,
        grid_opts=opts.GridOpts(pos_left="8%", pos_right="1%", pos_top=f"85%", height="8%"),
    )

    return grid_chart

if __name__ == '__main__':
    if False:
        fn = f"{dd('usr')}/CS/SN_CS_DAY/prod1w_20210810_20211111/1d/Pred_OHLCV_C_1d_1/C26.db"
        # fn = "/temp/test_data_20211116/C38/ohlcv_1d_pre.db"
        df = pickle.load(open(fn, "rb"))["300437.XSHE"]
        ln = plot_line(df, "test", (18, 6), False, 1.5)
        ln.render("/Fairedge_dev/app_QpsData/html/professional_line_chart.html")
    else:
        fn = f"{dd('usr')}/CS/SN_CS_DAY/prod1w_20210810_20211115/1d/Pred_ohlcv_pre_1d_1/600031.XSHG.db"
        df = pickle.load(open(fn, "rb"))["600031.XSHG"]
        grid_chart = plot_kline(df, "test", (30, 20), 1.5)
        grid_chart.render("/Fairedge_dev/app_QpsData/html/professional_kline_chart.html")

