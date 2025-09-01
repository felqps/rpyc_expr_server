import logging
import os,sys
from common_colors import *
import pandas as pd
import inspect

def logger_level(level):
    level_map = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }
    return level_map[level]

FDF_LOGGER = None
def logger():
    global FDF_LOGGER
    assert FDF_LOGGER is not None, f"ERROR: must initiate logger"
    return FDF_LOGGER
    

def init_logger(level="info", filename="", format=f"%(asctime)s;%(name)s;%(levelname)s: %(message)s", name=None, mode='w'):
    last_occurrence = format.rfind(";")
    if last_occurrence >= 0:
        format = f"{PURPLE}{format[:last_occurrence+1]}{NC}{format[last_occurrence+1:]}"

    global FDF_LOGGER
    if FDF_LOGGER is None:
        FDF_LOGGER = logging.getLogger(name)
        FDF_LOGGER.setLevel(logger_level(level))
        FDF_LOGGER.myformat = format

        if filename != "":
            handler = logging.FileHandler(filename, mode=mode)
            handler.setLevel(logger_level(level))
            handler.setFormatter(logging.Formatter(format))
            FDF_LOGGER.addHandler(handler)
        else:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logger_level(level))
            console_handler.setFormatter(logging.Formatter(format))
            FDF_LOGGER.addHandler(console_handler)
            #print(f"Logging handers: {logger().handlers}")
            # logging.basicConfig(level=logger_level(level), format=format)

def info(s):
    return logger().info(f"{NC}{s}{NC}")

def dbg(s):
    return logger().debug(f"{DBG}{s}{NC}")

def err(s):
    return logger().error(f"{ERR}{s}{NC}")

def wrn(s):
    return logger().error(f"{WRN}{s}{NC}")

def exception(err):
    return logger().exception(err)

def list2str(arr, rows=2):
    length = len(arr)
    if length<3*rows:
        return(f'{arr}')
    else:
        mid = int(length/2-1)
        return(f'{arr[:rows]} ... {arr[mid:mid+2]} ... {arr[-rows:]} [len {length}]')
    
def config_print_df():
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 500)
    pd.set_option('display.max_colwidth', 150)
    pd.set_option('display.max_rows', None)
    pd.options.display.float_format = '{:10.6f}'.format


def print_df(df, rows=3, cols=15, title='NA', debug=False, show_head=True, show_body=False, show_tail=True, loglevel='info', **kwargs):
    log_info_func = eval(f"logger().{loglevel}")

    info_str = "{log_info_func}"
    if type(df) == type(pd.DataFrame()):
        info_str = f"pd.DataFrame({df.shape})"
    elif type(df) == type(list()):
        info_str = f"list({len(df)})"
    elif type(df) == type(pd.Series()):
        info_str = f"pd.Series({df.size})"


    log_info_func(f'print_df Title({BLUE}{title}{NC}), {info_str})')
    fmt = logger().myformat
    global FDF_LOGGER
    for h in logger().handlers:
        h.setFormatter(logging.Formatter("%(message)s"))
    config_print_df()

    # logger().debug(f"{inspect.stack()[0]} {kwargs}")
    # logger().debug(f"{inspect.stack()[1]} {kwargs}")

    notEmpty = False

    if (type(df) == type(dict())) or (str(type(df)) == "<class 'collections.defaultdict'>"):
        #assert False, print(df)

        for (k,v) in df.items():
            logger().debug(f'====================, {type(v)}')
            if type(v) == type(dict()):
                log_info_func(f"print_df <dict>  {k}':' {v}")
                continue
            else:
                if v is not None:
                    if f"{type(v)}".find('datetime')>=0:
                        log_info_func(v)
                    else:
                        print_df(v, rows, title=f'{title} k={k}', debug=False, **kwargs)
                    notEmpty = True
        return notEmpty

    elif type(df) == type(None):
        pass

    elif type(df) == type(list()):
        length = len(df)
        if length < 2 * rows:
            log_info_func(f"{df}")
        else:
            if show_head:
                log_info_func(''.join(stringfy(df[:rows])))
                log_info_func('......')
            if show_body:
                log_info_func(''.join(stringfy(df[int((length-rows)/2):int((length+rows)/2)])))
                log_info_func('......')
            if show_tail:
                log_info_func(''.join(stringfy(df[-rows:])))

    elif str(type(df)) == "<class 'pandas.core.series.Series'>":
        length = df.size
        if show_head:
            log_info_func(f"{df.head(rows)}")
            log_info_func('......')
        if show_body:
            log_info_func(f"{df.iloc[int((length-rows)/2):int((length+rows)/2)]}")
            log_info_func('......')
        if show_tail:
            log_info_func(f"{df.tail(rows)}")

    elif str(type(df)) == "<class 'rqdatac.services.basic.Instrument'>" or \
        str(type(df)) == "<class 'pandas.core.indexes.datetimes.DatetimeIndex'>":
        log_info_func(f"{df}")
    else:
        #qps_print(df.shape)
        dfNoneNA = df.dropna(how='all', axis=1)
        length = dfNoneNA.shape[0]
        width = dfNoneNA.shape[1]
        # if width > cols:
        #     log_info_func(f'Truncating columns: {list2str(list(dfNoneNA.columns))}')
        #     dfNoneNA = dfNoneNA[dfNoneNA.columns[:cols]]
        
        if dfNoneNA.shape[0]>rows*3:
            if show_head:
                head = dfNoneNA.head(rows)
                head = head.dropna(how='all', axis=1)
                if head.shape[1]> cols:
                    head = head[head.columns[:cols]]
                log_info_func(f"head({title}:{rows}): {head}")
                log_info_func('......')
            if show_body:
                body = dfNoneNA.iloc[int((length-rows)/2):int((length+rows)/2)]
                body = body.dropna(how='all', axis=1)
                if body.shape[1]> cols:
                    body = body[body.columns[:cols]]
                log_info_func(f"body({title}:{rows}): {body}")
                log_info_func('......')
            if show_tail:
                tail = dfNoneNA.tail(rows)
                tail = tail.dropna(how='all', axis=1)
                if tail.shape[1]> cols:
                    tail = tail[tail.columns[:cols]]
                log_info_func(f"tail({title}:{rows}): {tail}")
        else:
            log_info_func(f"{dfNoneNA}")

    for h in logger().handlers:
        h.setFormatter(logging.Formatter(fmt))

    return True

def pretty_print_dict(d, title=""):
    pd.set_option('display.max_colwidth', None)
    #print({k:f"{v}" for k,v in d.items()})
    df = pd.DataFrame.from_dict({k:f"{v}" for k,v in d.items()}, orient='index')
    df.columns = [title]
    info(f"{df}")

def smart_print(ans, title="smart_print"):
    try:
        if type(ans) == type(pd.DataFrame()):
            print_df(ans.tail(5), title=title)
        elif type(ans) == type(dict()):
            for k,v in ans.items():
                print(f"INFO: {funcn} key= {k}; value=", smart_print(v))
        else:
            print(ans)
    except Exception as e:
        print(ans)

def dump_df_to_csv(title, df, fn):
    fn = fn.replace('xxx', title)
    print(f"{BROWN}INFO: {title} file://{fn}{NC}", file=sys.stdout)
    df.to_csv(fn, index_label="dtm")

if __name__ == '__main__':
    funcn = 'fdf_logging.main'
    from common_options_helper import *
    opt, args = get_options(funcn)

    logger().debug("info test")
    logger().info("info test")
    logger().warning("warning test")
    logger().error("error test")
    logger().critical("critical test")
