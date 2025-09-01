#!/home/shuser/anaconda3/bin/python

import sys

from optparse import OptionParser

import QpsUtil
import datetime
from common_colors import *
from common_ctx import *
from common_basic import *

@deprecated_disabled
def set_options_common(parser):
    parser.add_option("-F", 
                    "--force",
                    dest="force",
                    type="int",
                    help="force (default: %default)",
                    metavar="force",
                    default=0)

    parser.add_option("-D",
                    "--debug",
                    dest="debug",
                    type="int",
                    help="debug (default: %default)",
                    metavar="debug",
                    default=0)

    parser.add_option("-V",
                    "--verbose",
                    dest="verbose",
                    type="int",
                    help="verbose (default: %default)",
                    metavar="verbose",
                    default=0)

    parser.add_option("-S",
                    "--status_file",
                    dest="status_file",
                    type="str",
                    help="status_file (default: %default)",
                    metavar="status_file",
                    default='NA')

    parser.add_option("--realtime",
                    dest="realtime",
                    type="int",
                    help="realtime (default: %default)",
                    metavar="realtime",
                    default=0)

    parser.add_option("--dryrun",
                    dest="dryrun",
                    type="int",
                    help="dryrun (default: %default)",
                    metavar="dryrun",
                    default=0)

    parser.add_option("--install",
                    dest="install",
                    type="int",
                    help="install (default: %default)",
                    metavar="install",
                    default=0)

    parser.add_option("--quiet",
                    dest="quiet",
                    type="int",
                    help="quiet (default: %default)",
                    metavar="quiet",
                    default=0)

    parser.add_option("--batchno",
                    dest="batchno",
                    type="int",
                    help="batchno (default: %default)",
                    metavar="batchno",
                    default=-1)

    parser.add_option("--list_cmds",
                    dest="list_cmds",
                    help="list_cmds (default: %default)",
                    metavar="list_cmds",
                    default="")

    parser.add_option("--list_modules",
                    dest="list_modules",
                    help="list_modules (default: %default)",
                    metavar="list_modules",
                    default="")
                
    parser.add_option("--logs",
                    dest="logs",
                    help="logs (default: %default)",
                    metavar="logs",
                    default="logs")

    parser.add_option("--regtest",
                    dest="regtest",
                    help="regtest (default: %default)",
                    metavar="regtest",
                    default="")

    parser.add_option("--what",
                    dest="what",
                    help="what (default: %default)",
                    metavar="what",
                    default="")

    parser.add_option("--axis",
                    dest="axis",
                    type="str",
                    help="axis (default: %default)",
                    metavar="axis",
                    default='column')

    parser.add_option("--lazy",
                    dest="lazy",
                    type="int",
                    help="lazy (default: %default)",
                    metavar="lazy",
                    default=1)

    parser.add_option("--fld_cache",
                    dest="fld_cache",
                    type="int",
                    help="fld_cache (default: %default)",
                    metavar="fld_cache",
                    default=1)

    parser.add_option("--do",
                    dest="do",
                    help="do (default: %default)",
                    metavar="do",
                    default="NA")

    parser.add_option("--bash",
                    dest="bash",
                    type="str",
                    help="bash (default: %default)",
                    metavar="bash",
                    default="")

    parser.add_option("--resp",
                    dest = "resp",
                    type = "str",
                    help = "resp (default: %default)",
                    metavar = "resp",
                    default = "Resp_C2C")

    parser.add_option("-c",
                    "--dts_cfg",
                    dest="dts_cfg",
                    help="dts_cfg (default: %default)",
                    metavar="dts_cfg",
                    default="NA")
                    #default="fullhist")
                    
    parser.add_option("--symset",
                    dest="symset",
                    help="symset (default: %default)",
                    metavar="symset",
                    default='NA')
                    #default='CF_TEST03,CF_TEST02,NS_LARGE,CF_AGRI,EQTY,CF_MKTS') 

    parser.add_option("--univ",
                    dest = "univ",
                    type = "str",
                    help = "--univ (default: %default)",
                    metavar = "--univ",
                    default = 'Full')
                    #default = 'TOP1800')

    parser.add_option("--pred",
                    dest = "pred",
                    type = "str",
                    help = "pred (default: %default)",
                    metavar = "pred",
                    default = "all")

    parser.add_option("--resp_tag",
                    dest = "resp_tag",
                    type = "str",
                    help = "resp_tag (default: %default)",
                    metavar = "resp_tag",
                    default = "all")

    parser.add_option("--pred_tag",
                    dest = "pred_tag",
                    type = "str",
                    help = "pred_tag (default: %default)",
                    metavar = "pred_tag",
                    default = "all")
                
    parser.add_option("--comment",
                    dest = "comment",
                    type = "str",
                    help = "comment (default: %default)",
                    metavar = "comment",
                    default = "NA")

    parser.add_option("--priority",
                    dest = "priority",
                    type = "str",
                    help = "priority (default: %default)",
                    metavar = "priority",
                    default = "Z")

    parser.add_option("--study_name",
                    dest = "study_name",
                    type = "str",
                    help = "study_name (default: %default)",
                    metavar = "study_name",
                    default = "NA")
                
    parser.add_option("--script_name",
                    dest = "script_name",
                    type = "str",
                    help = "script_name (default: %default)",
                    metavar = "script_name",
                    default = "NA")

    parser.add_option("--jupyter_port",
                    dest = "jupyter_port",
                    type = "str",
                    help = "jupyter_port (default: %default)",
                    metavar = "jupyter_port",
                    default = "8888")
                
    parser.add_option("--tmpl",
                    dest="tmpl",
                    type="str",
                    help="tmpl (default: %default)",
                    metavar="tmpl",
                    default="cx")

    parser.add_option("--outfn",
                    dest="outfn",
                    type="str",
                    help="outfn (default: %default)",
                    metavar="outfn",
                    default="")

    parser.add_option("--remove_unspecified_formula",
                    dest="remove_unspecified_formula",
                    type="int",
                    help="remove_unspecified_formula (default: %default)",
                    metavar="remove_unspecified_formula",
                    default=0)
    
    parser.add_option("--formula_search_ver",
                    dest="formula_search_ver",
                    type="str",
                    help="formula_search_ver (default: %default)",
                    metavar="formula_search_ver",
                    default="formula_search_05")

    parser.add_option("--formula_build_ver",
                    dest="formula_build_ver",
                    type="str",
                    help="formula_build_ver (default: %default)",
                    metavar="formula_build_ver",
                    default="formula_builder_05")

    parser.add_option("--select_predictors_ver",
                    dest="select_predictors_ver",
                    type="str",
                    help="select_predictors_ver (default: %default)",
                    metavar="select_predictors_ver",
                    default="select_predictors_05")

    parser.add_option("--scn",
                    dest = "scn",
                    type = "str",
                    help = "scn (default: %default)",
                    metavar = "scn",
                    default = "W")

    parser.add_option("--now",
                    dest = "now",
                    type = "str",
                    help = "now (default: %default)",
                    metavar = "now",
                    default = f"{datetime.datetime.today().date()}".replace('-',''))
                    
    parser.add_option("--grep",
                    dest = "grep",
                    type = "str",
                    help = "grep (default: %default)",
                    metavar = "grep",
                    default = ".*")

    parser.add_option("--outdir",
                    dest = "outdir",
                    type = "str",
                    help = "outdir (default: %default)",
                    metavar = "outdir",
                    default = "")
        
    parser.add_option("--bookid",
                    dest="bookid",
                    type="str",
                    help="bookid (default: %default)",
                    metavar="bookid",
                    default="strat001")

    parser.add_option("--stratid",
                    dest = "stratid",
                    type = "str",
                    help = "stratid (default: %default)",
                    metavar = "stratid",
                    default = "STR_strat001")
            
    parser.add_option("--symset_filter",
                    dest="symset_filter",
                    type="str",
                    help="symset_filter (default: %default)",
                    metavar="symset_filter",
                    default="")

    parser.add_option("--undo",
                    dest="undo",
                    type="int",
                    help="undo (default: %default)",
                    metavar="undo",
                    default=0)

    parser.add_option("--formula_search_batch_size",
                    dest="formula_search_batch_size",
                    type="int",
                    help="formula_search_batch_size (default: %default)",
                    metavar="formula_search_batch_size",
                    default=1)

    parser.add_option("--detached_data_root",
                    dest = "detached_data_root",
                    type = "str",
                    help = "detached_data_root (default: %default)",
                    metavar = "detached_data_root",
                    default = "NA")

    parser.add_option("--addr_cfg",
                    dest = "addr_cfg",
                    type = "str",
                    help = "addr_cfg (default: %default)",
                    metavar = "addr_cfg",
                    default = f"{rootdata()}/config/FCScheduler.cfg")
    
    parser.add_option("--cloud",
                    dest = "cloud",
                    type = "str",
                    help = "cloud (default: %default)",
                    metavar = "cloud",
                    default = f"dealer")

    parser.add_option("--localize",
                    dest="localize",
                    type="str",
                    help="localize (default: %default)",
                    metavar="localize",
                    default="cn")
    
    parser.add_option("--minor_threshold",
                    dest="minor_threshold",
                    type="int",
                    help="minor_threshold (default: %default)",
                    metavar="minor_threshold",
                    default=100)

    
    return parser
@deprecated_disabled
def set_options_graphengine(parser):
    parser.add_option("--vps_cfg",
                    dest = "vps_cfg",
                    type = "str",
                    help = "vps_cfg (default: %default)",
                    metavar = "vps_cfg",
                    default = "dev-axiong")
    
    parser.add_option("--domain",
                    dest="domain",
                    type="str",
                    help="domain (default: %default)",
                    metavar="domain",
                    default="",)

    return parser
@deprecated_disabled
def set_options_jobgraph(parser):
    parser.add_option("--qdb_cfg",
                    dest="qdb_cfg",
                    help="qdb_cfg (default: %default)",
                    metavar="qdb_cfg",
                    default="all")
    
    parser.add_option("--cfg", #backward compatible support, replaced by 'qdb_cfg' 
                    dest="qdb_cfg",
                    help="qdb_cfg (default: %default)",
                    metavar="qdb_cfg",
                    default="all")

    parser.add_option("-a",
                    "--asofdate",
                    dest="asofdate",
                    help="asofdate (default: %default)",
                    metavar="asofdate",
                    default="uptodate")

    parser.add_option("--prevdate",
                    dest="prevdate",
                    help="prevdate (default: %default)",
                    metavar="prevdate",
                    default="")
    
    parser.add_option("--pre2date",
                    dest="pre2date",
                    help="pre2date (default: %default)",
                    metavar="pre2date",
                    default="")

    parser.add_option("--tradedate",
                    dest="tradedate",
                    help="tradedate (default: %default)",
                    metavar="tradedate",
                    default="")

    parser.add_option("--nextdate",
                    dest="nextdate",
                    help="nextdate (default: %default)",
                    metavar="nextdate",
                    default="")

    parser.add_option("-s",
                    "--session",
                    dest="session",
                    help="session (default: %default)",
                    metavar="session",
                    default='SN_CF_DNShort')
                    
    parser.add_option("--fld",
                    dest="fld",
                    help="fld (default: %default)",
                    metavar="fld",
                    default='ALL')

    parser.add_option("--step",
                    dest="step",
                    help="step (default: %default)",
                    metavar="step",
                    default='ALL')

    parser.add_option("--sectype",
                    dest="sectype",
                    help="sectype (default: %default)",
                    metavar="sectype",
                    default='CF')  

    parser.add_option("--secfilter",
                    dest="secfilter",
                    help="secfilter (default: %default)",
                    metavar="secfilter",
                    default='active')   

    parser.add_option("--bar",
                    dest="bar",
                    help="bar (default: %default)",
                    metavar="bar",
                    default='1d')  

    parser.add_option("--hhmm",
                    dest="hhmm",
                    help="hhmm (default: %default)",
                    metavar="hhmm",
                    default='')  

    parser.add_option("--calc",
                    dest="calc",
                    help="calc (default: %default)",
                    metavar="calc",
                    default='grpret')

    parser.add_option("--fp",
                    dest="fp",
                    help="fp (default: %default)",
                    metavar="fp",
                    default='')

    parser.add_option("--mktgt",
                    dest="mktgt",
                    help="mktgt (default: %default)",
                    metavar="mktgt",
                    default='')

    parser.add_option("--mklvl",
                    dest="mklvl",
                    help="mklvl (default: %default)",
                    metavar="mklvl",
                    default='5')

    parser.add_option("--draw",
                    dest="draw",
                    type="int",
                    help="draw (default: %default)",
                    metavar="draw",
                    default=0)
        
    parser.add_option("--instru_force", #force generate vps_instru db files
                    dest="instru_force",
                    type="int",
                    help="instru_force (default: %default)",
                    metavar="instru_force",
                    default=0)

    parser.add_option("--parallel",
                    dest="parallel",
                    type="int",
                    help="parallel (default: %default)",
                    metavar="parallel",
                    default=3) #use 4 cores

    parser.add_option("--gen1m",
                    dest="gen1m",
                    type="int",
                    help="gen1m (default: %default)",
                    metavar="gen1m",
                    default=1) 

    parser.add_option("--thread_parallel",
                    dest="thread_parallel",
                    type="int",
                    help="use thread parallel instead process parallel (default: %default)",
                    metavar="thread_parallel",
                    default=2) #use 4 cores

    parser.add_option("--download_tick", #force generate vps_instru db files
                    dest="download_tick",
                    type="int",
                    help="download_tick (default: %default)",
                    metavar="download_tick",
                    default=1)
@deprecated_disabled
def set_options_sgen(parser):
    # parser.add_option("--scn",
    #                   dest = "scn",
    #                   type = "str",
    #                   help = "scn (default: %default)",
    #                   metavar = "scn",
    #                   default = "W")

    parser.add_option("--pool_size",
                    dest="pool_size",
                    type= "int",
                    help="pool_size (default: %default)",
                    metavar="pool_size",
                    default=3)

    parser.add_option("--outsample",
        dest = "outsample",
        type = "int",
        help = "--outsample (default: %default)",
        metavar = "--outsample",
        default = 0)

    parser.add_option("--plt_show",
        dest = "plt_show",
        type = "int",
        help = "--plt_show (default: %default)",
        metavar = "--plt_show",
        default = 0)

    return parser
@deprecated_disabled
def set_options_qddownload(parser):
    parser.add_option(
        "--src",
        dest="src",
        help="src (default: %default)",
        metavar="src",
        default = "NA"
    )
    
    parser.add_option(
        "--tgt",
        dest="tgt",
        help="tgt (default: %default)",
        metavar="tgt",
        default="NA",
    )

    parser.add_option(
        "--sectype",
        dest="sectype",
        help="sectype (default: %default)",
        metavar="sectype",
        default="futures",
    )

    parser.add_option(
        "--file_filter",
        dest="file_filter",
        help="file_filter (default: %default)",
        metavar="file_filter",
        default=".db",
    )

    parser.add_option(
        "--asofdate",
        dest="asofdate",
        type="str",
        help="asofdate",
        metavar="asofdate",
        default=QpsUtil.getexchdate())

    parser.add_option(
        "--pool_size",
        dest="pool_size",
        type= "int",
        help="pool_size (default: %default)",
        metavar="pool_size",
        default=4,
    )

    parser.add_option(
        "--level",
        dest="level",
        type = "str",
        help="level (default: %default)",
        metavar="level",
        default='daily',
    )   

    parser.add_option(
        "--last_n_updates",
        dest="last_n_updates",
        type = "int",
        help="last_n_updates (default: %default)",
        metavar="last_n_updates",
        default=5,
        #default=0,
    )

    return parser
@deprecated_disabled
def set_options_qdsplit(parser):
    parser.add_option(
        "--src",
        dest="src",
        help="src (default: %default)",
        metavar="src",
        default=f"{rootdata()}/data_rq.20100101_uptodate",
    )
    parser.add_option(
        "--tgt",
        dest="tgt",
        help="tgt (default: %default)",
        metavar="tgt",
        default=f"{rootdata()}/data_rq.20100101_uptodate.test",
    )
    parser.add_option(
        "--split_date",
        dest="split_date",
        help="split_date (default: %default)",
        metavar="split_date",
        default="20210801", #example: "20210801 20220217" or "20210801"
    )
    parser.add_option(
        "--pool_size",
        dest="pool_size",
        type= "int",
        help="pool_size (default: %default)",
        metavar="pool_size",
        default=4,
    )
    parser.add_option(
        "--file_filter",
        dest="file_filter",
        help="file_filter (default: %default)",
        metavar="file_filter",
        default="",
    )
@deprecated_disabled
def set_options_fcclient(parser): #FactorCalculator
    # parser.add_option("--addr_cfg",
    #                 dest = "addr_cfg",
    #                 type = "str",
    #                 help = "addr_cfg (default: %default)",
    #                 metavar = "addr_cfg",
    #                 default = f"{rootdata()}/config/FCScheduler.cfg")
    pass
@deprecated_disabled
def set_options_influxdb(parser): 
    parser.add_option("--influx_cfg",
                    dest = "influx_cfg",
                    type = "str",
                    help = "influx_cfg (default: %default)",
                    metavar = "influx_cfg",
                    default = f"{rootdata()}/config/Influxdb.cfg")

    parser.add_option("--threshold",
                    dest="threshold",
                    type="float",
                    help="threshold (default: %default)",
                    metavar="threshold",
                    default=0.00)

    parser.add_option("--resetdt",
                    dest = "resetdt",
                    type = "str",
                    help = "resetdt (default: %default)",
                    metavar = "resetdt",
                    default = "")    
@deprecated_disabled
def set_options_plotly(parser): 
    parser.add_option("--sym",
                    dest = "sym",
                    type = "str",
                    help = "sym (default: %default)",
                    metavar = "sym",
                    default = "000957")

    parser.add_option("--filter",
                    dest="filter",
                    type="str",
                    help="filter (default: %default)",
                    metavar="filter",
                    default="OHLCV")
@deprecated_disabled
def set_options_mysql(parser):
    parser.add_option(
        "--host",
        dest="host",
        type="str",
        help="host (default: %default)",
        metavar="host",
        default="192.168.20.98",
    )
    parser.add_option(
        "--port",
        dest="port",
        type="int",
        help="port (default: %default)",
        metavar="port",
        default=3306,
    )
    parser.add_option(
        "--user",
        dest="user",
        type="str",
        help="user (default: %default)",
        metavar="user",
        default="root",
    )
    parser.add_option(
        "--passwd",
        dest="passwd",
        type="str",
        help="passwd (default: %default)",
        metavar="passwd",
        default="Sh654321",
    )
    parser.add_option(
        "--db",
        dest="db",
        type="str",
        help="db (default: %default)",
        metavar="db",
        default="db_cn",
    )
@deprecated_disabled
def set_options_factsum(parser): 
    # if ctx_debug(5):
    #     print("DEBUG_2321: customize_factsum_options", file=sys.stderr)
    parser.add_option("--wgt_riskparity",
        dest = "wgt_riskparity",
        type = "int",
        help = "--wgt_riskparity (default: %default)",
        metavar = "--wgt_riskparity",
        default = 0)

    parser.add_option("--factor_filter",
        dest="factor_filter",
        type="str",
        help="factor_filter (default: %default)",
        metavar="factor_filter",
        default="all")

    parser.add_option("--last_n_days",
        dest = "last_n_days",
        type = "int",
        help = "--last_n_days (default: %default)",
        metavar = "--last_n_days",
        default = 1)
@deprecated_disabled
def pre_parse(preparse_verbose=True):
    if os.path.exists("/Fairedge_dev/app_QpsData/commands") and False:
        now = datetime.datetime.today()
        fn = f"/Fairedge_dev/app_QpsData/commands/{now.strftime('%Y%m%d_%H%M%S.%f')}.txt"
        print(f"python {' '.join(sys.argv)}", file=open(fn, 'w'))
    else:
        cmd = ' '.join(sys.argv)
        if len(cmd)>360:
            cmd = cmd[:360]

        if cmd.find("--list_modules")<0:
            if preparse_verbose:
                print(f"{CYAN}Pre_parse: python {cmd}{NC}", file=sys.stdout)
                print(f"{CYAN}Pre_parse: python {cmd}{NC}", file=sys.stderr)

    if '--list_cmd' not in sys.argv:
        if False:
            print('pre_parse_cmd:', ' '.join(sys.argv), file=sys.stdout)
@deprecated_disabled
def post_parse(list_cmds, opt, args):
    # set_verbose(opt.verbose)
    ctx_set_opt(opt)

    if list_cmds and opt.list_cmds:
        list_cmds(opt)
        exit(0)

    if opt.list_modules:
        list_modules()
        exit(0)

    if opt.status_file != 'NA':
        opt.quiet = 1

    if hasattr(opt, 'scn'):
        if opt.scn != "NA":
            setDataEnv(opt.scn)

    if hasattr(opt, 'dts_cfg'):
        if opt.dts_cfg != "NA":
            setDataEnv(opt.dts_cfg)
        else:
            opt.dts_cfg = getDataEnvName()

    if hasattr(opt, 'tgt'):
        if opt.tgt == "NA":
            opt.tgt = dd('raw')

    if hasattr(opt, 'src'):
        if opt.src == "NA":            
            # default=f"{rootdata()}/raw/data_rq.202012*",
            opt.src = f"{rootdata()}/raw/data_rq.20100101_20200929,/qpsdata/raw/data_rq.202*_????????"

    studyResp(opt.resp)

    return(opt, args)
@deprecated_disabled
def get_options_graphengine(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="GraphEngine")
    set_options_graphengine(parser)
    set_options_jobgraph(parser)
    set_options_common(parser)
    (options, args) = parser.parse_args()
    print(parser)
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_jobgraph(list_cmds=None, args=None, customize_options=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="JobGraph Options")
    set_options_jobgraph(parser)
    set_options_common(parser)
    if customize_options is not None:
        set_customize_options(customize_options)
        customize_options(parser)
    (options, args) = parser.parse_args(args=args)
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_sgen(list_cmds=None, customize_options=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="sgen Options")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_sgen(parser)
    set_options_factsum(parser)
    if customize_options is not None:
        set_customize_options(customize_options)
        customize_options(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_qddownload(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="QDDownload Options")
    set_options_common(parser)
    set_options_qddownload(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_qdsplit(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="QDDownload Options")
    set_options_common(parser)
    set_options_qdsplit(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_fcclient(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="FCClient Options")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_fcclient(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_influxdb(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="InfluxDb")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_influxdb(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_plotly(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="InfluxDb")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_plotly(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_model_engine(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="modelEngine")
    set_options_graphengine(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_common(list_cmds=None, customize_options=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="JobGraph Options")
    set_options_common(parser)
    if customize_options is not None:
        set_customize_options(customize_options)
        customize_options(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_options_defaults(list_cmds=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="JobGraph Options")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_plotly(parser)
    set_options_sgen(parser)
    customize_factsum_options(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
@deprecated_disabled
def get_FCAdvancedClient_options(list_cmds=None, customize_options=None, preparse_verbose=True):
    parser = OptionParser(description="FCAdvancedClient")
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="FCClient Options")
    set_options_common(parser)
    set_options_jobgraph(parser)
    set_options_sgen(parser)
    set_options_fcclient(parser)
    if customize_options is not None:
        customize_options(parser=parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)
    # parser.add_option("--addr_cfg",
    #                 dest = "addr_cfg",
    #                 type = "str",
    #                 help = "addr_cfg (default: %default)",
    #                 metavar = "addr_cfg",
    #                 default = f"{rootdata()}/config/FCScheduler.cfg")
    # parser.add_option("--pool_size",
    #                 dest = "pool_size",
    #                 type = "int",
    #                 help = "pool_size (default: %default)",
    #                 metavar = "pool_size",
    #                 default = 100)
    # parser.add_option("--list_cmds",
    #                 dest="list_cmds",
    #                 type = "str",
    #                 help="list_cmds (default: %default)",
    #                 metavar="list_cmds",
    #                 default="")
    # parser.add_option("--quiet",
    #                 dest = "quiet",
    #                 type = "int",
    #                 help = "quiet (default: %default)",
    #                 metavar = "quiet",
    #                 default = 100)
    # (options, args) = parser.parse_args()
    # return (options, args)
@deprecated_disabled
def get_mysql_options(list_cmds=None, customize_options=None, preparse_verbose=True):
    pre_parse(preparse_verbose=preparse_verbose)
    parser = OptionParser(description="mysql")
    set_options_common(parser)
    set_options_mysql(parser)
    if customize_options is not None:
        set_customize_options(customize_options)
        customize_options(parser)
    (options, args) = parser.parse_args()
    return post_parse(list_cmds, options, args)

if __name__ == '__main__':
    (options, args) = get_options_graphengine()
    qps_print(options)
    (options, args) = get_options_jobgraph()
    qps_print(options)

