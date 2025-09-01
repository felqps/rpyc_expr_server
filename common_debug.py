
import os,sys,datetime
from common_colors import *
import pandas as pd
import pickle
from common_smart_load import buf2md5
import glob

def format_path(dump_dir,title,data_type,note):
    # note=note.replace(' ','')
    # note=note.replace('(','\(')
    # note=note.replace(')','\)')
    md5=buf2md5(note)[-4:]
    now = datetime.datetime.today()
    tm = now.strftime("%H%M%S.%f")
    return (f"{dump_dir}/{tm}_{md5}.{data_type}", f"{dump_dir}/{tm}_{md5}.{data_type}.{title}.txt", note)

CLEAR_DEBUG_DUMP=True
def debug_dump(opt, data, title="NA", data_type="df", note=""):
    global CLEAR_DEBUG_DUMP
    skip=True
    if hasattr(opt, "debug_dump"):
        dump_dir=opt.debug_dump
    else:
        dump_dir="debug_dump_NA"

    if os.path.exists(opt.debug_dump):
        skip=False
        if CLEAR_DEBUG_DUMP:
            CLEAR_DEBUG_DUMP=False
            for fn in glob.glob(f"{opt.debug_dump}/??????.*_*.*"):
                os.remove(fn)

    (dump_fn, note_fn, note)=format_path(dump_dir, title, data_type, note)
    if not skip:
        pickle.dump(data, open(dump_fn, 'wb'))
        open(note_fn, 'w').write(note)
    
    COLOR = BROWN if skip else GREEN
    print(f"{COLOR}DEBUG_DUMP(skip={int(skip)}): dump_fn= {dump_fn};{NC} note_fn= {note_fn}; {CYAN}note= {note}{NC}")
    


if __name__ == '__main__':
    funcn = 'fdf_logging.main'
    from options_helper import *
    opt, args = get_options(funcn)

    logger().debug("info test")
    logger().info("info test")
    logger().warning("warning test")
    logger().error("error test")
    logger().critical("critical test")
