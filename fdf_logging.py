#!/usr/bin/env python

import os,sys
from common_logging import *

if __name__ == '__main__':
    funcn = 'fdf_logging.main'
    from options_helper import *
    opt, args = get_options(funcn)

    logger().debug("info test")
    logger().info("info test")
    logger().warning("warning test")
    logger().error("error test")
    logger().critical("critical test")
    
