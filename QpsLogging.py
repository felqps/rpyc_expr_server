import logging
import os

LOG_FORMAT = "%(levelname)s:%(asctime)s:%(message)s"
# LOG_FORMAT = "%(levelname)s:%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

class Logger():
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }

    def __init__(self, filename="", level='info', mode='a', fmt=LOG_FORMAT, max_bytes=1024*20, backup_cnt=1):
        self.filename = filename
        self.level = level
        self.mode = mode
        self.fmt = fmt
        self.max_bytes = max_bytes
        self.backup_cnt = backup_cnt
        # self.log = logging.getLogger(os.path.basename(filename))
        # self.log.setLevel(level = Logger.level_relations[level])
        # handler = logging.FileHandler(filename)
        # handler.setLevel(Logger.level_relations[level])
        # handler.setFormatter(logging.Formatter(fmt))
        # self.log.addHandler(handler)
        # self.log = self.getFileLogger() if self.filename else None

    def getRotatingFileLogger(self, name=None):
        logger = logging.getLogger(name = name)
        handler = logging.handlers.RotatingFileHandler(self.filename, mode=self.mode, maxBytes=self.max_bytes, backupCount=self.backup_cnt) #每 1024Bytes重写一个文件,保留2(backupCount) 个旧文件
        handler.setLevel(Logger.level_relations[self.level])
        handler.setFormatter(logging.Formatter(self.fmt))
        logger.addHandler(handler)
        return logger

    def getFileLogger(self, name=None):
        logger = logging.getLogger(name = name)
        handler = logging.FileHandler(self.filename, mode=self.mode)
        handler.setLevel(Logger.level_relations[self.level])
        handler.setFormatter(logging.Formatter(self.fmt))
        logger.addHandler(handler)
        return logger

    def getSimpleLogger(self, name=None):
        logger = logging.getLogger(name = name)
        return logger

    # @property
    # def logger(self):
    #     return self.log

if __name__ == '__main__':
    logger = Logger("/home/shuser/test.log").getFileLogger("test")
    logger.info("info test")
    logger.warning("warning test")
    logger.error("error test")
    try:
        raise EOFError("aaaaaaaaa")
    except Exception as e:
        logger.exception(e)
