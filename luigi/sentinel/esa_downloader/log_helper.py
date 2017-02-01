import logging
import os

def setup_logging(jobName, debug):
    """
    Sets up a logger to log to a file and the console

    :param path: Path to logs directory
    :returns: Return a logger setup to log to the console and a file in the directory specified (path)
    """
    program = 'sentinal_downloader'
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger = logging.getLogger(jobName)
    logger.setLevel(level)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # configure console logging
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Configure file logging
    # logPath = os.path.join(workPath, '%s-log' % (jobName))  
    # if not os.path.isdir(workPath):
    #     os.makedirs(workPath)
    # fh = logging.FileHandler(logPath)
    # fh.setLevel(level)
    # fh.setFormatter(formatter)  
    # logger.addHandler(fh)


    return logger