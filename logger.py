__author__ = 'soldier'


import logging
import sys

FORMAT = '%(asctime)-15s %(threadName)s: %(message)s'
logging.basicConfig(format=FORMAT, stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)