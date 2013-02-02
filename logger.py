import logging
import sys

FORMAT = '%(asctime)-15s %(threadName)s: %(message)s'
logging.basicConfig(format=FORMAT, stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)