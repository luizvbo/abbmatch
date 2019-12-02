"""Top-level package for abbmatch."""
import logging

__author__ = """Luiz Otavio Vilas Boas Oliveira"""
__email__ = 'luiz.vbo@gmail.com'
__version__ = '0.1.0'

logging.basicConfig(filename='abbmatch.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


# create file handler which logs INFO messages
# fh = logging.FileHandler('abbmatch.log')
# fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
# ch.setFormatter(formatter)
# add the handlers to the logger
# logger.addHandler(fh)
# logger.addHandler(ch)
