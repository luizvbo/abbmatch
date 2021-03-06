"""Top-level package for abbmatch."""
import logging

__author__ = """Luiz Otavio Vilas Boas Oliveira"""
__email__ = 'luiz.vbo@gmail.com'
__version__ = '0.1.0'

logging.basicConfig(filename='abbmatch.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)