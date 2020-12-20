
import sys
import logging
import etl_operations

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    etl_operations.run(sys.argv)
