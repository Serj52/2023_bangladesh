from business import IndexBusiness
import logging
import logging.config
from config import Config as cfg
from Lib import log

def run():
    IndexBusiness().first_start()
    while True:
        IndexBusiness().process_task()

if __name__ == '__main__':
    log.set_1(cfg)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })
    logging.info('\n\n=== Start ===\n\n')
    logging.info(f'Режим запуска {cfg.mode}')
    run()


