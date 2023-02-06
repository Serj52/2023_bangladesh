import os



class Config:
    mode = 'test'
    folder_root = os.path.dirname(os.path.abspath(__file__))
    url = {
        'ICP': 'https://www.bb.org.bd/econdata/nsdp/xml/bgd/ppi_bgd.xml',
        'IPC': 'https://www.bb.org.bd/econdata/nsdp/xml/bgd/cpi_bgd.xml'
    }
    folder_load = os.path.join(folder_root, 'Load')
    task_log = os.path.join(folder_root, 'Templates', 'task_log.xlsx')
    response = os.path.join(folder_root, 'Templates', 'response.json')
    in_process = os.path.join(folder_root, 'in_process')
    processed = os.path.join(folder_root, 'processed')
    file_data = os.path.join(in_process, 'file_data.json')
    folder_logs = os.path.join(folder_root, 'Logs')
    period = os.path.join(folder_root, 'Templates', 'period.json')
    log_file = "robot.log"
    if mode == 'test':
        #только для тестов
        bed_url = {
            'ICP': 'https://www.bb.org.bd/en/inde',
            'IPC': 'https://www.bb.org.bd/en/inde'
        }
        bad_period = os.path.join(folder_root, 'tests', 'bad_period.json')
        rabbit_host = '1docz-s-app01.gk.rosatom.local'
        rabbit_login = 'rpauser'
        rabbit_pwd = os.environ['rabbit_pwd']
        queue_request = 'hello'
        rabbit_port = 5672
        path = '/'
        queue_error = 'rpa.errors'

