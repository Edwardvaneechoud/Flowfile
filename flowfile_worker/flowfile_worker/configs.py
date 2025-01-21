import logging

logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('FlowfileWorker')
logger.setLevel(logging.INFO)
