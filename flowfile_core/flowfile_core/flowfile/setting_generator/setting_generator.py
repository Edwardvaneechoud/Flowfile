from flowfile_core.configs import logger
from typing import Callable


class SettingGenerator:
    setting_generator_set = set()

    def __init__(self):
        self.setting_generator_set = set()

    def add_setting_generator_func(self, f: Callable):
        self.setting_generator_set.update([f.__name__])
        setattr(self, f.__name__, f)

    def get_setting_generator(self, node_type: str) -> Callable:
        logger.info('getting setting generator for ' + node_type)

        if node_type in self.setting_generator_set:
            logger.info('setting generator found')
            return getattr(self, node_type)
        else:
            return lambda x: x


class SettingUpdator:
    setting_updator_set = set()

    def __init__(self):
        self.setting_updator_set = set()

    def add_setting_updator_func(self, f: Callable):
        self.setting_updator_set.update([f.__name__])
        setattr(self, f.__name__, f)

    def get_setting_updator(self, node_type: str) -> Callable:
        logger.info('getting setting updator for ' + node_type)
        if node_type in self.setting_updator_set:
            logger.info('setting updator found')
            return getattr(self, node_type)
        else:
            return lambda x: x
