from abc import ABC, abstractmethod


class IndexSetup(ABC):
    @property
    @abstractmethod
    def supported_message_types(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def index_settings(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def index_mappings(self):
        raise NotImplementedError

    @abstractmethod
    def build_and_cache_source_doc(self, message_type, target_id):
        raise NotImplementedError

    @abstractmethod
    def build_action_generator(self, index_name, message_type):
        raise NotImplementedError
