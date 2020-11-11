from abc import ABC, abstractmethod

from share.models.core import NormalizedData


class MetadataFormatter(ABC):
    @abstractmethod
    def format(self, normalized_data: NormalizedData) -> str:
        raise NotImplementedError
