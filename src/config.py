import dataclasses
from abc import ABC, abstractmethod


@dataclasses.dataclass(frozen=True)
class AlphavantageConfig:
    fiat_url_pattern: str
    api_key: str


class AbstractServiceConfig(ABC):

    @abstractmethod
    def alphavantage_cfg(self) -> AlphavantageConfig:
        pass
