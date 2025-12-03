from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class AlphavantageConfig:
    fiat_url_pattern: str
    crypto_url_pattern: str
    api_key: str

@dataclass(frozen=True)
class PolygonConfig:
    url_pattern: str
    api_key: str
    ignore_spread: bool

class AbstractServiceConfig(ABC):

    @abstractmethod
    def alphavantage_cfg(self) -> AlphavantageConfig:
        pass

    @abstractmethod
    def polygon_cfg(self) -> PolygonConfig:
        pass