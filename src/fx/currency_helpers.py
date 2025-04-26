from typing import List

from currency_codes import Currency, get_fiat_currencies, get_crypto_currencies


def is_crypto(code: str) -> bool:
    return CurrencyTypeResolver.is_crypto(code)


def is_fiat(code: str) -> bool:
    return CurrencyTypeResolver.is_fiat(code)


def _resolve_3_letter_currency_codes(currencies: List[Currency]) -> set[str]:
    return {c.code.upper() for c in currencies if c.code}


class CurrencyTypeResolver:
    _crypto_currencies_codes = _resolve_3_letter_currency_codes(get_crypto_currencies())
    _fiat_currencies_codes = _resolve_3_letter_currency_codes(get_fiat_currencies())

    @classmethod
    def is_crypto(cls, code: str) -> bool:
        if not code:
            return False
        return code.upper() in CurrencyTypeResolver._crypto_currencies_codes

    @classmethod
    def is_fiat(cls, code: str) -> bool:
        if not code:
            return False
        return code.upper() in CurrencyTypeResolver._fiat_currencies_codes
