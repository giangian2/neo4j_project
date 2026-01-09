"""neo4j_manager/parsers.py - Parser per le risposte"""

import pandas as pd
from typing import List, Dict, Any
from neo4j import Result


class DataFrameParser:
    """Parser che converte in DataFrame pandas"""
    def parse(self, result: Result, query: str, params: Dict) -> pd.DataFrame:
        return pd.DataFrame([dict(record) for record in result])


class ListParser:
    """Parser che restituisce lista di dizionari"""
    def parse(self, result: Result, query: str, params: Dict) -> List[Dict]:
        return [dict(record) for record in result]


class CountParser:
    """Parser che restituisce conteggio"""
    def parse(self, result: Result, query: str, params: Dict) -> int:
        return len(list(result))


class SingleValueParser:
    """Parser per singolo valore"""
    def parse(self, result: Result, query: str, params: Dict) -> Any:
        record = result.single()
        return record[0] if record else None