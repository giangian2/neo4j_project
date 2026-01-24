import pandas as pd
from typing import List, Dict, Any
from neo4j import Result


class DataFrameParser:
    def parse(self, result: Result, query: str, params: Dict) -> pd.DataFrame:
        return pd.DataFrame([dict(record) for record in result])


class ListParser:
    def parse(self, result: Result, query: str, params: Dict) -> List[Dict]:
        return [dict(record) for record in result]


class CountParser:
    def parse(self, result: Result, query: str, params: Dict) -> int:
        return len(list(result))


class SingleValueParser:
    def parse(self, result: Result, query: str, params: Dict) -> Any:
        record = result.single()
        return record[0] if record else None