from typing import Any, Dict, Optional, Protocol
from dataclasses import dataclass
from neo4j import Result


class ResponseParser(Protocol):
    def parse(self, result: Result, query: str, params: Dict) -> Any:
        ...


@dataclass
class QueryResult:
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    execution_time: float = 0.0
    query: Optional[str] = None


@dataclass
class Neo4jConfig:
    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = "StrongPassword123"
    database: str = "neo4j"