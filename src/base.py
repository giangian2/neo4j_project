"""neo4j_manager/base.py - Interfacce base per Neo4j"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol
from dataclasses import dataclass
from neo4j import Result


# ============================================================================
# INTERFACCE
# ============================================================================

class ResponseParser(Protocol):
    """Protocollo per i parser delle risposte"""
    def parse(self, result: Result, query: str, params: Dict) -> Any:
        ...


@dataclass
class QueryResult:
    """Risultato di una query"""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    execution_time: float = 0.0
    query: Optional[str] = None


@dataclass
class Neo4jConfig:
    """Configurazione connessione Neo4j"""
    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = "password"
    database: str = "neo4j"