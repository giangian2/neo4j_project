"""neo4j_manager/manager.py - Manager principale per Neo4j"""

from typing import Dict, Any, Optional, Union
import time
from neo4j import GraphDatabase, Driver, Result

from base import Neo4jConfig, QueryResult, ResponseParser
from parsers import DataFrameParser, ListParser, CountParser, SingleValueParser


class Neo4jManager:
    """Gestisce connessione ed esecuzione query Neo4j"""
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.config = config or Neo4jConfig()
        self.driver: Optional[Driver] = None
        self._setup_parsers()
    
    def _setup_parsers(self):
        """Configura parser predefiniti"""
        self.parsers = {
            'dataframe': DataFrameParser(),
            'list': ListParser(),
            'count': CountParser(),
            'single': SingleValueParser()
        }
    
    def connect(self) -> bool:
        """Stabilisce la connessione"""
        try:
            self.driver = GraphDatabase.driver(
                self.config.uri,
                auth=(self.config.username, self.config.password)
            )
            
            # Test connessione
            with self.driver.session(database=self.config.database) as session:
                session.run("RETURN 1").single()
            
            print(f"✅ Connesso a Neo4j: {self.config.uri}")
            return True
            
        except Exception as e:
            print(f"❌ Errore connessione: {e}")
            self.driver = None
            return False
    
    def disconnect(self):
        """Chiude la connessione"""
        if self.driver:
            self.driver.close()
            self.driver = None
            print("🔌 Connessione chiusa")
    
    def is_connected(self) -> bool:
        """Verifica se è connesso"""
        if not self.driver:
            return False
        try:
            with self.driver.session() as session:
                session.run("RETURN 1").single()
            return True
        except Exception:
            return False
    
    def run_cypher(self, 
                  query: str, 
                  params: Optional[Dict] = None,
                  parser: Union[str, ResponseParser] = 'list') -> QueryResult:
        """
        Esegue una query Cypher
        
        Args:
            query: Query Cypher da eseguire
            params: Parametri per la query
            parser: Nome parser o istanza ResponseParser
            
        Returns:
            QueryResult con il risultato
        """
        if not self.is_connected():
            if not self.connect():
                return QueryResult(
                    success=False,
                    error="Non connesso a Neo4j"
                )
        
        start_time = time.time()
        params = params or {}
        
        try:
            with self.driver.session(database=self.config.database) as session:
                result = session.run(query, params)
                
                # Ottieni il parser
                if isinstance(parser, str):
                    response_parser = self.parsers.get(parser, self.parsers['list'])
                else:
                    response_parser = parser
                
                # Parsing della risposta
                data = response_parser.parse(result, query, params)
                
                return QueryResult(
                    success=True,
                    data=data,
                    execution_time=time.time() - start_time,
                    query=query
                )
                
        except Exception as e:
            return QueryResult(
                success=False,
                error=str(e),
                execution_time=time.time() - start_time,
                query=query
            )
    
    def register_parser(self, name: str, parser: ResponseParser):
        """Registra un nuovo parser"""
        self.parsers[name] = parser
    
    def __enter__(self):
        """Context manager enter"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()