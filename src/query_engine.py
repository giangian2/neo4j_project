"""Query Engine per eseguire e misurare performance delle query Neo4j"""

import os
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import pandas as pd

from manager import Neo4jManager
from base import Neo4jConfig, QueryResult


@dataclass
class QueryMetrics:
    """Metriche di esecuzione di una query"""
    query_name: str
    execution_time: float
    rows_returned: int
    success: bool
    error: Optional[str] = None
    dataset_info: Optional[Dict] = None


class QueryEngine:
    """Engine per eseguire query e raccogliere metriche"""
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.manager = Neo4jManager(config)
        self.metrics: List[QueryMetrics] = []
        self.queries: Dict[str, str] = {}
        
    def load_query(self, name: str, filepath: str) -> bool:
        """Carica una query da file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.queries[name] = f.read()
            print(f"✅ Query '{name}' caricata da {filepath}")
            return True
        except Exception as e:
            print(f"❌ Errore caricamento query '{name}': {e}")
            return False
    
    def load_queries_from_dir(self, directory: str = "queries") -> int:
        """Carica tutte le query da una directory"""
        if not os.path.exists(directory):
            print(f"⚠️  Directory {directory} non trovata")
            return 0
        
        loaded = 0
        for filename in os.listdir(directory):
            if filename.endswith('.cypher'):
                name = filename.replace('.cypher', '')
                filepath = os.path.join(directory, filename)
                if self.load_query(name, filepath):
                    loaded += 1
        
        print(f"\n📊 {loaded} query caricate")
        return loaded
    
    def execute_query(
        self, 
        name: str, 
        params: Optional[Dict] = None,
        parser: str = 'dataframe',
        dataset_info: Optional[Dict] = None
    ) -> QueryMetrics:
        """Esegue una query e registra le metriche"""
        if name not in self.queries:
            return QueryMetrics(
                query_name=name,
                execution_time=0.0,
                rows_returned=0,
                success=False,
                error=f"Query '{name}' non trovata"
            )
        
        query = self.queries[name]
        
        print(f"Esecuzione {name}...", end=" ")
        
        # Esegui query
        result = self.manager.run_cypher(query, params, parser)
        
        # Calcola righe restituite
        rows_returned = 0
        if result.success and result.data is not None:
            if isinstance(result.data, pd.DataFrame):
                rows_returned = len(result.data)
            elif isinstance(result.data, list):
                rows_returned = len(result.data)
            elif isinstance(result.data, int):
                rows_returned = result.data
        
        # Crea metriche
        metrics = QueryMetrics(
            query_name=name,
            execution_time=result.execution_time,
            rows_returned=rows_returned,
            success=result.success,
            error=result.error,
            dataset_info=dataset_info
        )
        
        # Registra metriche
        self.metrics.append(metrics)
        
        # Stampa risultato
        if result.success:
            print(f"OK ({result.execution_time:.3f}s, {rows_returned} righe)")
        else:
            print(f"ERRORE: {result.error}")
        
        return metrics
    
    def save_results_simple(self, output_dir: str = "results"):
        """Salva risultati query in file separati"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Salva tempi esecuzione
        times_df = pd.DataFrame([{
            'query': m.query_name,
            'execution_time_seconds': m.execution_time,
            'rows': m.rows_returned,
            'success': m.success
        } for m in self.metrics])
        
        times_file = os.path.join(output_dir, 'execution_times.csv')
        times_df.to_csv(times_file, index=False)
        print(f"Tempi salvati in {times_file}")
    
    def get_last_result(self) -> Optional[Any]:
        """Ritorna i dati dell'ultima query eseguita"""
        if not self.metrics:
            return None
        return self.metrics[-1]
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Recupera informazioni sul dataset dal database"""
        info = {}
        
        queries = {
            'customers': "MATCH (c:Customer) RETURN count(c) as count",
            'terminals': "MATCH (t:Terminal) RETURN count(t) as count",
            'transactions': "MATCH (tx:Transaction) RETURN count(tx) as count",
            'quarters': "MATCH (q:Quarter) RETURN count(q) as count"
        }
        
        for name, query in queries.items():
            result = self.manager.run_cypher(query, parser='single')
            if result.success:
                info[name] = result.data
        
        return info
    
    def connect(self) -> bool:
        """Connetti al database"""
        return self.manager.connect()
    
    def disconnect(self):
        """Disconnetti dal database"""
        self.manager.disconnect()
    
    def __enter__(self):
        """Context manager"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.disconnect()


class QueryExecutor:
    """Executor specifico per le query del progetto"""
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.engine = QueryEngine(config)
    
    def run_query_3a(
        self, 
        min_shared_terminals: int = 4,
        max_tx_diff: int = 2
    ) -> QueryMetrics:
        """
        Query 3.a - Customer con terminali condivisi
        
        Args:
            min_shared_terminals: Minimo terminali condivisi (default: 4)
            max_tx_diff: Massima differenza transazioni (default: 2)
        """
        return self.engine.execute_query(
            'q1a',
            params={
                'min_shared': min_shared_terminals,
                'max_diff': max_tx_diff
            }
        )
    
    def run_query_3b(
        self,
        threshold: float = 1.3
    ) -> QueryMetrics:
        """
        Query 3.b - Outlier per trimestre
        
        Args:
            threshold: Soglia percentuale (default: 1.3 = +30%)
        """
        return self.engine.execute_query(
            'q1b',
            params={'threshold': threshold}
        )
    
    def run_query_3c(
        self,
        customer_id: str,
        degree: int = 3
    ) -> QueryMetrics:
        """
        Query 3.c - Co-customer network
        
        Args:
            customer_id: ID del customer di partenza
            degree: Grado della rete (default: 3)
        """
        return self.engine.execute_query(
            'q1c',
            params={
                'customer_id': customer_id,
                'degree': degree
            }
        )
    
    def run_all_queries_simple(self, output_dir: str = "results"):
        """Esegue tutte e 3 le query e salva risultati"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        print("Esecuzione query...")
        
        # Query 3.a
        self.engine.execute_query('q1a')
        if self.engine.metrics[-1].success:
            result = self.engine.manager.run_cypher(
                self.engine.queries['q1a'],
                parser='dataframe'
            )
            if result.success and isinstance(result.data, pd.DataFrame):
                result.data.to_csv(f"{output_dir}/query_3a.csv", index=False)
        
        # Query 3.b
        self.engine.execute_query('q1b')
        if self.engine.metrics[-1].success:
            result = self.engine.manager.run_cypher(
                self.engine.queries['q1b'],
                parser='dataframe'
            )
            if result.success and isinstance(result.data, pd.DataFrame):
                result.data.to_csv(f"{output_dir}/query_3b.csv", index=False)
        
        # Query 3.c
        if 'q1c' in self.engine.queries:
            self.engine.execute_query('q1c', params={'customerId': '889'})
            if self.engine.metrics[-1].success:
                result = self.engine.manager.run_cypher(
                    self.engine.queries['q1c'],
                    params={'customerId': '889'},
                    parser='dataframe'
                )
                if result.success and isinstance(result.data, pd.DataFrame):
                    result.data.to_csv(f"{output_dir}/query_3c.csv", index=False)
        
        # Salva tempi
        self.engine.save_results_simple(output_dir)
        
        print(f"\nRisultati salvati in {output_dir}/")
    
    def load_queries(self, directory: str = "queries") -> int:
        """Carica le query"""
        return self.engine.load_queries_from_dir(directory)
    
    def connect(self) -> bool:
        """Connetti"""
        return self.engine.connect()
    
    def disconnect(self):
        """Disconnetti"""
        self.engine.disconnect()
    

