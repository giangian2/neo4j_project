import os
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from manager import Neo4jManager
from base import Neo4jConfig


@dataclass
class QueryMetrics:
    query_name: str
    execution_time: float
    rows_returned: int
    success: bool
    error: Optional[str] = None
    data: Optional[Any] = None


class QueryEngine:
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.manager = Neo4jManager(config)
        self.metrics: List[QueryMetrics] = []
        self.queries: Dict[str, str] = {}
        
    def load_query(self, name: str, filepath: str) -> bool:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.queries[name] = f.read()
            return True
        except Exception:
            return False
    
    def load_queries_from_dir(self, directory: str = "queries") -> int:
        if not os.path.exists(directory):
            return 0
        loaded = 0
        for filename in os.listdir(directory):
            if filename.endswith('.cypher'):
                name = filename.replace('.cypher', '')
                filepath = os.path.join(directory, filename)
                if self.load_query(name, filepath):
                    loaded += 1
                    print(f"Query '{name}' caricata da {filepath}")
        print(f"\n{loaded} query caricate")
        return loaded
    
    def execute_query(self, name: str, params: Optional[Dict] = None, parser: str = 'dataframe') -> QueryMetrics:
        if name not in self.queries:
            return QueryMetrics(name, 0.0, 0, False, f"Query '{name}' non trovata", None)
        
        query = self.queries[name]
        print(f"Esecuzione {name}...", end=" ")
        result = self.manager.run_cypher(query, params, parser)
        
        rows_returned = 0
        if result.success and result.data is not None:
            if isinstance(result.data, pd.DataFrame):
                rows_returned = len(result.data)
            elif isinstance(result.data, list):
                rows_returned = len(result.data)
            elif isinstance(result.data, int):
                rows_returned = result.data
        
        metrics = QueryMetrics(
            query_name=name,
            execution_time=result.execution_time,
            rows_returned=rows_returned,
            success=result.success,
            error=result.error,
            data=result.data if result.success else None
        )
        
        self.metrics.append(metrics)
        
        if result.success:
            print(f"OK ({result.execution_time:.3f}s, {rows_returned} righe)")
        else:
            print(f"ERRORE: {result.error}")
        
        return metrics
    
    def save_results_simple(self, output_dir: str = "results"):
        os.makedirs(output_dir, exist_ok=True)
        times_df = pd.DataFrame([{
            'query': m.query_name,
            'execution_time_seconds': m.execution_time,
            'rows': m.rows_returned,
            'success': m.success
        } for m in self.metrics])
        times_df.to_csv(os.path.join(output_dir, 'execution_times.csv'), index=False)
    
    def connect(self) -> bool:
        return self.manager.connect()
    
    def disconnect(self):
        self.manager.disconnect()


class QueryExecutor:
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.engine = QueryEngine(config)
    
    def run_all_queries_simple(self, output_dir: str = "results"):
        os.makedirs(output_dir, exist_ok=True)
        print("Esecuzione query...")
        
        m1 = self.engine.execute_query('q1a')
        if m1.success and isinstance(m1.data, pd.DataFrame):
            m1.data.to_csv(f"{output_dir}/query_3a.csv", index=False)
        
        m2 = self.engine.execute_query('q1b')
        if m2.success and isinstance(m2.data, pd.DataFrame):
            m2.data.to_csv(f"{output_dir}/query_3b.csv", index=False)
        
        if 'q1c' in self.engine.queries:
            m3 = self.engine.execute_query('q1c', params={'customerId': '889'})
            if m3.success and isinstance(m3.data, pd.DataFrame):
                m3.data.to_csv(f"{output_dir}/query_3c.csv", index=False)
        
        self.engine.save_results_simple(output_dir)
        print(f"Tempi salvati in {output_dir}/execution_times.csv")
        print(f"\nRisultati salvati in {output_dir}/")
    
    def run_extend_queries(self, output_dir: str = "results"):
        os.makedirs(output_dir, exist_ok=True)
        print("\n=== ESTENSIONE DATABASE ===\n")
        
        print("1. Estensione transazioni...")
        self.engine.load_query('extend_tx', 'queries/extend_transactions.cypher')
        self.engine.execute_query('extend_tx')
        
        print("\n2. Creazione relazioni FREQUENT_COLLABORATOR...")
        self.engine.load_query('freq_collab', 'queries/create_frequent_collaborators.cypher')
        self.engine.execute_query('freq_collab')
        
        print("\n3. Calcolo statistiche per giorno settimana...")
        self.engine.load_query('stats_day', 'queries/stats_by_day.cypher')
        m3 = self.engine.execute_query('stats_day')
        
        if m3.success and isinstance(m3.data, pd.DataFrame):
            m3.data.to_csv(f"{output_dir}/stats_by_day.csv", index=False)
            print(f"\nStatistiche salvate in {output_dir}/stats_by_day.csv")
        
        self.engine.save_results_simple(output_dir)
        print(f"Tempi salvati in {output_dir}/execution_times.csv")
        print("\n=== ESTENSIONE COMPLETATA ===")
    
    def load_queries(self, directory: str = "queries") -> int:
        return self.engine.load_queries_from_dir(directory)
    
    def connect(self) -> bool:
        return self.engine.connect()
    
    def disconnect(self):
        self.engine.disconnect()
    

