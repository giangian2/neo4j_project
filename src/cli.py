import argparse
from typing import Tuple
from original import generate_dataset, add_frauds
from converters import Converters
from query_engine import QueryExecutor
from base import Neo4jConfig


class Cli:
    """Classe per gestire i comandi CLI e chiamare i servizi"""
    
    def __init__(self):
        self.converter = Converters()
        self.query_executor = None
    
    def parse_args(self) -> argparse.Namespace:
        """Parsa gli argomenti della CLI"""
        parser = argparse.ArgumentParser(description='Fraud Detection - Dataset Generator & Query Engine')
        
        subparsers = parser.add_subparsers(dest='command')
        
        # Comando: generate
        gen_parser = subparsers.add_parser('generate')
        gen_parser.add_argument('--customers', type=int, default=1000)
        gen_parser.add_argument('--terminals', type=int, default=200)
        gen_parser.add_argument('--days', type=int, default=90)
        gen_parser.add_argument('--output', type=str, default='init-data')
        gen_parser.add_argument('--size', type=str, choices=['50MB', '100MB', '200MB'])
        
        # Comando: query
        query_parser = subparsers.add_parser('query')
        query_parser.add_argument('--name', type=str)
        query_parser.add_argument('--all', action='store_true')
        query_parser.add_argument('--uri', type=str, default='bolt://localhost:7687')
        query_parser.add_argument('--user', type=str, default='neo4j')
        query_parser.add_argument('--password', type=str, default='StrongPassword123')
        query_parser.add_argument('--output', type=str, default='results')
        
        # Comando: extend
        extend_parser = subparsers.add_parser('extend')
        extend_parser.add_argument('--uri', type=str, default='bolt://localhost:7687')
        extend_parser.add_argument('--user', type=str, default='neo4j')
        extend_parser.add_argument('--password', type=str, default='StrongPassword123')
        extend_parser.add_argument('--output', type=str, default='results')
        
        return parser.parse_args()
    
    def estimate_parameters(self, target_size_mb: str) -> Tuple[int, int, int]:
        """
        Stima i parametri per raggiungere la dimensione target
        
        Args:
            target_size_mb: Dimensione target ('50MB', '100MB', '200MB', '500MB')
            
        Returns:
            Tuple (n_customers, n_terminals, nb_days)
        """
        size_map = {
            '50MB': (1000, 100, 120),
            '100MB': (1000, 100, 365),
            '200MB': (1000, 100, 1000),
        }
        return size_map.get(target_size_mb, (1000, 200, 90))
    
    def generate(self, n_customers: int, n_terminals: int, nb_days: int, 
                 output_folder: str) -> None:
        """
        Genera il dataset e lo converte in CSV
        
        Args:
            n_customers: Numero di clienti
            n_terminals: Numero di terminali
            nb_days: Numero di giorni
            output_folder: Cartella di output
        """
        print(f"Generazione dataset...")
        print(f"   Clienti: {n_customers}")
        print(f"   Terminali: {n_terminals}")
        print(f"   Giorni: {nb_days}")
        
        # Genera dataset
        customers, terminals, transactions = generate_dataset(
            n_customers=n_customers,
            n_terminals=n_terminals,
            nb_days=nb_days
        )
        
        # Aggiungi frodi
        transactions = add_frauds(customers, terminals, transactions)
        
        # Converti in CSV
        print(f"\nConversione in CSV...")
        self.converter.to_csv(customers, terminals, transactions, output_folder)
        
        print(f"\nDataset generato in {output_folder}/")
    
    def run_query_command(self, args) -> None:
        """Esegue comando query"""
        config = Neo4jConfig(
            uri=args.uri,
            username=args.user,
            password=args.password
        )
        
        executor = QueryExecutor(config)
        
        if not executor.connect():
            print("Errore: impossibile connettersi a Neo4j")
            return
        
        # Carica query
        executor.load_queries()
        
        try:
            if args.all:
                executor.run_all_queries_simple(args.output)
            elif args.name:
                executor.engine.execute_query(args.name)
            else:
                print("Specifica --name <query> o --all")
        finally:
            executor.disconnect()
    
    
    def run_extend_command(self, args) -> None:
        """Esegue comando extend"""
        import os
        config = Neo4jConfig(
            uri=args.uri,
            username=args.user,
            password=args.password
        )
        
        executor = QueryExecutor(config)
        
        if not executor.connect():
            print("Errore: impossibile connettersi a Neo4j")
            return
        
        os.makedirs(args.output, exist_ok=True)
        
        try:
            print("\n=== ESTENSIONE DATABASE ===\n")
            
            # 1. Estendi transazioni
            print("1. Estensione transazioni...")
            executor.engine.load_query('extend_tx', 'queries/extend_transactions.cypher')
            m1 = executor.engine.execute_query('extend_tx')
            
            # 2. Crea relazioni FREQUENT_COLLABORATOR
            print("\n2. Creazione relazioni FREQUENT_COLLABORATOR...")
            executor.engine.load_query('freq_collab', 'queries/create_frequent_collaborators.cypher')
            m2 = executor.engine.execute_query('freq_collab')
            
            # 3. Calcola statistiche per giorno
            print("\n3. Calcolo statistiche per giorno settimana...")
            executor.engine.load_query('stats_day', 'queries/stats_by_day.cypher')
            m3 = executor.engine.execute_query('stats_day')
            
            # Salva risultati statistiche
            if m3.success:
                result = executor.engine.manager.run_cypher(
                    executor.engine.queries['stats_day'],
                    parser='dataframe'
                )
                if result.success:
                    import pandas as pd
                    result.data.to_csv(f"{args.output}/stats_by_day.csv", index=False)
                    print(f"\nStatistiche salvate in {args.output}/stats_by_day.csv")
            
            # Salva tempi esecuzione
            executor.engine.save_results_simple(args.output)
            
            print("\n=== ESTENSIONE COMPLETATA ===")
            
        finally:
            executor.disconnect()
    
    def run(self) -> None:
        """Esegue il comando CLI"""
        args = self.parse_args()
        
        if args.command == 'generate':
            if args.size:
                n_customers, n_terminals, nb_days = self.estimate_parameters(args.size)
            else:
                n_customers = args.customers
                n_terminals = args.terminals
                nb_days = args.days
            
            self.generate(n_customers, n_terminals, nb_days, args.output)
        
        elif args.command == 'query':
            self.run_query_command(args)
        
        elif args.command == 'extend':
            self.run_extend_command(args)
        
        else:
            print("Usa: generate, query o extend")

