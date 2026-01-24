import argparse
import os
from original import generate_dataset, add_frauds
from converters import Converters
from query_engine import QueryExecutor
from base import Neo4jConfig


class Cli:
    
    def __init__(self):
        self.converter = Converters()
    
    def parse_args(self):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest='command')
        
        gen_parser = subparsers.add_parser('generate')
        gen_parser.add_argument('--size', type=str, choices=['50MB', '100MB', '200MB'], required=True)
        gen_parser.add_argument('--output', type=str, default='init-data')
        
        query_parser = subparsers.add_parser('query')
        query_parser.add_argument('--all', action='store_true', required=True)
        query_parser.add_argument('--output', type=str, default='results')
        
        extend_parser = subparsers.add_parser('extend')
        extend_parser.add_argument('--output', type=str, default='results')
        
        return parser.parse_args()
    
    def estimate_parameters(self, target_size_mb):
        size_map = {
            '50MB': (1000, 200, 250),
            '100MB': (1000, 200, 500),
            '200MB': (1000, 200, 1000),
        }
        return size_map[target_size_mb]
    
    def generate(self, n_customers, n_terminals, nb_days, output_folder):
        customers, terminals, transactions = generate_dataset(
            n_customers=n_customers,
            n_terminals=n_terminals,
            nb_days=nb_days
        )
        transactions = add_frauds(customers, terminals, transactions)
        self.converter.to_csv(customers, terminals, transactions, output_folder)
    
    def run_query_command(self, args):
        config = Neo4jConfig()
        executor = QueryExecutor(config)
        if not executor.connect():
            return
        executor.load_queries()
        try:
            executor.run_all_queries_simple(args.output)
        finally:
            executor.disconnect()
    
    def run_extend_command(self, args):
        config = Neo4jConfig()
        executor = QueryExecutor(config)
        if not executor.connect():
            return
        try:
            executor.run_extend_queries(args.output)
        finally:
            executor.disconnect()
    
    def run(self):
        args = self.parse_args()
        if args.command == 'generate':
            n_customers, n_terminals, nb_days = self.estimate_parameters(args.size)
            self.generate(n_customers, n_terminals, nb_days, args.output)
        elif args.command == 'query':
            self.run_query_command(args)
        elif args.command == 'extend':
            self.run_extend_command(args)

