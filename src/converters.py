import os
import pandas as pd


class Converters:

    def to_csv(
        self,
        customers: pd.DataFrame,
        terminals: pd.DataFrame,
        transactions: pd.DataFrame,
        output_folder: str = "init-data"
    ) -> str:

        os.makedirs(output_folder, exist_ok=True)

        # =====================================================
        # PREPARAZIONE TRANSAZIONI (YEAR / QUARTER)
        # =====================================================
        tx = transactions.copy()
        tx['year'] = tx['TX_DATETIME'].dt.year
        tx['quarter'] = tx['TX_DATETIME'].dt.quarter
        
        # DEBUG: Verifica range date
        print(f"Date range transazioni: {tx['TX_DATETIME'].min()} to {tx['TX_DATETIME'].max()}")
        print(f"Anni unici: {sorted(tx['year'].unique())}")
        print(f"Quarter unici: {sorted(tx['quarter'].unique())}")

        # =====================================================
        # SOLUZIONE SEMPLICE: Quarter solo per combinazioni REALI
        # =====================================================
        # 1. Calcola mediana per ogni combo reale (TERMINAL_ID, year, quarter)
        median_q = (
            tx.groupby(['TERMINAL_ID', 'year', 'quarter'])['TX_AMOUNT']
            .median()
            .reset_index(name='current_median')
        )
        
        # 2. Crea Quarter nodes SOLO per combo reali
        quarter_nodes = median_q.copy()
        
        # 3. Calcola quarter precedente per OGNI riga
        quarter_nodes['prev_year'] = quarter_nodes['year']
        quarter_nodes['prev_quarter'] = quarter_nodes['quarter'] - 1
        
        quarter_nodes.loc[quarter_nodes['prev_quarter'] == 0, 'prev_quarter'] = 4
        quarter_nodes.loc[quarter_nodes['prev_quarter'] == 4, 'prev_year'] -= 1
        
        # 4. Aggiungi prev_median (se esiste)
        # Crea mappa per lookup veloce
        median_map = median_q.set_index(['TERMINAL_ID', 'year', 'quarter'])['current_median'].to_dict()
        
        quarter_nodes['prev_median'] = quarter_nodes.apply(
            lambda r: median_map.get((r['TERMINAL_ID'], r['prev_year'], r['prev_quarter']), None),
            axis=1
        )
        
        # 5. Crea ID Quarter (deve essere uguale a quello usato nelle transazioni!)
        quarter_nodes['TERMINAL_ID_STR'] = 'T' + quarter_nodes['TERMINAL_ID'].astype(str)
        quarter_nodes['quarterId:ID(Quarter)'] = quarter_nodes.apply(
            lambda r: f"T{r['TERMINAL_ID']}_Y{r['year']}_Q{r['quarter']}",
            axis=1
        )
        
        print(f"\nQuarter nodes creati: {len(quarter_nodes)}")
        print(f"Terminali coperti: {quarter_nodes['TERMINAL_ID'].nunique()}")
        print(f"Range anni: {quarter_nodes['year'].min()} - {quarter_nodes['year'].max()}")

        # =====================================================
        # 1. CUSTOMERS
        # =====================================================
        total_tx = (
            transactions
            .groupby('CUSTOMER_ID')['TRANSACTION_ID']
            .count()
            .reset_index(name='total_tx_count')
        )

        customers_df = (
            customers
            .merge(total_tx, on='CUSTOMER_ID', how='left')
            .fillna(0)
        )

        customers_csv = customers_df[[
            'CUSTOMER_ID',
            'x_customer_id',
            'y_customer_id',
            'mean_amount',
            'std_amount',
            'mean_nb_tx_per_day',
            'total_tx_count'
        ]].rename(columns={
            'CUSTOMER_ID': 'customerId:ID(Customer)',
            'x_customer_id': 'x',
            'y_customer_id': 'y'
        })

        customers_csv[':LABEL'] = 'Customer'
        customers_csv.to_csv(f'{output_folder}/customers.csv', index=False)

        # =====================================================
        # 2. TERMINALS
        # =====================================================
        terminals['TERMINAL_ID'] = 'T' + terminals['TERMINAL_ID'].astype(str)

        terminals_csv = terminals.rename(columns={
            'TERMINAL_ID': 'terminalId:ID(Terminal)',
            'x_terminal_id': 'x',
            'y_terminal_id': 'y'
        })

        terminals_csv[':LABEL'] = 'Terminal'
        terminals_csv = terminals_csv[['terminalId:ID(Terminal)', 'x', 'y', ':LABEL']]
        terminals_csv.to_csv(f'{output_folder}/terminals.csv', index=False)

        # =====================================================
        # 2b. QUARTER NODES
        # =====================================================
        # Crea CSV Quarter - SOLO colonne essenziali
        quarter_csv = quarter_nodes[[
            'quarterId:ID(Quarter)',
            'year',
            'quarter',
            'current_median',
            'prev_median'
        ]].copy()
        
        quarter_csv[':LABEL'] = 'Quarter'
        quarter_csv.to_csv(f'{output_folder}/quarters.csv', index=False)
        
        print(f"\nPrime 5 Quarter nodes:")
        print(quarter_csv.head())

        # =====================================================
        # 2c. TERMINAL -> QUARTER (RELAZIONE)
        # =====================================================
        terminal_quarter_rel = pd.DataFrame({
            ':START_ID(Terminal)': quarter_nodes['TERMINAL_ID_STR'],
            ':END_ID(Quarter)': quarter_nodes['quarterId:ID(Quarter)'],
            ':TYPE': 'HAS_QUARTER'
        })
        
        terminal_quarter_rel = terminal_quarter_rel.drop_duplicates()
        terminal_quarter_rel.to_csv(f'{output_folder}/terminal_quarter.csv', index=False)

        # =====================================================
        # 3. TRANSACTIONS - DEVE usare lo STESSO quarter_id!
        # =====================================================
        # Crea quarter_id PERFETTAMENTE UGUALI a quelli in quarter_nodes
        tx['quarter_id'] = tx.apply(
            lambda r: f"T{r['TERMINAL_ID']}_Y{r['year']}_Q{r['quarter']}",
            axis=1
        )
        
        # VERIFICA: tutte le transazioni hanno un quarter_id valido?
        valid_quarter_ids = set(quarter_nodes['quarterId:ID(Quarter)'])
        tx['has_valid_quarter'] = tx['quarter_id'].isin(valid_quarter_ids)
        
        print(f"\nTransazioni totali: {len(tx)}")
        print(f"Transazioni con quarter_id valido: {tx['has_valid_quarter'].sum()}")
        print(f"Transazioni senza quarter_id valido: {len(tx) - tx['has_valid_quarter'].sum()}")
        
        # Mostra transazioni problematiche
        if not tx['has_valid_quarter'].all():
            problematic = tx[~tx['has_valid_quarter']]
            print("\nTransazioni problematiche (prime 5):")
            print(problematic[['TRANSACTION_ID', 'TERMINAL_ID', 'year', 'quarter', 'quarter_id']].head())
            
            # Crea Quarter nodes mancanti per queste transazioni
            missing_quarters = problematic[['TERMINAL_ID', 'year', 'quarter']].drop_duplicates()
            print(f"\nCreazione di {len(missing_quarters)} quarter nodes mancanti...")
            
            for _, row in missing_quarters.iterrows():
                quarter_id = f"T{row['TERMINAL_ID']}_Y{row['year']}_Q{row['quarter']}"
                new_quarter = pd.DataFrame([{
                    'quarterId:ID(Quarter)': quarter_id,
                    'year': row['year'],
                    'quarter': row['quarter'],
                    'current_median': 0.0,  # Default
                    'prev_median': None,
                    ':LABEL': 'Quarter'
                }])
                quarter_csv = pd.concat([quarter_csv, new_quarter], ignore_index=True)
                
                # Aggiungi relazione Terminal->Quarter
                new_rel = pd.DataFrame([{
                    ':START_ID(Terminal)': f"T{row['TERMINAL_ID']}",
                    ':END_ID(Quarter)': quarter_id,
                    ':TYPE': 'HAS_QUARTER'
                }])
                terminal_quarter_rel = pd.concat([terminal_quarter_rel, new_rel], ignore_index=True)
            
            # Salva versioni aggiornate
            quarter_csv.to_csv(f'{output_folder}/quarters.csv', index=False)
            terminal_quarter_rel.to_csv(f'{output_folder}/terminal_quarter.csv', index=False)

        transactions_csv = tx[[
            'TRANSACTION_ID',
            'TX_AMOUNT',
            'TX_DATETIME',
            'TX_FRAUD'
        ]].rename(columns={
            'TRANSACTION_ID': 'transactionId:ID(Transaction)',
            'TX_AMOUNT': 'amount',
            'TX_DATETIME': 'datetime',
            'TX_FRAUD': 'fraud'
        })

        transactions_csv[':LABEL'] = 'Transaction'
        transactions_csv.to_csv(f'{output_folder}/transactions.csv', index=False)

        # =====================================================
        # 3b. TRANSAZIONE -> QUARTER (SOLO relazioni valide!)
        # =====================================================
        # Filtra SOLO transazioni con quarter_id valido
        tx_valid = tx[tx['has_valid_quarter']].copy()
        
        transaction_quarter_rel = pd.DataFrame({
            ':START_ID(Transaction)': tx_valid['TRANSACTION_ID'],
            ':END_ID(Quarter)': tx_valid['quarter_id'],
            ':TYPE': 'IN_QUARTER'
        })
        
        transaction_quarter_rel.to_csv(f'{output_folder}/transaction_quarter.csv', index=False)
        print(f"Relazioni Transaction->Quarter create: {len(transaction_quarter_rel)}")

        # =====================================================
        # 4. CUSTOMER -> TRANSACTION
        # =====================================================
        cust_tx = transactions[[
            'CUSTOMER_ID',
            'TRANSACTION_ID'
        ]].rename(columns={
            'CUSTOMER_ID': ':START_ID(Customer)',
            'TRANSACTION_ID': ':END_ID(Transaction)'
        })

        cust_tx[':TYPE'] = 'MADE_TRANSACTION'
        cust_tx.to_csv(f'{output_folder}/cust_tx.csv', index=False)

        # =====================================================
        # 5. TRANSACTION -> TERMINAL
        # =====================================================
        tx_term = transactions[[
            'TRANSACTION_ID',
            'TERMINAL_ID'
        ]].copy()

        tx_term['TERMINAL_ID'] = 'T' + tx_term['TERMINAL_ID'].astype(str)

        tx_term = tx_term.rename(columns={
            'TRANSACTION_ID': ':START_ID(Transaction)',
            'TERMINAL_ID': ':END_ID(Terminal)'
        })

        tx_term[':TYPE'] = 'AT_TERMINAL'
        tx_term.to_csv(f'{output_folder}/tx_term.csv', index=False)

        # =====================================================
        # 6. CUSTOMER -> TERMINAL (USED_TERMINAL)
        # =====================================================
        used_terminal = (
            transactions
            .groupby(['CUSTOMER_ID', 'TERMINAL_ID'])
            .size()
            .reset_index(name='tx_count')
        )

        used_terminal[':START_ID(Customer)'] = used_terminal['CUSTOMER_ID']
        used_terminal[':END_ID(Terminal)'] = 'T' + used_terminal['TERMINAL_ID'].astype(str)
        used_terminal[':TYPE'] = 'USED_TERMINAL'

        used_terminal_csv = used_terminal[[
            ':START_ID(Customer)',
            ':END_ID(Terminal)',
            'tx_count',
            ':TYPE'
        ]]

        used_terminal_csv.to_csv(f'{output_folder}/used_terminal.csv', index=False)

        # =====================================================
        # 7. CUSTOMER ↔ CUSTOMER (SHARES_TERMINAL)
        # =====================================================
        cust_term_map = (
            transactions
            .groupby('CUSTOMER_ID')['TERMINAL_ID']
            .apply(set)
            .to_dict()
        )

        shares_records = []
        customer_ids = list(cust_term_map.keys())

        for i, c1 in enumerate(customer_ids):
            for c2 in customer_ids[i + 1:]:
                shared = cust_term_map[c1] & cust_term_map[c2]
                for t in shared:
                    shares_records.append({
                        ':START_ID(Customer)': c1,
                        ':END_ID(Customer)': c2,
                        'terminal_id': f'T{t}',
                        ':TYPE': 'SHARES_TERMINAL'
                    })

        pd.DataFrame(shares_records).to_csv(
            f'{output_folder}/shares_terminal.csv',
            index=False
        )

        # =====================================================
        # VERIFICA FINALE
        # =====================================================
        print(f"\n✅ CONVERSIONE COMPLETATA")
        print(f"Cartella: {output_folder}")
        
        # Verifica consistenza
        print("\n=== VERIFICA CONSISTENZA ===")
        quarters_set = set(quarter_csv['quarterId:ID(Quarter)'])
        tx_quarters_set = set(tx['quarter_id'].unique())
        
        print(f"Quarter nodes nel file: {len(quarters_set)}")
        print(f"Quarter unici nelle transazioni: {len(tx_quarters_set)}")
        
        missing = tx_quarters_set - quarters_set
        if missing:
            print(f"⚠️  Quarter mancanti in nodes: {len(missing)}")
            print(f"Esempi: {list(missing)[:5]}")
        else:
            print("✅ Tutti i quarter delle transazioni hanno un nodo corrispondente")
        
        # Verifica relazioni
        rel_df = pd.read_csv(f'{output_folder}/transaction_quarter.csv')
        print(f"\nRelazioni Transaction->Quarter: {len(rel_df)}")

        return output_folder