import numpy as np
import pandas as pd
import datetime
import time
import random
import os

def generate_customer_profiles_table(n_customers, random_state=0):
    np.random.seed(random_state)
    customer_id_properties=[]
    
    for customer_id in range(n_customers):
        x_customer_id = np.random.uniform(0,100)
        y_customer_id = np.random.uniform(0,100)
        mean_amount = np.random.uniform(5,100)
        std_amount = mean_amount/2
        mean_nb_tx_per_day = np.random.uniform(0,4)
        
        customer_id_properties.append([
            customer_id, x_customer_id, y_customer_id,
            mean_amount, std_amount, mean_nb_tx_per_day
        ])
    
    return pd.DataFrame(customer_id_properties, columns=[
        'CUSTOMER_ID', 'x_customer_id', 'y_customer_id',
        'mean_amount', 'std_amount', 'mean_nb_tx_per_day'
    ])

def generate_terminal_profiles_table(n_terminals, random_state=0):
    np.random.seed(random_state)
    terminal_id_properties=[]
    
    for terminal_id in range(n_terminals):
        x_terminal_id = np.random.uniform(0,100)
        y_terminal_id = np.random.uniform(0,100)
        terminal_id_properties.append([terminal_id, x_terminal_id, y_terminal_id])
    
    return pd.DataFrame(terminal_id_properties, columns=[
        'TERMINAL_ID', 'x_terminal_id', 'y_terminal_id'
    ])

def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
    x_y_customer = customer_profile[['x_customer_id','y_customer_id']].values.astype(float)
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))
    available_terminals = list(np.where(dist_x_y<r)[0])
    return available_terminals

def generate_transactions_table(customer_profile, start_date="2018-04-01", nb_days=10):
    customer_transactions = []
    
    random.seed(int(customer_profile.CUSTOMER_ID))
    np.random.seed(int(customer_profile.CUSTOMER_ID))
    
    for day in range(nb_days):
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)
        
        if nb_tx>0:
            for tx in range(nb_tx):
                time_tx = int(np.random.normal(86400/2, 20000))
                
                if (time_tx>0) and (time_tx<86400):
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)
                    
                    if amount<0:
                        amount = np.random.uniform(0, customer_profile.mean_amount*2)
                    
                    amount=np.round(amount,decimals=2)
                    
                    if len(customer_profile.available_terminals)>0:
                        terminal_id = random.choice(customer_profile.available_terminals)
                        
                        customer_transactions.append([
                            time_tx+day*86400, day,
                            customer_profile.CUSTOMER_ID,
                            terminal_id, amount
                        ])
    
    customer_transactions = pd.DataFrame(customer_transactions, columns=[
        'TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT'
    ])
    
    if len(customer_transactions)>0:
        customer_transactions['TX_DATETIME'] = pd.to_datetime(
            customer_transactions["TX_TIME_SECONDS"], unit='s', origin=start_date
        )
        customer_transactions = customer_transactions[[
            'TX_DATETIME','CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT',
            'TX_TIME_SECONDS', 'TX_TIME_DAYS'
        ]]
    
    return customer_transactions

def generate_dataset(n_customers=10000, n_terminals=1000000, nb_days=90, 
                     start_date="2018-04-01", r=5):
    start_time=time.time()
    customer_profiles_table = generate_customer_profiles_table(n_customers, random_state=0)
    print("Time to generate customer profiles table: {0:.2}s".format(time.time()-start_time))
    
    start_time=time.time()
    terminal_profiles_table = generate_terminal_profiles_table(n_terminals, random_state=1)
    print("Time to generate terminal profiles table: {0:.2}s".format(time.time()-start_time))
    
    start_time=time.time()
    x_y_terminals = terminal_profiles_table[['x_terminal_id','y_terminal_id']].values.astype(float)
    customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
        lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=r), axis=1
    )
    customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)
    print("Time to associate terminals to customers: {0:.2}s".format(time.time()-start_time))
    
    start_time=time.time()
    transactions_df = customer_profiles_table.groupby('CUSTOMER_ID').apply(
        lambda x: generate_transactions_table(x.iloc[0], nb_days=nb_days)
    ).reset_index(drop=True)
    
    print("Time to generate transactions: {0:.2}s".format(time.time()-start_time))
    
    transactions_df = transactions_df.sort_values('TX_DATETIME')
    transactions_df.reset_index(inplace=True, drop=True)
    transactions_df.reset_index(inplace=True)
    transactions_df.rename(columns={'index':'TRANSACTION_ID'}, inplace=True)
    
    return (customer_profiles_table, terminal_profiles_table, transactions_df)

def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):
    transactions_df['TX_FRAUD'] = 0
    transactions_df['TX_FRAUD_SCENARIO'] = 0
    
    transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD'] = 1
    transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD_SCENARIO'] = 1
    nb_frauds_scenario_1 = transactions_df.TX_FRAUD.sum()
    print("Number of frauds from scenario 1: " + str(nb_frauds_scenario_1))
    
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)
        compromised_transactions = transactions_df[
            (transactions_df.TX_TIME_DAYS >= day) &
            (transactions_df.TX_TIME_DAYS < day + 28) &
            (transactions_df.TERMINAL_ID.isin(compromised_terminals))
        ]
        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD'] = 1
        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD_SCENARIO'] = 2
    
    nb_frauds_scenario_2 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_1
    print("Number of frauds from scenario 2: " + str(nb_frauds_scenario_2))
    
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(
            n=3, random_state=day
        ).values
        compromised_transactions = transactions_df[
            (transactions_df.TX_TIME_DAYS >= day) &
            (transactions_df.TX_TIME_DAYS < day + 14) &
            (transactions_df.CUSTOMER_ID.isin(compromised_customers))
        ]
        nb_compromised_transactions = len(compromised_transactions)
        
        random.seed(day)
        index_frauds = random.sample(
            list(compromised_transactions.index.values),
            k=int(nb_compromised_transactions/3)
        )
        
        transactions_df.loc[index_frauds, 'TX_AMOUNT'] = transactions_df.loc[index_frauds, 'TX_AMOUNT'] * 5
        transactions_df.loc[index_frauds, 'TX_FRAUD'] = 1
        transactions_df.loc[index_frauds, 'TX_FRAUD_SCENARIO'] = 3
    
    nb_frauds_scenario_3 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_2 - nb_frauds_scenario_1
    print("Number of frauds from scenario 3: " + str(nb_frauds_scenario_3))
    
    return transactions_df