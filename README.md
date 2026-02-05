# Fraud Detection - Neo4j

Sistema per generare dataset fraud detection e eseguire query su Neo4j.

## Setup

```bash
make setup          # Installa dipendenze
make size-50mb      # Genera dataset 50MB
make start          # Avvia Neo4j
```

## Query

```bash
make query          # Esegui query 3.a, 3.b, 3.c
make extend         # Estendi DB (3.d, 3.e)
```

Output in `results/`:
- `query_3a.csv` - Customer con terminali condivisi
- `query_3b.csv` - Outlier per trimestre
- `query_3c.csv` - Co-customer network
- `stats_by_day.csv` - Statistiche per giorno settimana
- `execution_times.csv` - Tempi di esecuzione

## Comandi

```bash
# Dataset
make size-50mb      # 50MB
make size-100mb     # 100MB
make size-200mb     # 200MB

# Neo4j
make start          # Avvia
make stop           # Ferma
make reimport       # Cancella e reimporta

# Query
make query          # Query 3.a, 3.b, 3.c
make extend         # Estendi DB (3.d, 3.e)

# Utility
make clean          # Pulisci tutto
make logs           # Logs Neo4j
```

## Query Implementate

### Query 3.a - Customer Simili
Trova coppie di customer con ≥4 terminali condivisi e differenza transazioni ≤2.

### Query 3.b - Outlier
Transazioni >30% sopra mediana trimestre precedente.

### Query 3.c - Co-Customer Network
Customer raggiungibili tramite path di grado 3.

## Struttura

```
src/
├── converters.py       # Conversione DataFrame → CSV Neo4j
├── manager.py          # Gestione connessioni Neo4j
├── query_engine.py     # Esecuzione query e metriche
└── cli.py              # CLI

queries/
├── q1a.cypher          # Query 3.a
├── q1b.cypher          # Query 3.b
└── q1c.cypher          # Query 3.c
```

## Modello Neo4j

**Nodi**: Customer, Terminal, Transaction, Quarter  
**Relazioni Ottimizzate**:
- `SHARES_TERMINAL`: Customer↔Customer (precalcolata per Query 3.a)
- `USED_TERMINAL`: Customer→Terminal
- `IN_QUARTER`: Transaction→Quarter (con mediane precalcolate per Query 3.b)

## Neo4j

- Browser: http://localhost:7474
- Bolt: bolt://localhost:7687
- User: neo4j / StrongPassword123

