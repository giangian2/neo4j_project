#!/bin/bash
# custom-entrypoint.sh
# Sostituisce l'entrypoint default di Neo4j

set -e

echo "========================================"
echo "ENTRYPOINT PERSONALIZZATO NEO4J"
echo "========================================"

# Directory CSV
CSV_DIR="/init-csv"
DATA_DIR="/data/databases/neo4j"

# 1. Controlla se ci sono CSV da importare
if [ -f "${CSV_DIR}/customers.csv" ]; then
    echo "CSV rilevati in ${CSV_DIR}"
    echo "File trovati:"
    ls -la ${CSV_DIR}/*.csv 2>/dev/null | awk '{print "   - " $9}'
    
    # 2. Controlla se il database ESISTE già
    if [ -d "${DATA_DIR}" ] && [ "$(ls -A ${DATA_DIR} 2>/dev/null)" ]; then
        echo "Database già esistente, salto l'import"
    else
        echo "Database vuoto, avvio import con neo4j-admin..."
        
        # 3. Esegui neo4j-admin import
        START_TIME=$(date +%s)
        
        neo4j-admin database import full \
            --nodes="${CSV_DIR}/customers.csv" \
            --nodes="${CSV_DIR}/terminals.csv" \
            --nodes="${CSV_DIR}/transactions.csv" \
            --nodes="${CSV_DIR}/quarters.csv" \
            --relationships="${CSV_DIR}/cust_tx.csv" \
            --relationships="${CSV_DIR}/tx_term.csv" \
            --relationships="${CSV_DIR}/shares_terminal.csv" \
            --relationships="${CSV_DIR}/used_terminal.csv" \
            --relationships="${CSV_DIR}/transaction_quarter.csv" \
            --relationships="${CSV_DIR}/terminal_quarter.csv" \
            --skip-duplicate-nodes=true \
            --skip-bad-relationships=true \
            --bad-tolerance=10000 \
            --delimiter="," \
            --multiline-fields=true \
            neo4j --verbose
        
        IMPORT_RESULT=$?
        END_TIME=$(date +%s)
        
        if [ $IMPORT_RESULT -eq 0 ]; then
            echo "Import completato in $((END_TIME - START_TIME)) secondi"
            echo "Constraints verranno creati automaticamente all'avvio di Neo4j"
        else
            echo "Import fallito!"
            exit 1
        fi
    fi
else
    echo "⏭Nessun CSV da importare, avvio normale"
fi

# 5. Verifica che constraints.cypher sia presente
if [ -f "/var/lib/neo4j/init/constraints.cypher" ]; then
    echo "File constraints.cypher trovato, verrà eseguito automaticamente all'avvio"
fi


# Trova e chiama l'entrypoint originale di Neo4j
if [ -f "/startup/docker-entrypoint.sh" ]; then
    exec /startup/docker-entrypoint.sh neo4j
elif [ -f "/docker-entrypoint.sh" ]; then
    exec /docker-entrypoint.sh neo4j
else
    # Se non troviamo l'entrypoint, avviamo Neo4j direttamente
    exec /usr/bin/neo4j console
fi