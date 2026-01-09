# Makefile - Fraud Detection
.PHONY: help setup size-50mb size-100mb size-200mb start stop reimport query extend clean logs

help:
	@echo "Comandi disponibili:"
	@echo "  make setup       - Setup ambiente"
	@echo "  make size-50mb   - Genera dataset 50MB"
	@echo "  make size-100mb  - Genera dataset 100MB"
	@echo "  make size-200mb  - Genera dataset 200MB"
	@echo "  make start       - Avvia Neo4j"
	@echo "  make stop        - Ferma Neo4j"
	@echo "  make reimport    - Cancella DB e reimporta"
	@echo "  make query       - Esegui query 3.a, 3.b, 3.c"
	@echo "  make extend      - Estendi DB (3.d, 3.e)"
	@echo "  make clean       - Pulisci tutto"
	@echo "  make logs        - Logs Neo4j"

setup:
	@python3 -m venv venv
	@venv/bin/pip install -q --upgrade pip
	@venv/bin/pip install -q -r requirements.txt
	@echo "✅ Setup completato"

size-50mb:
	@venv/bin/python src/generate.py generate --size 50MB --output init-data
	@echo "✅ Dataset 50MB generato"

size-100mb:
	@venv/bin/python src/generate.py generate --size 100MB --output init-data
	@echo "✅ Dataset 100MB generato"

size-200mb:
	@venv/bin/python src/generate.py generate --size 200MB --output init-data
	@echo "✅ Dataset 200MB generato"

start:
	@docker-compose up -d neo4j
	@echo "✅ Neo4j avviato (http://localhost:7474)"

stop:
	@docker-compose down

reimport:
	@docker-compose down -v
	@docker-compose up -d neo4j
	@echo "✅ Database reimportato"

query:
	@venv/bin/python src/generate.py query --all --output results
	@echo "✅ Query completate (vedi results/)"

extend:
	@venv/bin/python src/generate.py extend --output results
	@echo "✅ Estensione completata (vedi results/)"

clean:
	@docker-compose down -v
	@rm -rf init-data/*.csv results/*
	@echo "✅ Pulizia completata"

logs:
	@docker-compose logs -f neo4j
