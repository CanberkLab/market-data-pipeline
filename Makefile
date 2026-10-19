up:        ## Tüm altyapıyı ayağa kaldır
	docker compose up -d --build
down:
	docker compose down -v
trigger:   ## DAG'i bugün için tetikle
	docker compose exec airflow airflow dags unpause market_data_pipeline
	docker compose exec airflow airflow dags trigger market_data_pipeline
psql:      ## Warehouse'a bağlan
	docker compose exec postgres-dw psql -U dw -d market_dw
test:      ## Unit testler (lokalde Java 11/17 + pip install -r requirements.txt gerekir)
	python -m pytest -q tests
