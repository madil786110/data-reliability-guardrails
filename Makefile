up:
	docker compose up -d
	@echo "Waiting for services..."
	sleep 5
	# Initialize reference data
	python3 -m drg.cli init

down:
	docker compose down

demo-good:
	@echo "Running Good Pipeline..."
	$(eval RUN_ID := $(shell uuidgen))
	@echo "Run ID: $(RUN_ID)"
	python3 -m drg.cli ingest --run-id $(RUN_ID) --seed 123
	python3 -m drg.cli validate --run-id $(RUN_ID)
	python3 -m drg.cli downstream run --run-id $(RUN_ID)

demo-bad:
	@echo "Running Bad Pipeline..."
	$(eval RUN_ID := $(shell uuidgen))
	@echo "Run ID: $(RUN_ID)"
	# Inject schema drift
	python3 -m drg.cli ingest --run-id $(RUN_ID) --inject schema_drift --seed 666
	# Validation should fail
	-python3 -m drg.cli validate --run-id $(RUN_ID)
	# Downstream should be blocked (will exit 1)
	-python3 -m drg.cli downstream run --run-id $(RUN_ID)

replay:
	@echo "Replaying Run..."
	@read -p "Enter Run ID to replay: " RUN_ID; \
	python3 -m drg.cli replay --run-id $$RUN_ID --fix clean

bench:
	@echo "Running Benchmarks..."
	python3 -m drg.bench.runner

test:
	pytest tests/

clean:
	rm -rf data/raw/*
	rm -rf data/reference/*
	find . -type d -name "__pycache__" -exec rm -rf {} +
