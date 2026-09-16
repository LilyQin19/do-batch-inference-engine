.PHONY: dev test lint typecheck run demo memory-probe

dev:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy --strict src/

test:
	pytest --cov=src/batchengine --cov-report=term --cov-fail-under=85

run:
	uvicorn batchengine.main:app --reload --port 8000

demo:
	python scripts/generate_batch.py --n 1000
	uvicorn batchengine.main:app --port 8000 &
	sleep 1
	curl -s -X POST localhost:8000/job -H "Content-Type: application/json" \
		-d '{"input_path": "data/sample_batch.json", "concurrency": 4}'

memory-probe:
	python scripts/memory_probe.py
