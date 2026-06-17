.PHONY: verify test lint coverage ci

verify:
	python tools/verify_pipeline.py

test:
	python -m pytest tests/ -v --timeout=120 --no-cov

lint:
	python -m flake8 strategy/ risk/ evaluation/ governance/ --max-line-length=120 --exclude=__pycache__

coverage:
	python -m pytest tests/ --cov=. --cov-report=term --cov-report=html --no-cov-on-fail

ci: verify test
	@echo "CI 全绿"
