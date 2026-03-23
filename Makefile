.PHONY: install test lint validate deploy clean

install:
	pip install -r requirements.txt

test:
	pytest tests/unit/ -v --cov=jobs --cov-report=term-missing

lint:
	pylint jobs/ dags/ --fail-under=8.0 --disable=C0114,C0115,C0116,W0611
	black --check jobs/ dags/ tests/

format:
	black jobs/ dags/ tests/

validate:
	python - <<'EOF'
	import pandas as pd, great_expectations as gx, sys
	from great_expectations.core.batch import RuntimeBatchRequest
	ctx = gx.get_context(context_root_dir="great_expectations")
	df = pd.read_csv("data/sample/retail_orders.csv")
	br = RuntimeBatchRequest(
	    datasource_name="spark_datasource",
	    data_connector_name="runtime_data_connector",
	    data_asset_name="raw_orders",
	    runtime_parameters={"batch_data": df},
	    batch_identifiers={"run_id": "local_run"},
	)
	result = ctx.run_checkpoint(
	    checkpoint_name="raw_checkpoint",
	    validations=[{"batch_request": br, "expectation_suite_name": "retail_raw_suite"}]
	)
	sys.exit(0 if result.success else 1)
	EOF

deploy:
	./scripts/deploy_jobs.sh
	./scripts/deploy_dag.sh

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage coverage.xml
