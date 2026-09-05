PYTHON ?= .venv/bin/python
RUFF ?= .venv/bin/ruff
PYTHONPYCACHEPREFIX ?= .venv/pycache

.PHONY: test lint check video prepare-llava clean-pycache

test:
	PYTHONPYCACHEPREFIX=$(PYTHONPYCACHEPREFIX) $(PYTHON) -m pytest -q

lint:
	PYTHONPYCACHEPREFIX=$(PYTHONPYCACHEPREFIX) $(RUFF) check .

check: test lint

video:
	PYTHONPYCACHEPREFIX=$(PYTHONPYCACHEPREFIX) $(PYTHON) -m curator_flow.run_video_pipelines $(ARGS)

prepare-llava:
	PYTHONPYCACHEPREFIX=$(PYTHONPYCACHEPREFIX) $(PYTHON) scripts/data/prepare_llava_video_shard.py $(ARGS)

clean-pycache:
	find . -path './.venv' -prune -o -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -path './.venv' -prune -o -type f -name '*.py[co]' -delete
