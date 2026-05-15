.PHONY: run venv deps

VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

venv:
	python3 -m venv $(VENV)

deps: venv
	$(PIP) -q install -r requirements.txt

run: deps
	NO_PROXY=127.0.0.1,localhost $(VENV)/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
