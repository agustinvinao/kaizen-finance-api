env:
	source .venv/bin/activate
server:
	python3 src/app.py
ui:
	python3 src/ui.py
install_local:
	pip install -e ./src/lib