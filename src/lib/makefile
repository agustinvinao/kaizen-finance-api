sync:
	rsync -a . uqbar@tlon:/home/uqbar/hub/data/nodered/python-scripts/kaizenbrain

env_activate:
	source /data/node_modules/@background404/node-red-contrib-python-venv/pyenv/bin/activate

dist:
	python3 setup.py sdist

install:
	pip install -e .

cd-code:
	cd /data/python-scripts/kaizenbrain

nodered:
	docker-compose exec nodered bash