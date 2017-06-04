build:
	pip3 install -r requirements.txt

test:
	python3 -m pytest tests

clean:
	echo "0">save.tmp
	>access.log
