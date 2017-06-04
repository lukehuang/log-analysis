build:
	pip install -r requirements.txt

test:
	py.test test

clean:
	echo "0">save.tmp
	>access.log
