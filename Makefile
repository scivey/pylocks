test:
	py.test pylocks

clean:
	find pylocks -name "*.pyc" | xargs rm -f
	find pylocks -name "__pycache__" | xargs rm -rf
	find pylocks -name ".cache" | xargs rm -rf
	rm -rf dist *.egg-info .cache *.pyc __pycache__

release:
	python setup.py sdist upload


pyflakes:
	pyflakes pylocks
