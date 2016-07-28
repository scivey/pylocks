test:
	py.test pylocks/*.py pylocks/**/*.py

clean:
	find pylocks -name "*.pyc" | xargs rm -f
	find pylocks -name "__pycache__" | xargs rm -rf
