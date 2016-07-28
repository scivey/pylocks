test:
	py.test locks/*.py locks/**/*.py

clean:
	find locks -name "*.pyc" | xargs rm -f
	find locks -name "__pycache__" | xargs rm -rf
