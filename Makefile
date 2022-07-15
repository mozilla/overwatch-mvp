lint:
	flake8 analysis --max-line-length 100
	isort --check --line-length 100 analysis ./*.py
	black --check analysis ./*.py                                            
	
lint_apply:
	flake8 analysis --max-line-length 100
	isort --line-length 100 analysis ./*.py
	black analysis ./*.py                                            