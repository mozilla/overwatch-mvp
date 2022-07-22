lint:
	flake8 analysis tests --max-line-length 100
	isort --check --line-length 100 analysis tests ./*.py
	black --check analysis tests ./*.py                                            
	
lint_apply:
	flake8 analysis tests --max-line-length 100
	isort --line-length 100 analysis tests ./*.py
	black analysis tests ./*.py                                            

test:
	PYTHONPATH=. pytest