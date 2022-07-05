lint:
	flake8 analysis --max-line-length 100
	isort --check analysis ./*.py
	black --check analysis ./*.py                                            
	
lint_apply:
	flake8 analysis --max-line-length 100
	isort  analysis  ./*.py
	black  analysis ./*.py                                            