# Local development

Use python version 3.10 for development.  

`pyenv` is the recommended method to install and manage python versions.

## To set up your local development environment run the following commands: 
```
python -m venv venv/
source venv/bin/activate
python -m pip install pip-tools
./update_deps
pip install -e ".[testing]"
```

## To update dependencies:
### DO NOT UPDATE requirements.txt or requirements.in manually!!!

1. If you have not setup your local environment run the steps described above.
  
2.  Activate your local environment in not already activated.
```
source venv/bin/activate
```
3. Make required changes to `pyproject.toml`
   
4. Generate a new version of requirements.in and requirements.txt
```
./update_deps
```
5. Apply updated requirements.txt to venv.
```
pip install -e ".[testing]"
```

## To verify formatting:
run `make lint`
 
To apply formatting changes use `make lint_apply`

Run unit tests:
`make test`
