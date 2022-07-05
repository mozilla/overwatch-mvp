# Development

Use python version 3.10.4 for development.  

`pyenv` is the recommended method to install and manage python versions.

Create a virtual env using venv and install the requirements.txt.
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

To verify formatting:
`make lint`
 
To apply formatting changes use `make lint_apply`