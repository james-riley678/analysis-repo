# Baseline Engine

This service calulates baseling for certain metrics (Example energy)

# Author
James Riley

# Run
```bash
python -m pip install --upgrade pip

pip install pipenv

cd src && pipenv install

export PYTHON_CONFIG_DIR=config

export PYTHON_ENV=template

pipenv run build && pipenv run prod
```
Edit template.json file to the correct configuration

# Note 
This app is for python 3, anything below python 3.0 will not work for this app.
