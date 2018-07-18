#!/bin/bash

# virtual environment has to be activated for this!

export FLASK_APP="$PYTHONPATH/act/client/app.py"
python -m flask run
