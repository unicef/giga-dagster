#!/bin/bash

exec gunicorn main:app --bind 0.0.0.0:$PORT --config gunicorn.conf.py
