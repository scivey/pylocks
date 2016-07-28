#!/bin/bash
ROOT=$(git rev-parse --show-toplevel)
export PYTHONPATH=${ROOT}
celery -A celery_app.conf worker -l info --concurrency 4

