celery -A core worker -l debug --concurrency 1 -S django
