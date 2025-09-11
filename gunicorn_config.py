# gunicorn_config.py
import multiprocessing

# Server socket
bind = "0.0.0.0:10000"  # Render provides the port, but 10000 is a common default

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "gthread"
threads = 2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Timeout
timeout = 120