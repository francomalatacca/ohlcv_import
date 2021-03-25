FROM python:3.9-slim

RUN apt-get update && apt-get -y install cron vim
WORKDIR /app
COPY crontab /etc/cron.d/crontab
COPY ohlcv_import.py /app/ohlcv_import.py
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

# run crond as main process of container
CMD ["cron", "-f"]