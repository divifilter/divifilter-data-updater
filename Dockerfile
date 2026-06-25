FROM python:3.14.6

ENV PYTHONUNBUFFERED=1

WORKDIR /divifilter

COPY . /divifilter

WORKDIR /divifilter

RUN pip install -r /divifilter/requirements.txt

# Liveness check: the run loop stamps a heartbeat file each iteration; this fails
# only if the loop hasn't progressed within its max inter-run delay (+ margin),
# i.e. it has hung. Tune via HEALTHCHECK_MAX_STALENESS_SECONDS.
HEALTHCHECK --interval=5m --timeout=15s --start-period=5m --retries=3 \
    CMD ["python", "/divifilter/healthcheck.py"]

CMD ["python", "update_divifilter_data.py"]
