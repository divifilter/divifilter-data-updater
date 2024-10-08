FROM python:3.13.0

ENV PYTHONUNBUFFERED=1

WORKDIR /divifilter

COPY . /divifilter

WORKDIR /divifilter

RUN pip install -r /divifilter/requirements.txt

CMD ["python", "update_divifilter_data.py"]