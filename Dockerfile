FROM python:3.11.5

WORKDIR /divifilter

COPY . /divifilter

WORKDIR /divifilter

RUN pip install -r /divifilter/requirements.txt

CMD ["python", "update_divifilter_data.py"]