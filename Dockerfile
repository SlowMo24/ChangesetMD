FROM python:3.12

WORKDIR /changesetmd

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY  queries.py .

COPY changesetmd.py .

ENTRYPOINT ["python", "changesetmd.py"]

