FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy pandas numpy pymysql cryptography certifi cffi chardet idna PGPy protobuf pyasn1 pycparser requests selenium six urllib3 PyYAML openpyxl

WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]