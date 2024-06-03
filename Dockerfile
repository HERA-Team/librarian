FROM python:3.11

COPY . librarian
RUN pip install --no-cache-dir --upgrade ./librarian
RUN pip install --no-cache-dir --upgrade "psycopg[binary,pool]"

CMD ["librarian-server-start"]