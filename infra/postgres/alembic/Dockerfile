FROM python:3.12-slim

WORKDIR /alembic

COPY . .

RUN pip install --no-cache-dir alembic sqlalchemy psycopg2-binary

ENTRYPOINT ["alembic"]
CMD ["upgrade", "head"]
