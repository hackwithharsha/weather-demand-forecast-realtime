FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml .
COPY __init__.py tools/__init__.py
COPY lake.py tools/lake.py

RUN uv pip install --system "boto3>=1.34" "duckdb>=1.0" "pandas>=2.0"

ENTRYPOINT ["python", "-m", "tools.lake"]
CMD ["list"]
