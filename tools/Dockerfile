FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

# Install deps from pyproject.toml before copying source so this layer is
# cached as long as dependencies don't change.
COPY pyproject.toml .
RUN mkdir -p tools && touch tools/__init__.py \
    && uv pip install --system --no-cache .

COPY __init__.py     tools/__init__.py
COPY lake.py         tools/lake.py
COPY bench.py        tools/bench.py
COPY reconstruct.py  tools/reconstruct.py
COPY skew_check.py   tools/skew_check.py

# Default entrypoint is the lake CLI; override with the bench service.
ENTRYPOINT ["python", "-m", "tools.lake"]
CMD ["list"]
