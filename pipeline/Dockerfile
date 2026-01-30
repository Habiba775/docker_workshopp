FROM python:3.13.11
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# COPY pipeline.py .

# ENTRYPOINT [ "python","pipeline.py" ]