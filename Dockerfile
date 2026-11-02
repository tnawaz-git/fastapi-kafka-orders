FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /code

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY workers ./workers
COPY scripts ./scripts

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


FROM base AS test

COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt

COPY pytest.ini .
COPY tests ./tests

CMD ["pytest", "-v"]


# Keep the runtime image as the default build target.
FROM base AS runtime
