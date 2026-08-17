FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.7.21 /uv /uvx /bin/
COPY pyproject.toml README.md ./
RUN uv sync --no-dev --no-install-project
COPY src ./src
RUN uv sync --no-dev

EXPOSE 8000
CMD ["fmp-weknora-bridge"]

