FROM ghcr.io/astral-sh/uv:0.8.23-python3.13-trixie-slim

WORKDIR /usr/local/

ADD src src/
ADD pyproject.toml ./
ADD uv.lock ./
RUN uv sync --locked

CMD ["/usr/local/.venv/bin/fastapi", "run", "src/main.py", "--port", "8080", "--host", "0.0.0.0"]