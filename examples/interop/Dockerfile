# PyWebTransport Interop Server Dockerfile

FROM python:3.14-slim-trixie

LABEL org.opencontainers.image.source="https://github.com/wtransport/pywebtransport"
LABEL org.opencontainers.image.description="WebTransport interoperability server"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY dist/*.whl /tmp/

RUN pip install --no-cache-dir /tmp/*.whl uvloop && rm /tmp/*.whl

COPY examples/interop/main.py ./main.py

EXPOSE 4433/udp

ENTRYPOINT ["python", "main.py"]