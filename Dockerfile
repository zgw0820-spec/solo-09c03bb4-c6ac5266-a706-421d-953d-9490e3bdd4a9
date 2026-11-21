FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd -r -u 10001 appuser && mkdir -p /data && chown appuser:appuser /data
USER appuser

ENV DATA_DIR=/data \
    ZONE_COUNT=4 \
    ZONE_CAPACITY_BYTES=131072 \
    RESERVED_FREE_ZONES=1 \
    GC_STEP_DELAY_MS=0 \
    READ_DELAY_MS=0

EXPOSE 8000
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# 单实例单进程：状态在进程内，由 asyncio 锁串行化
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
