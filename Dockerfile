# Polymarket collector image: python -m src.collector
#
# Built and pushed by .github/workflows/docker.yml; run on the VM with
# deploy/compose.yml. Only the collector's runtime dependencies are installed
# (requirements.txt); tests and notebooks are not in the image.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first: a code-only change reuses this layer from cache.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/

# Run as a normal user, uid 1000: the same uid as azureuser on the VM, so the
# mounted ~/polymarket/data is writable without chown.
RUN useradd --uid 1000 --create-home collector
USER 1000

# SIGTERM -> the collector stops, seals every segment and uploads what is left.
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "-m", "src.collector"]
