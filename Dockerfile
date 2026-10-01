FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY common.py collector.py compact.py ./
# El comando real lo define cada servicio en Railway (railway.*.json)
CMD ["python", "collector.py"]
