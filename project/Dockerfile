FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/producer.py .

# HuggingFace 캐시 디렉토리 생성 -- 흠...
# 컨테이너 시작 시 load_dataset()이 수백MB를 다운로드합니다. 캐시 설정 없으면 재시작마다 재다운로드.
ENV HF_HOME=/app/hf_cache

CMD ["python", "producer.py"]