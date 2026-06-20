FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Menginstall dependensi sistem operasi yang dibutuhkan matplotlib & wordcloud
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/* # Membersihkan cache apt agar image tetap kecil

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Menyalin script buatan Teman 3
COPY . .

# Menjalankan worker
CMD ["python", "-u", "worker_wordcloud.py"]
