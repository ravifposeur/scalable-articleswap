# Menggunakan versi slim Debian untuk menekan ukuran file
FROM python:3.10-slim

# Mencegah Python membuat file .pyc (cache) yang menuh-menuhin disk
ENV PYTHONDONTWRITEBYTECODE=1
# Memastikan log aplikasi langsung tampil di terminal Docker tanpa delay
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Menyalin daftar library yang dibutuhkan (requirements.txt)
COPY requirements.txt .

# Menginstall library (seperti pika untuk RabbitMQ, Sastrawi, psycopg2)
# --no-cache-dir mencegah pip menyimpan cache instalasi
RUN pip install --no-cache-dir -r requirements.txt

# Menyalin script buatan Teman 2
COPY . .

# Menjalankan worker
CMD ["python", "worker_stemming.py"]
