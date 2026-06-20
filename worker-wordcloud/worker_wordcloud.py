"""
Worker Word Cloud
Tugas: Menerima pesan teks dari RabbitMQ, membuat gambar word cloud, dan mengupdate URL-nya ke PostgreSQL.
"""
import json
import os
import sys
import time
import logging

import pika
import psycopg2
from wordcloud import WordCloud
import matplotlib
matplotlib.use("Agg") # Backend non-GUI agar tidak butuh display server
import matplotlib.pyplot as plt

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger("worker_wordcloud")

# Konfigurasi dari Environment Variables (.env)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "secretpassword")

DB_HOST = os.getenv("DB_HOST", "pgbouncer")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "articleuser")
DB_PASS = os.getenv("DB_PASS", "articlepassword")
DB_NAME = os.getenv("DB_NAME", "articleswap")

# Nama exchange dan queue yang disepakati dengan Backend
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "article_exchange")
QUEUE_NAME = os.getenv("QUEUE_NAME", "wordcloud_queue")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")


def connect_rabbitmq(max_retries=10, retry_delay=5):
    """Membuat koneksi ke RabbitMQ dengan mekanisme retry."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
        heartbeat=600, blocked_connection_timeout=300,
    )
    for attempt in range(1, max_retries + 1):
        try:
            conn = pika.BlockingConnection(parameters)
            logger.info("Berhasil terhubung ke RabbitMQ")
            return conn
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Koneksi RabbitMQ gagal (percobaan {attempt}/{max_retries}): {e}")
            if attempt < max_retries: time.sleep(retry_delay)
            else: raise


def connect_postgres(max_retries=10, retry_delay=5):
    """Membuat koneksi ke PostgreSQL via PgBouncer dengan mekanisme retry."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
            )
            conn.autocommit = True
            logger.info("Berhasil terhubung ke PostgreSQL via PgBouncer")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Koneksi PostgreSQL gagal (percobaan {attempt}/{max_retries}): {e}")
            if attempt < max_retries: time.sleep(retry_delay)
            else: raise


def generate_wordcloud(text, article_id):
    """Membuat gambar word cloud dari teks dan menyimpannya sebagai PNG."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Konfigurasi visual word cloud
    wc = WordCloud(
        width=800, height=400, background_color="white", max_words=200,
        colormap="viridis", contour_width=1, contour_color="steelblue", min_font_size=10
    )
    wc.generate(text)

    # Simpan gambar ke dalam volume Docker
    filename = f"{article_id}.png"
    filepath = os.path.join(OUTPUT_DIR, filename)

    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Word cloud disimpan: {filepath}")
    return f"/output/{filename}"


def update_wordcloud_url(db_conn, article_id, image_url):
    """Menyimpan path URL gambar word cloud ke database."""
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "UPDATE articles SET wordcloud_url = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s::uuid",
                (image_url, article_id)
            )
        logger.info(f"Database diupdate: article_id={article_id}, wordcloud_url={image_url}")
    except psycopg2.Error as e:
        logger.error(f"Gagal update database untuk article_id={article_id}: {e}")
        raise


def on_message(channel, method, properties, body, db_conn):
    """Callback yang dipanggil ketika ada pesan masuk dari RabbitMQ."""
    try:
        # 1. Parse JSON pesan (Format: {"id": "uuid", "text": "isi artikel"})
        message = json.loads(body.decode("utf-8"))
        article_id = message.get("id")
        text = message.get("text", "")

        if not article_id or not text.strip():
            logger.warning("Payload pesan tidak valid. Diabaikan.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(f"Mulai memproses article_id={article_id}")
        
        # 2. Generate gambar dan simpan ke DB
        image_url = generate_wordcloud(text, article_id)
        update_wordcloud_url(db_conn, article_id, image_url)

        # 3. Tandai pesan selesai diproses (Ack)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Selesai memproses article_id={article_id}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON tidak valid: {e}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error saat memproses pesan: {e}", exc_info=True)
        # Jika error, kembalikan pesan ke antrean (Nack) agar dicoba lagi
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """Fungsi utama untuk menjalankan worker."""
    db_conn = connect_postgres()
    rmq_conn = connect_rabbitmq()
    channel = rmq_conn.channel()

    # Pastikan exchange dan queue siap
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)
    channel.basic_qos(prefetch_count=1)

    # Mulai mendengarkan pesan masuk
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, props, body: on_message(ch, method, props, body, db_conn),
        auto_ack=False
    )

    logger.info(f"Menunggu pesan di exchange '{EXCHANGE_NAME}'...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Worker dihentikan.")
        channel.stop_consuming()
    finally:
        rmq_conn.close()
        db_conn.close()

if __name__ == "__main__":
    main()
