import json
import logging
import os
import sys
import time

import matplotlib
import pika
import psycopg2
from wordcloud import WordCloud

matplotlib.use("Agg")  # Mode headless agar aman berjalan di dalam Docker Linux
import matplotlib.pyplot as plt

# Setup logging ke terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("worker_wordcloud")

# Konfigurasi dinamis penuh dari .env
DB_HOST = os.getenv("DB_HOST", "pgbouncer")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "articleuser")
DB_PASS = os.getenv("DB_PASS", "articlepassword")
DB_NAME = os.getenv("DB_NAME", "articleswap")

RMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RMQ_PASS = os.getenv("RABBITMQ_PASS", "secretpassword")

EXCHANGE_NAME = os.getenv("RABBITMQ_EXCHANGE", "articles_exchange")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_WORDCLOUD", "wordcloud_queue")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")


def get_db_connection():
    """Membuat koneksi ke PostgreSQL via PgBouncer"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def generate_wordcloud(text, article_id):
    """Membuat file gambar Word Cloud PNG berdasarkan isi teks"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Kustomisasi visualisasi grafik wordcloud
    wc = WordCloud(
        width=800,
        height=400,
        background_color="white",
        max_words=150,
        colormap="viridis",
        contour_width=1,
        contour_color="steelblue",
    )
    wc.generate(text)

    filename = f"{article_id}.png"
    filepath = os.path.join(OUTPUT_DIR, filename)

    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"[+] Word cloud image successfully saved at: {filepath}")
    return f"/output/{filename}"


def update_wordcloud_url(article_id, image_url):
    """Menyimpan jalur path/URL gambar ke PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cursor:
            query = """
                UPDATE articles
                SET wordcloud_url = %s, status = 'completed', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid
            """
            cursor.execute(query, (image_url, article_id))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating wordcloud URL for article {article_id}: {e}")
        return False
    finally:
        conn.close()


def on_message(channel, method, properties, body):
    """Callback pengolah antrean dari RabbitMQ"""
    try:
        # Sinkronisasi format JSON dari Go Backend: {"id": "uuid", "text": "isi"}
        message = json.loads(body.decode("utf-8"))
        article_id = message.get("id")
        text = message.get("text", "")

        if not article_id or not text.strip():
            logger.warning(
                "Payload tidak valid atau teks kosong. Mengirim ACK abaikan."
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(
            f"[*] Article [{article_id}] -> Memulai proses visualisasi Word Cloud..."
        )

        # Eksekusi generator gambar dan update DB
        image_url = generate_wordcloud(text, article_id)

        if update_wordcloud_url(article_id, image_url):
            logger.info(
                f"[✓] Article [{article_id}] -> Visualisasi selesai & DB diperbarui."
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logger.error(
                f"[x] Article [{article_id}] -> Gagal update URL ke DB. Requeue pesan."
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(
            f"Error sistem saat memproses gambar wordcloud: {e}", exc_info=True
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    logger.info("Starting Worker Word Cloud Service...")

    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=RMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Deklarasikan ulang infrastruktur AMQP untuk memastikan keselarasan
            channel.exchange_declare(
                exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True
            )
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False
            )

            logger.info(f"🚀 Worker Word Cloud mendengarkan antrean '{QUEUE_NAME}'...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            logger.warning(
                "Koneksi ke RabbitMQ terputus. Mencoba menyambung ulang dalam 5 detik..."
            )
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Worker dihentikan oleh pengguna.")
            break


if __name__ == "__main__":
    main()
