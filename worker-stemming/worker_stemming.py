import json
import logging
import os
import sys
import time

import pika
import psycopg2
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

# Setup logging ke terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("worker_stemming")

# Inisialisasi Sastrawi Stemmer
logger.info("Initializing Sastrawi Stemmer engine...")
factory = StemmerFactory()
stemmer = factory.create_stemmer()

# Konfigurasi dinamis penuh dari .env
DB_HOST = os.getenv("DB_HOST", "pgbouncer")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "articleswap")
DB_USER = os.getenv("DB_USER", "articleuser")
DB_PASS = os.getenv("DB_PASS", "articlepassword")

RMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RMQ_PASS = os.getenv("RABBITMQ_PASS", "secretpassword")

EXCHANGE_NAME = os.getenv("RABBITMQ_EXCHANGE", "articles_exchange")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_STEMMING", "stemming_queue")


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


def update_article_stemmed(article_id, stemmed_content):
    """Menyimpan hasil stemming ke DB dan mengubah status"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cursor:
            query = """
                UPDATE articles
                SET stemmed_content = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s::uuid
            """
            cursor.execute(query, (stemmed_content, article_id))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating article {article_id} in DB: {e}")
        return False
    finally:
        conn.close()


def process_message(ch, method, properties, body):
    """Callback ketika ada pesan masuk dari Fanout Exchange Go"""
    try:
        # Sinkronisasi format JSON dari Go Backend: {"id": "uuid", "text": "isi"}
        message = json.loads(body.decode("utf-8"))
        article_id = message.get("id")
        raw_content = message.get("text")

        if not article_id or not raw_content:
            logger.warning(f"Payload pesan cacat / tidak valid. Diabaikan: {message}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(f"[*] Article [{article_id}] -> Memulai proses stemming NLP...")

        # Eksekusi algoritma Sastrawi
        stemmed_text = stemmer.stem(raw_content)
        logger.info(f"[+] Article [{article_id}] -> Stemming selesai.")

        # Update database
        if update_article_stemmed(article_id, stemmed_text):
            logger.info(f"[✓] Article [{article_id}] -> Berhasil disimpan ke DB.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logger.error(
                f"[x] Article [{article_id}] -> Gagal simpan ke DB. Requeue pesan."
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except json.JSONDecodeError as e:
        logger.error(f"Format JSON error: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error tidak terduga saat memproses pesan: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    logger.info("Starting Worker Stemming Service...")

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

            # Pastikan Exchange dan Queue terbentuk & terikat (Bound) satu sama lain
            channel.exchange_declare(
                exchange=EXCHANGE_NAME, exchange_type="fanout", durable=True
            )
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)

            # Prefetch count = 1 agar pembagian kerja seimbang jika container di-scale
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

            logger.info(f"🚀 Worker Stemming mendengarkan antrean '{QUEUE_NAME}'...")
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
