import pika
import psycopg2
import json
import os
import logging
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Sastrawi stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'pgbouncer')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

# RabbitMQ configuration
RMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RMQ_USER = os.getenv('RABBITMQ_USER')
RMQ_PASS = os.getenv('RABBITMQ_PASS')

# RabbitMQ queue name
QUEUE_NAME = 'article_queue'


def get_db_connection():
    """Membuat koneksi ke PostgreSQL melalui PgBouncer"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def update_article_stemmed(article_id, stemmed_content):
    """Memperbarui kolom stemmed_content dan status menjadi 'completed' di PostgreSQL"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to connect to database")
            return False

        cursor = conn.cursor()
        query = """
            UPDATE articles 
            SET stemmed_content = %s, status = 'completed', updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        cursor.execute(query, (stemmed_content, article_id))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Article {article_id} updated successfully with stemmed content")
        return True
    except Exception as e:
        logger.error(f"Error updating article {article_id}: {e}")
        return False


def update_article_status(article_id, status):
    """Memperbarui status artikel di PostgreSQL"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to connect to database")
            return False

        cursor = conn.cursor()
        query = """
            UPDATE articles 
            SET status = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        cursor.execute(query, (status, article_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error updating article status: {e}")
        return False


def process_message(ch, method, properties, body):
    """Callback untuk memproses pesan dari RabbitMQ"""
    try:
        # Parse JSON message
        message = json.loads(body)
        article_id = message.get('article_id')
        raw_content = message.get('raw_content')

        if not article_id or not raw_content:
            logger.error(f"Invalid message format: {message}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(f"Processing article {article_id}...")

        # Update status to processing
        update_article_status(article_id, 'processing')

        # Perform stemming
        stemmed_text = stemmer.stem(raw_content)
        logger.info(f"Stemming completed for article {article_id}")

        # Update database with stemmed content
        if update_article_stemmed(article_id, stemmed_text):
            logger.info(f"Article {article_id} processing completed successfully")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logger.error(f"Failed to update article {article_id}")
            # Negative acknowledge to requeue the message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledge to requeue the message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """Main function untuk menjalankan worker"""
    logger.info("Starting Worker Stemming...")

    # Setup RabbitMQ connection
    credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RMQ_HOST,
        port=5672,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Declare queue
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Set QoS (prefetch count)
        channel.basic_qos(prefetch_count=1)

        # Set up consumer
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=process_message
        )

        logger.info(f"Worker Stemming listening on queue '{QUEUE_NAME}'")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"Connection error: {e}")
    finally:
        try:
            connection.close()
        except:
            pass


if __name__ == "__main__":
    main()
