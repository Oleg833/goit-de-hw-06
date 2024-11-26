from kafka import KafkaConsumer, KafkaError
import logging

# Налаштування логера для запису повідомлень про помилки
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Створення консюмера з параметрами безпеки
    consumer = KafkaConsumer(
        "alert_Kafka_topic",  # Ім'я топіка, який ви хочете споживати
        bootstrap_servers="77.81.230.104:9092",  # Адреса вашого Kafka брокера
        security_protocol="SASL_PLAINTEXT",  # Протокол безпеки (SASL з передачею тексту)
        sasl_mechanism="PLAIN",  # Використовуємо SASL механізм 'PLAIN'
        sasl_plain_username="admin",  # Логін для автентифікації
        sasl_plain_password="VawEzo1ikLtrA8Ug8THa",  # Пароль для автентифікації
        auto_offset_reset="earliest",  # Почати читання з самого початку, якщо офсети недоступні
        enable_auto_commit=True,  # Автоматичне підтвердження повідомлень
        group_id="alert_consumer_group",  # Група консюмерів для координації читання
    )
except KafkaError as e:
    logger.error(f"Не вдалося створити консюмер: {e}")
    exit(1)

print("Починаємо читання повідомлень з топіка...")
try:
    for message in consumer:
        try:
            # Обробка повідомлення
            print(f"Отримано повідомлення: {message.value.decode('utf-8')}")
        except Exception as e:
            logger.error(f"Помилка при обробці повідомлення: {e}")
except KafkaError as e:
    logger.error(f"Помилка при читанні повідомлень з Kafka: {e}")
except KeyboardInterrupt:
    logger.info("Читання повідомлень було перервано користувачем.")
except Exception as e:
    logger.error(f"Непередбачена помилка: {e}")
finally:
    consumer.close()
