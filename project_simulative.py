import requests
import logging
import psycopg2
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import gspread
import smtplib
import ssl
from email.message import EmailMessage

"""Параметры подключения к БД"""
HOST = "localhost"
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "postgres"

"""Параметры отправки email"""
SMTP_CONFIG = {
    "SENDER_EMAIL": "api5mtp@yandex.ru",
    "EMAIL_PASSWORD": "etjarzcyhvwdwzrs",
    "SMTP_SERVER": "smtp.yandex.ru",
    "SMTP_PORT": 465,
    "RECEIVER_EMAIL": "nf111@mail.ru",
}

"""Параметры подключения к Google Sheets"""
credentials = {
    "type": "service_account",
    "project_id": "first-api-project-450210",
    "private_key_id": "bf3bde7be7080cddef5da77f874ef49f232c802a",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCqNNfm3a+5APZ8\nnD44gakH+La77mfCjMh12IC2oXXVy2/vuZctCyk3sEfH575QH2ea+MCVLaT7w+tp\njcniS1vIqNUmFL4+4vQUgtkYaQNhc1DezQYqybZjfGN4PRuF9M+gkfM3bv4VUsUL\nPaIEhG+bbeoYZcYk0yL9ANtZ4lXSsRliBdMljIvWJmC6VNMSznNCnUHp7+I3r1D0\nveO4wkKI7WCnMhKtocwrwzhBDweitfHrnOEDRgqqLnd36coGjc4c1e2dgMEPnNyk\nT+Nvwt/6vd4b6SgduH7nWEluzQLBLFBNA9EGaxsQwvXrvDh4O6QXlhjEaV13arJ9\ntL1g/jnvAgMBAAECggEAKwm0pCRfKBUVvpmVZB1pG+JodmnIo9mMYLwPg96c2KQZ\n2FCh6NpfID07UVGIfmMKhBWtQKtrgdve/XBEYauzeDA0wW3SUz15AWy6r2DyYWRS\nyRunmQ8rRsmvrzooaP1nuM7e5J/0sHie6YI6oxDH1dH7tRKbATnAKaBbVsWT9PO3\n2IVNcZSuw0TaZg4Q+1HBix8u6+DGCbjzQCva3z3PZxMzz/wPcWb2YOzLADPFhHyB\nnjX/LOHLSdIsaglLcabmZst6hIQ2Vo8plHmt7UuO3EPiBcQXkwnC9/JXwGJ4hp3F\nVfbd9gLyddj7lLb1cGi8Uzy/JEDRJZ1RuqfL8STSlQKBgQDqF8JEgYNkOQBLG6pn\n5AQhmIAr5ORCJu2KGPBqm+sDXpXelujkp5Wz7H1HFqD9qONbCJlaTfpzfpTZLOXx\n8V1Bay2j71rrUDV26ZQowqP3HfEBZ+ra2GhG1/rI2YjbnUfqhdDmAQvMiOsBmZYk\nqF8Tyj/Kn0Pikj90JWkYUmiFPQKBgQC6IolaTdUydZGUBrPDqAwfbcbyNbZEeRwa\nVr7du6szoJqd5/fOtXCgmESHmbjlyNIOCROOM9MFt0aQrBSi9wWOb8XKADpx8rMd\nWD850rjPXDiV9KpfyoiU6IQSygS3WQ9x2uB05+YF90HGlVsGFJ5tZ67VRUWHZs2X\nV6ll/5CmmwKBgH78Om7tDrhsT2Nu84Z941vSHR0ygjv8X/j8xFYglGD0izn/BQEc\ng5HhpKfOd7CCUCrOxFl/WXATZ21T9LzFIMBfApgePGTP0uDZnnxp8YYY4ObRV041\n/IMJoEoZ7yqq7BslUgei46KlV0474X2rNEBJA0fIE4wxp2g67wK5mWdFAoGAP1Ue\n/Q38m6WrOf2pzsnwDgWRKrqh4NyAdmnLmbCQSQm9cjsKc1gDEXGd57GjWvQgnMvz\nzm/NvmUk6nbSsrxwNI3Nc7TuLfVRgouTP752SX/sdQGBswQ7wsb2oVoBjs0L10aN\nFtoMxzzhMYuCJCiB22Hq+AAQ70hIbdVS+zSg6D8CgYAfZM5tr7fRs3XGWiqn0eF4\nu/Sapwj+KAOLyZ3i3VnnLSLb9fFSMJbZPenRlr5pO77yf6EgupQa+hOnTGYVkHhg\nYaIGj9mI+4s/J2ie+mKih324x8nISPtMoK7ttQbpUAveie13Kwg4G1bShBV7sncR\nmOz3FrzQ7t25oGkDPWsp4A==\n-----END PRIVATE KEY-----\n",
    "client_email": "api-service-account@first-api-project-450210.iam.gserviceaccount.com",
    "client_id": "113264450427209421860",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/api-service-account%40first-api-project-450210.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}

current_date = datetime.now()

api_url = "https://b2b.itresume.ru/api/statistics"


def cleanup_old_logs(log_dir=".", days_to_keep=3):
    """Удаляет лог-файлы старше указанного количества дней"""

    for file in Path(log_dir).glob("*.log"):
        try:
            # Получаем дату из имени файла
            file_date = datetime.strptime(file.stem, "%Y-%m-%d")
            # Если файл старше заданного периода - удаляем
            if (current_date - file_date).days > days_to_keep:
                os.remove(file)
                logging.info(f"Удален старый лог-файл: {file}")
        except Exception as e:

            logging.error(f"Ошибка при удалении старого лог-файла {file}: {e}")


# Получаем абсолютный путь к директории скрипта
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

# Очищаем старые логи при запуске
cleanup_old_logs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log"),
    filemode="a",
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    level=logging.INFO,
)


class Attempt:
    def __init__(
        self,
        user_id,
        oauth_consumer_key,
        lis_result_sourcedid,
        lis_outcome_service_url,
        is_correct,
        attempt_type,
        created_at,
    ):
        self.user_id = user_id
        self.oauth_consumer_key = oauth_consumer_key
        self.lis_result_sourcedid = lis_result_sourcedid
        self.lis_outcome_service_url = lis_outcome_service_url
        self.is_correct = is_correct
        self.attempt_type = attempt_type
        self.created_at = created_at

    def get_user_id(self):
        return self.user_id

    def get_oauth_consumer_key(self):
        return self.oauth_consumer_key

    def get_lis_result_sourcedid(self):
        return self.lis_result_sourcedid

    def get_lis_outcome_service_url(self):
        return self.lis_outcome_service_url

    def get_is_correct(self):
        return self.is_correct

    def get_attempt_type(self):
        return self.attempt_type

    def get_created_at(self):
        return self.created_at


class DatabaseConnection:
    _instance = None
    _connection = None
    _cursor = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._connect()
        return cls._instance

    def _connect(self):
        """Создание подключения и курсора"""
        try:
            self._connection = psycopg2.connect(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
            )

            self._cursor = self._connection.cursor()
            logging.info("Подключение к БД успешно установлено")
        except psycopg2.Error as e:
            logging.error(f"Ошибка подключения к БД: {e}", exc_info=True)
            raise

    def execute_query(self, query, params=None):
        """Выполнение SQL запроса"""
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
            # Возвращаем результат только если это SELECT запрос
            if query.strip().upper().startswith("SELECT"):
                return self._cursor.fetchall()
            return None

        except psycopg2.Error as e:
            self._connection.rollback()
            logging.error(f"Ошибка выполнения запроса: {e}", exc_info=True)
            raise

    def close_connection(self):
        """Закрытие курсора и соединения"""
        try:
            if self._cursor:
                self._cursor.close()
            if self._connection:
                self._connection.close()
                logging.info("Соединение с БД закрыто")
        except psycopg2.Error as e:
            logging.error(f"Ошибка при закрытии соединения: {e}")
            raise

    def __del__(self):
        """Деструктор для автоматического закрытия соединения"""
        self.close_connection()


def get_last_date():
    db = DatabaseConnection()
    last_date = db.execute_query("SELECT MAX(created_at) FROM attempts")
    logging.info(f"Последняя дата в БД: {last_date[0][0]}")
    return last_date[0][0]


def get_data(url, params):
    logging.info(
        f"Получаем данные из API за период: {params['start']} - {params['end']}"
    )
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Получено {len(data)} записей из API")
        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении данных: {e}", exc_info=True)
        return None


def process_data(data):

    logging.info("Обрабатываем данные")
    processed_data = []

    for dct in data:
        try:
            # Пробуем распарсить passback_params, если не получается - используем None
            try:
                passback_params = (
                    json.loads(dct["passback_params"].replace("'", '"'))
                    if dct.get("passback_params")
                    else {}
                )
            except (json.JSONDecodeError, AttributeError) as e:
                logging.warning(f"Ошибка парсинга passback_params: {e}")
                passback_params = {}

            # Безопасное получение значений с дефолтом None
            is_correct = (
                bool(dct["is_correct"]) if dct.get("is_correct") is not None else None
            )

            attempt = Attempt(
                user_id=dct.get("lti_user_id"),
                oauth_consumer_key=passback_params.get("oauth_consumer_key", None),
                lis_result_sourcedid=passback_params.get("lis_result_sourcedid", None),
                lis_outcome_service_url=passback_params.get(
                    "lis_outcome_service_url", None
                ),
                is_correct=is_correct,
                attempt_type=dct.get("attempt_type"),
                created_at=(
                    datetime.strptime(dct["created_at"], "%Y-%m-%d %H:%M:%S.%f")
                    if dct.get("created_at")
                    else None
                ),
            )

            processed_data.append(attempt)

        except Exception as e:
            logging.error(f"Ошибка при обработке данных: {e}", exc_info=True)

            continue
    logging.info(f"Обработано {len(processed_data)} строк")
    return processed_data


def load_data(data):
    logging.info(f"Начинаем загрузку {len(data)} записей в БД")
    success_count = 0
    error_count = 0
    db = DatabaseConnection()

    for attempt in data:
        try:
            params = (
                attempt.user_id,
                attempt.oauth_consumer_key,
                attempt.lis_result_sourcedid,
                attempt.lis_outcome_service_url,
                attempt.is_correct,
                attempt.attempt_type,
                attempt.created_at,
            )

            db.execute_query(
                """
                INSERT INTO attempts (
                    user_id, oauth_consumer_key, lis_result_sourcedid, 
                    lis_outcome_service_url, is_correct, attempt_type, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                params,
            )
            success_count += 1
        except Exception as e:
            error_count += 1
            logging.error(f"Ошибка загрузки данных: {str(e)}", exc_info=True)
            continue

    logging.info(f"Загрузка завершена. Успешно: {success_count}, Ошибок: {error_count}")
    db.close_connection()


def daily_report(data):
    date = datetime.now().strftime("%Y-%m-%d")
    amount_of_attempts = len(data)
    amount_of_correct_attempts = len(
        [attempt for attempt in data if attempt.is_correct]
    )
    amount_of_unique_users = len(set([attempt.user_id for attempt in data]))
    values = [
        date,
        amount_of_attempts,
        amount_of_correct_attempts,
        amount_of_unique_users,
    ]

    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open("API-Daily-Report")
    wks = sh.sheet1
    wks.insert_row(values, 2)
    logging.info("Данные успешно записаны в Google Sheets")
    return amount_of_attempts, amount_of_correct_attempts, amount_of_unique_users


def send_email(
    subject, message_body, receiver_email, smtp_server, port, sender_email, password
):
    message = EmailMessage()
    message.set_content(message_body)
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:

            server.login(sender_email, password)
            server.send_message(message)
    except Exception as e:
        logging.error(f"Ошибка при отправке email: {e}", exc_info=True)
    else:
        logging.info("Email отправлен успешно")


def main():
    start_time = datetime.now()
    logging.info("=" * 50)
    logging.info("Начало выполнения программы")

    DATE_FROM = (get_last_date() + timedelta(microseconds=1)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    DATE_TO = current_date.strftime("%Y-%m-%d %H:%M:%S.%f")

    EMAIL_SUBJECT = f"Отчет за {DATE_FROM} - {DATE_TO}"

    params = {
        "client": "Skillfactory",
        "client_key": "M2MGWS",
        "start": DATE_FROM,
        "end": DATE_TO,
    }

    data = get_data(api_url, params)
    if data:
        data = process_data(data)
        load_data(data)
        amount_of_attempts, amount_of_correct_attempts, amount_of_unique_users = (
            daily_report(data)
        )
        message_body = f"Отчет за {DATE_FROM} - {DATE_TO}.\nКоличество попыток: {amount_of_attempts}.\nКоличество правильных попыток: {amount_of_correct_attempts}.\nКоличество уникальных пользователей: {amount_of_unique_users}."
        try:
            send_email(
                smtp_server=SMTP_CONFIG["SMTP_SERVER"],
                port=SMTP_CONFIG["SMTP_PORT"],
                sender_email=SMTP_CONFIG["SENDER_EMAIL"],
                password=SMTP_CONFIG["EMAIL_PASSWORD"],
                receiver_email=SMTP_CONFIG["RECEIVER_EMAIL"],
                subject=EMAIL_SUBJECT,
                message_body=message_body,
            )
        except Exception as e:
            logging.error(f"Ошибка: {e}")

    end_time = datetime.now()
    execution_time = end_time - start_time
    logging.info(f"Программа завершена. Время выполнения: {execution_time}")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()
