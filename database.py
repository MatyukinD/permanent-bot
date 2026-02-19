import sqlite3
import datetime

# Регистрация адаптеров для корректной работы с датами в Python 3.12+
def adapt_date_iso(val):
    return val.isoformat()

def adapt_datetime_iso(val):
    return val.isoformat(" ")

def convert_date(val):
    return datetime.date.fromisoformat(val.decode())

def convert_datetime(val):
    return datetime.datetime.fromisoformat(val.decode())

sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)

# Подключение к базе данных (файл создастся автоматически)
def get_db():
    conn = sqlite3.connect('bot_database.db', detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn

# Создание таблиц и обновление схемы при первом запуске
def init_db():
    conn = get_db()
    cur = conn.cursor()

    # Таблица пользователей
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            phone TEXT,
            registered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Добавляем поля, если их нет
    cur.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cur.fetchall()]
    if 'age' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN age INTEGER")
    if 'survey_data' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN survey_data TEXT")

    # Таблица мастеров
    cur.execute('''
        CREATE TABLE IF NOT EXISTS masters (
            master_id INTEGER PRIMARY KEY,
            user_id INTEGER UNIQUE,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')

    # Таблица слотов (свободное время для записи)
    # Статусы: free, booked, confirmed, completed
    cur.execute('''
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_id INTEGER,
            zone TEXT,  -- может быть NULL (универсальный слот) или конкретная зона
            slot_time TIMESTAMP,
            status TEXT DEFAULT 'free',
            user_id INTEGER,
            price INTEGER,  -- стоимость процедуры
            is_primary INTEGER DEFAULT 1,  -- 1 - первичная, 0 - коррекция
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (master_id) REFERENCES masters(master_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')
    # Проверяем наличие новых полей в appointments
    cur.execute("PRAGMA table_info(appointments)")
    app_columns = [col[1] for col in cur.fetchall()]
    if 'price' not in app_columns:
        cur.execute("ALTER TABLE appointments ADD COLUMN price INTEGER")
    if 'is_primary' not in app_columns:
        cur.execute("ALTER TABLE appointments ADD COLUMN is_primary INTEGER DEFAULT 1")

    # Таблица отзывов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            zone TEXT,
            review_text TEXT,
            rating INTEGER CHECK(rating >= 1 AND rating <= 5),
            photo TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')
    # Проверяем наличие поля photo (для старых баз)
    cur.execute("PRAGMA table_info(reviews)")
    rev_columns = [col[1] for col in cur.fetchall()]
    if 'photo' not in rev_columns:
        cur.execute("ALTER TABLE reviews ADD COLUMN photo TEXT")

    # Таблица для временного хранения ответов опросника
    cur.execute('''
        CREATE TABLE IF NOT EXISTS survey_answers (
            user_id INTEGER PRIMARY KEY,
            zone TEXT,
            step INTEGER,
            answers TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Таблица для напоминаний о коррекции
    cur.execute('''
        CREATE TABLE IF NOT EXISTS correction_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            appointment_id INTEGER,
            reminder_date TIMESTAMP,
            sent INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (appointment_id) REFERENCES appointments(id)
        )
    ''')

    # Таблица для истории с фотографиями (до/после)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS history_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appointment_id INTEGER,
            user_id INTEGER,
            photo_path TEXT,
            photo_type TEXT CHECK(photo_type IN ('before', 'after')),
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (appointment_id) REFERENCES appointments(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')

    # Таблица для ручных записей (не через бота)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS manual_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_id INTEGER,
            user_name TEXT,
            phone TEXT,
            zone TEXT,
            appointment_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

# Функция для добавления мастера (если еще не добавлен)
def add_master(user_id):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO masters (user_id) VALUES (?)", (user_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # мастер уже есть
    finally:
        conn.close()

# Функции для работы с пользователями
def add_user(user_id, username, first_name, last_name):
    """Добавляет пользователя, возвращает True, если пользователь новый."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR IGNORE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name))
    conn.commit()
    new = cur.rowcount > 0
    conn.close()
    return new

def set_user_phone(user_id, phone):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET phone = ? WHERE user_id = ?", (phone, user_id))
    conn.commit()
    conn.close()

def get_user(user_id):
    """Возвращает словарь с данными пользователя или None."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return dict(row)  # преобразуем Row в словарь, чтобы работал .get()
    return None

def update_user(user_id, first_name=None, last_name=None, phone=None, age=None):
    conn = get_db()
    cur = conn.cursor()
    if first_name is not None:
        cur.execute("UPDATE users SET first_name = ? WHERE user_id = ?", (first_name, user_id))
    if last_name is not None:
        cur.execute("UPDATE users SET last_name = ? WHERE user_id = ?", (last_name, user_id))
    if phone is not None:
        cur.execute("UPDATE users SET phone = ? WHERE user_id = ?", (phone, user_id))
    if age is not None:
        cur.execute("UPDATE users SET age = ? WHERE user_id = ?", (age, user_id))
    conn.commit()
    conn.close()

def update_survey_data(user_id, data_json):
    """Сохраняет JSON с ответами анкеты в поле survey_data."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET survey_data = ? WHERE user_id = ?", (data_json, user_id))
    conn.commit()
    conn.close()

# Функции для работы с опросником (временные данные)
def save_survey_step(user_id, zone, step, answers_dict):
    import json
    conn = get_db()
    cur = conn.cursor()
    answers_json = json.dumps(answers_dict, ensure_ascii=False)
    cur.execute('''
        INSERT INTO survey_answers (user_id, zone, step, answers)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            zone=excluded.zone,
            step=excluded.step,
            answers=excluded.answers,
            updated_at=CURRENT_TIMESTAMP
    ''', (user_id, zone, step, answers_json))
    conn.commit()
    conn.close()

def get_survey_data(user_id):
    import json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT zone, step, answers FROM survey_answers WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            'zone': row['zone'],
            'step': row['step'],
            'answers': json.loads(row['answers'])
        }
    return None

def clear_survey(user_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM survey_answers WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

# Функции для работы со слотами
def add_slot(master_id, zone, slot_time):
    """Добавление свободного слота мастером. zone может быть None (универсальный слот)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO appointments (master_id, zone, slot_time, status)
        VALUES (?, ?, ?, 'free')
    ''', (master_id, zone, slot_time))
    conn.commit()
    conn.close()

def get_free_slots():
    """Получить все свободные слоты (без фильтра по зоне)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id, slot_time FROM appointments
        WHERE status = 'free' AND slot_time > datetime('now')
        ORDER BY slot_time
    ''')
    rows = cur.fetchall()
    conn.close()
    # Возвращаем как есть (Row) — в коде используются индексы, это нормально
    return rows

def book_slot(slot_id, user_id, zone):
    """Забронировать слот с указанием зоны, выбранной клиентом."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        UPDATE appointments
        SET status = 'booked', user_id = ?, zone = ?
        WHERE id = ? AND status = 'free'
    ''', (user_id, zone, slot_id))
    conn.commit()
    success = cur.rowcount > 0
    conn.close()
    return success

def get_appointment_by_id(slot_id):
    """Возвращает словарь с данными записи или None."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT * FROM appointments WHERE id = ?
    ''', (slot_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return dict(row)  # преобразуем в словарь для удобства
    return None

def confirm_appointment(slot_id):
    """Подтвердить запись мастером."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        UPDATE appointments SET status = 'confirmed' WHERE id = ?
    ''', (slot_id,))
    conn.commit()
    conn.close()

def cancel_appointment(slot_id):
    """Отменить запись (освободить слот)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        UPDATE appointments SET status = 'free', user_id = NULL WHERE id = ?
    ''', (slot_id,))
    conn.commit()
    conn.close()

# Функции для отзывов
def add_review(user_id, zone, text, rating, photo_path=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO reviews (user_id, zone, review_text, rating, photo)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, zone, text, rating, photo_path))
    conn.commit()
    conn.close()

def get_all_reviews():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT r.*, u.username, u.first_name, u.last_name
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        ORDER BY r.created_at DESC
    ''')
    rows = cur.fetchall()
    conn.close()
    # Возвращаем как есть (Row) — в коде используются имена колонок, это работает
    return rows

# Функции для напоминаний о коррекции
def add_correction_reminder(user_id, appointment_id, reminder_date):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO correction_reminders (user_id, appointment_id, reminder_date)
        VALUES (?, ?, ?)
    ''', (user_id, appointment_id, reminder_date))
    conn.commit()
    conn.close()

def get_unsent_correction_reminders():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id, user_id FROM correction_reminders
        WHERE sent = 0 AND reminder_date <= datetime('now')
    ''')
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_correction_reminder_sent(reminder_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE correction_reminders SET sent = 1 WHERE id = ?', (reminder_id,))
    conn.commit()
    conn.close()