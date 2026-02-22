import telebot
from telebot import types
from telebot_calendar import Calendar, CallbackData
import datetime
import json
import time
import threading
import os
import re
import shutil
import icalendar
import requests
import http.server
import socketserver
from datetime import datetime, timedelta
from config import BOT_TOKEN, MASTER_ID
import database as db

# Простой HTTP-сервер для health check на Railway/Render
def run_http_server():
    port = int(os.environ.get('PORT', 8000))
    
    class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
    
    with socketserver.TCPServer(("0.0.0.0", port), HealthCheckHandler) as httpd:
        print(f"Health check server running on port {port}")
        httpd.serve_forever()

threading.Thread(target=run_http_server, daemon=True).start()

# Принудительно удаляем вебхук перед запуском
try:
    requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=5)
    time.sleep(1)
except:
    pass

# Константы
OFFICE_ADDRESS = "г. Тверь, ул. Фадеева, д. 15"
BACKUP_CHANNEL = -1003729357878

# Цены на услуги
PRICES = {
    "Губы": {"primary": 4000, "correction": 2000},
    "Брови": {"primary": 4000, "correction": 2000},
    "Межресничка": {"primary": 1500, "correction": 1000}
}

# Инициализация базы данных
db.init_db()
db.add_master(MASTER_ID)

bot = telebot.TeleBot(BOT_TOKEN)
bot.delete_webhook(drop_pending_updates=True)

user_states = {}
user_navigation = {}

calendar = Calendar()
calendar_callback = CallbackData("calendar", "action", "year", "month", "day")

# ---- Список вопросов опросника ----
survey_questions = [
    ("Как вас зовут?", "text"),
    ("Сколько вам лет?", "text"),
    ("Делали ли ранее перманент выбранной зоны?", "yesno"),
    ("Как давно? (если делали ранее)", "text"),
    ("Существуют абсолютные и относительные противопоказания.", "info"),
    ("Присутствует ли онкология в стадии обострения? Да/нет", "yesno"),
    ("Присутствует ли нарушение свертываемости крови (гемофилия)? Да/нет", "yesno"),
    ("Присутствует ли патологии соматического характера (анорексия, депрессивное, конверсионное, сексуальное расстройство, астенический синдром) и психические заболевания? Да/нет (если да, то какое?)", "yesno"),
    ("Присутствует ли обострение кожных заболеваний (дерматит, герпес, акне) в зоне выбранного перманентного макияжа? Да/нет", "yesno"),
    ("Присутствует ли ВИЧ или СПИД? Да/нет", "yesno"),
    ("Присутствует ли гипертония? Да/нет", "yesno"),
    ("Принимаете ли кроворазжижающие препараты? Да/нет", "yesno"),
    ("Беременность и период лактации на момент проведения процедуры? Да/нет", "yesno"),
    ("Кейлоидные рубцы? Да/нет", "yesno"),
    ("Разрастания соединительной ткани, возникающие в месте травмы? Да/нет", "yesno"),
    ("Сахарный диабет инсулинозависимый (первого типа)? Да/нет", "yesno"),
    ("Аутоиммунные заболевания? Да/нет", "yesno"),
    ("Эпилепсия? да/нет", "yesno"),
    ("Хроническое неинфекционное заболевание головного мозга, поражающее людей в любом возрасте? Да/нет", "yesno"),
    ("Неудовлетворительное самочувствие: стресс, бессонная ночь, ринит и орви? (впишите ответ или нет)", "text"),
    ("Менструация на момент выполнения процедуры? Да/нет", "yesno"),
    ("Появление воспалений и гнойничков в зоне планируемого воздействия? Да/нет", "yesno"),
    ("Прием антибиотиков? Да/нет", "yesno"),
    ("Наличие в области воздействия бородавок или родинок? Да/нет", "yesno"),
    ("При наличии герпесной инфекции или аллергии перманентный макияж делать после выполнения всех рекомендаций мастера и лечащего врача.", "info"),
    ("Есть ли аллергические реакции на лекарственные препараты? (например, лидокаин/новокаин). Если не знаете, вспомните, были ли у стоматолога. Да/нет + пояснение", "text"),
    ("Какие лекарственные препараты вы принимаете на постоянной основе? (например, системные ретиноиды, антибиотики и тд.)", "text"),
]

def send_to_channel(text):
    try:
        bot.send_message(BACKUP_CHANNEL, text, parse_mode='Markdown')
    except Exception as e:
        print(f"Ошибка отправки в канал: {e}")

def get_main_menu(user_id):
    if user_id == MASTER_ID:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        markup.add(
            types.KeyboardButton("➕ Добавить слот"),
            types.KeyboardButton("📋 Посмотреть отзывы"),
            types.KeyboardButton("✅ Подтвердить записи"),
            types.KeyboardButton("📅 Показать все записи"),
            types.KeyboardButton("⚙️ Сгенерировать слоты"),
            types.KeyboardButton("➕ Ручное добавление клиента"),
            types.KeyboardButton("⛔️ Черный список"),
            types.KeyboardButton("🏠 Главное меню")
        )
        return markup
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(
            types.KeyboardButton("📝 Записаться на процедуру"),
            types.KeyboardButton("⭐️ Оставить отзыв"),
            types.KeyboardButton("📖 Посмотреть отзывы"),
            types.KeyboardButton("👤 Мой профиль"),
            types.KeyboardButton("📩 Задать вопрос мастеру"),
            types.KeyboardButton("🏠 Главное меню")
        )
        return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    
    is_new = db.add_user(user_id, username, first_name, last_name)
    if is_new:
        send_to_channel(f"🆕 *Новый пользователь*\n"
                        f"Имя: {first_name} {last_name}\n"
                        f"Username: @{username}\n"
                        f"ID: `{user_id}`")
    
    welcome_text = (
        "✨ *Добро пожаловать в студию перманентного макияжа!* ✨\n\n"
        "Я помогу вам записаться на процедуру, напомню о визите и сохраню ваши данные.\n"
        "Используйте кнопки меню для навигации."
    )
    bot.send_message(user_id, welcome_text, parse_mode='Markdown', reply_markup=get_main_menu(user_id))

@bot.message_handler(func=lambda message: message.text == "🏠 Главное меню")
def back_to_main(message):
    send_welcome(message)

# Обработчик кнопки "◀️ Назад"
@bot.message_handler(func=lambda message: message.text == "◀️ Назад")
def handle_back_button(message):
    user_id = message.from_user.id
    prev = user_navigation.get(user_id, 'main')
    if prev == 'profile':
        show_profile(message)
    elif prev == 'booking_zone':
        handle_booking_start(message)
    elif prev == 'review_zone':
        ask_review(message)
    elif prev == 'calendar':
        show_calendar(user_id)
    else:
        send_welcome(message)

# -------------------- ЛИЧНЫЙ КАБИНЕТ КЛИЕНТА --------------------
@bot.message_handler(func=lambda message: message.text == "👤 Мой профиль")
def show_profile(message):
    user_id = message.from_user.id
    user_navigation[user_id] = 'main'
    user = db.get_user(user_id)
    if not user:
        bot.send_message(user_id, "❌ Ошибка загрузки профиля.")
        return
    text = f"👤 *Ваш профиль*\n\n"
    text += f"▪️ Имя: {user['first_name'] or 'не указано'}\n"
    text += f"▪️ Фамилия: {user['last_name'] or 'не указано'}\n"
    text += f"▪️ Возраст: {user['age'] or 'не указан'}\n"
    text += f"▪️ Телефон: {user['phone'] or 'не указан'}\n"
    text += f"▪️ Анкета: {'✅ заполнена' if user.get('survey_data') else '❌ не заполнена'}"
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📋 Мои записи", callback_data="my_appointments"),
        types.InlineKeyboardButton("📁 Мои процедуры", callback_data="my_history"),
        types.InlineKeyboardButton("✏️ Редактировать", callback_data="edit_profile"),
        types.InlineKeyboardButton("📝 Заполнить анкету", callback_data="fill_survey"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
    )
    bot.send_message(user_id, text, parse_mode='Markdown', reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "my_appointments")
def my_appointments_callback(call):
    user_id = call.from_user.id
    user_navigation[user_id] = 'profile'
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id, zone, slot_time, status, is_primary FROM appointments
        WHERE user_id = ? AND status IN ('booked', 'confirmed')
        ORDER BY slot_time
    ''', (user_id,))
    upcoming = cur.fetchall()
    cur.execute('''
        SELECT id, zone, slot_time, is_primary FROM appointments
        WHERE user_id = ? AND status = 'confirmed' AND slot_time < datetime('now')
        ORDER BY slot_time DESC
    ''', (user_id,))
    past = cur.fetchall()
    conn.close()

    text = "📋 *Ваши записи*\n\n"
    if upcoming:
        text += "🔜 *Предстоящие:*\n"
        for row in upcoming:
            status = "⏳ ожидает" if row['status'] == 'booked' else "🔒 подтверждена"
            dt = row['slot_time'].strftime('%d.%m.%Y %H:%M')
            proc_type = "первичная" if row['is_primary'] else "коррекция"
            text += f"▪ {dt} – {row['zone']} ({proc_type}) – {status}\n"
    else:
        text += "🔜 Нет предстоящих записей.\n"
    
    if past:
        text += "\n🔙 *Прошедшие:*\n"
        for row in past:
            dt = row['slot_time'].strftime('%d.%m.%Y %H:%M')
            proc_type = "первичная" if row['is_primary'] else "коррекция"
            text += f"▪ {dt} – {row['zone']} ({proc_type})\n"
    else:
        text += "\n🔙 Нет прошедших записей."

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_profile"))
    bot.edit_message_text(text, chat_id=user_id, message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "my_history")
def my_history_callback(call):
    user_id = call.from_user.id
    user_navigation[user_id] = 'profile'
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT a.id, a.zone, a.slot_time, a.is_primary
        FROM appointments a
        WHERE a.user_id = ? AND a.status = 'confirmed' AND a.slot_time < datetime('now')
        ORDER BY a.slot_time DESC
    ''', (user_id,))
    appointments = cur.fetchall()
    conn.close()
    if not appointments:
        bot.send_message(user_id, "У вас пока нет завершённых процедур.")
        return
    for app in appointments:
        text = f"📅 {app['slot_time'].strftime('%d.%m.%Y %H:%M')}\n"
        text += f"📍 Зона: {app['zone']}\n"
        text += f"🔹 Тип: {'Первичная' if app['is_primary'] else 'Кррекция'}\n"
        conn = db.get_db()
        cur = conn.cursor()
        cur.execute('SELECT photo_path, photo_type FROM history_photos WHERE appointment_id = ?', (app['id'],))
        photos = cur.fetchall()
        conn.close()
        if photos:
            text += f"📸 Фото: {len(photos)} шт.\n"
            bot.send_message(user_id, text)
            for photo in photos:
                try:
                    with open(photo['photo_path'], 'rb') as f:
                        bot.send_photo(user_id, f, caption=f"Тип: {photo['photo_type']}")
                except:
                    bot.send_message(user_id, f"(Фото {photo['photo_type']} недоступно)")
        else:
            bot.send_message(user_id, text + "\n(нет фотографий)")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("◀️ Назад в профиль", callback_data="back_to_profile"))
    bot.send_message(user_id, "Выберите действие:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "fill_survey")
def fill_survey_callback(call):
    user_id = call.from_user.id
    user_navigation[user_id] = 'profile'
    db.update_survey_data(user_id, None)
    bot.edit_message_text("📝 Заполнение анкеты...", chat_id=user_id, message_id=call.message.message_id)
    start_survey(user_id)

@bot.callback_query_handler(func=lambda call: call.data == "edit_profile")
def edit_profile_callback(call):
    user_id = call.from_user.id
    user_navigation[user_id] = 'profile'
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("Имя", callback_data="edit_name"),
        types.InlineKeyboardButton("Фамилия", callback_data="edit_lastname"),
        types.InlineKeyboardButton("Возраст", callback_data="edit_age"),
        types.InlineKeyboardButton("Телефон", callback_data="edit_phone"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_profile")
    )
    bot.edit_message_text("Что вы хотите изменить?", chat_id=user_id, message_id=call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('edit_') and call.data not in ['edit_profile', 'edit_review'])
def edit_field_callback(call):
    user_id = call.from_user.id
    field = call.data.split('_')[1]
    user_states[user_id] = {'editing': field}
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("◀️ Назад")
    bot.edit_message_text(f"✏️ Введите новое значение для {field}:", chat_id=user_id, message_id=call.message.message_id)
    msg = bot.send_message(user_id, "Введите значение или нажмите '◀️ Назад' для отмены:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_profile_edit)

def process_profile_edit(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        show_profile(message)
        return
    field = user_states.get(user_id, {}).get('editing')
    if not field:
        return
    value = message.text.strip()
    if field == 'age':
        if not value.isdigit():
            bot.send_message(user_id, "❌ Возраст должен быть числом. Попробуйте снова.")
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add("◀️ Назад")
            msg = bot.send_message(user_id, "Введите возраст числом:", reply_markup=markup)
            bot.register_next_step_handler(msg, process_profile_edit)
            return
        value = int(value)
    if field == 'name':
        db.update_user(user_id, first_name=value)
    elif field == 'lastname':
        db.update_user(user_id, last_name=value)
    elif field == 'age':
        db.update_user(user_id, age=value)
    elif field == 'phone':
        db.update_user(user_id, phone=value)
        user = db.get_user(user_id)
        send_to_channel(f"📞 *Клиент изменил телефон*\n"
                        f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                        f"Новый телефон: `{value}`")
    bot.send_message(user_id, "✅ Профиль обновлён!", reply_markup=get_main_menu(user_id))
    del user_states[user_id]

@bot.callback_query_handler(func=lambda call: call.data == "back_to_profile")
def back_to_profile_callback(call):
    user_id = call.from_user.id
    user = db.get_user(user_id)
    if not user:
        return
    text = f"👤 *Ваш профиль*\n\n"
    text += f"▪️ Имя: {user['first_name'] or 'не указано'}\n"
    text += f"▪️ Фамилия: {user['last_name'] or 'не указано'}\n"
    text += f"▪️ Возраст: {user['age'] or 'не указан'}\n"
    text += f"▪️ Телефон: {user['phone'] or 'не указан'}\n"
    text += f"▪️ Анкета: {'✅ заполнена' if user.get('survey_data') else '❌ не заполнена'}"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📋 Мои записи", callback_data="my_appointments"),
        types.InlineKeyboardButton("📁 Мои процедуры", callback_data="my_history"),
        types.InlineKeyboardButton("✏️ Редактировать", callback_data="edit_profile"),
        types.InlineKeyboardButton("📝 Заполнить анкету", callback_data="fill_survey"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
    )
    bot.edit_message_text(text, chat_id=user_id, message_id=call.message.message_id, parse_mode='Markdown', reply_markup=markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_main")
def back_to_main_callback(call):
    user_id = call.from_user.id
    bot.delete_message(user_id, call.message.message_id)
    welcome_text = (
        "✨ *Добро пожаловать в студию перманентного макияжа!* ✨\n\n"
        "Я помогу вам записаться на процедуру, напомню о визите и сохраню ваши данные.\n"
        "Используйте кнопки меню для навигации."
    )
    bot.send_message(user_id, welcome_text, parse_mode='Markdown', reply_markup=get_main_menu(user_id))
    bot.answer_callback_query(call.id)

# -------------------- АНКЕТА --------------------
def is_survey_completed(user_id):
    user = db.get_user(user_id)
    return user and user.get('survey_data') is not None

def start_survey(user_id):
    db.save_survey_step(user_id, None, 0, {})
    ask_survey_question(user_id, 0)

def ask_survey_question(user_id, q_index):
    survey = db.get_survey_data(user_id)
    if not survey:
        bot.send_message(user_id, "❌ Ошибка. Начните заново.")
        return
    if q_index >= len(survey_questions):
        answers = survey['answers']
        db.update_survey_data(user_id, json.dumps(answers, ensure_ascii=False))
        if 'q1' in answers and answers['q1'].isdigit():
            db.update_user(user_id, age=int(answers['q1']))
        bot.send_message(user_id, "✅ Спасибо! Анкета сохранена.", reply_markup=get_main_menu(user_id))
        return
    question, qtype = survey_questions[q_index]
    if qtype == "info":
        bot.send_message(user_id, f"ℹ️ {question}")
        answers = survey['answers']
        db.save_survey_step(user_id, None, q_index + 1, answers)
        ask_survey_question(user_id, q_index + 1)
        return
    markup = None
    if qtype == "yesno":
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("Да", "Нет", "◀️ Назад")
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("◀️ Назад")
    msg = bot.send_message(user_id, f"❓ {question}", reply_markup=markup)
    bot.register_next_step_handler(msg, lambda m: process_survey_answer(m, q_index))

def process_survey_answer(message, q_index):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        show_profile(message)
        return
    survey = db.get_survey_data(user_id)
    if not survey:
        bot.send_message(user_id, "❌ Ошибка. Начните заново.")
        return
    answers = survey['answers']
    answers[f'q{q_index}'] = message.text
    if q_index == 2 and message.text.lower() == "нет":
        next_index = q_index + 2
    else:
        next_index = q_index + 1
    db.save_survey_step(user_id, None, next_index, answers)
    ask_survey_question(user_id, next_index)

# -------------------- ЗАПИСЬ НА ПРОЦЕДУРУ --------------------
def has_primary_done(user_id, zone):
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id FROM appointments
        WHERE user_id = ? AND zone = ? AND is_primary = 1 AND status = 'confirmed' AND slot_time < datetime('now')
    ''', (user_id, zone))
    row = cur.fetchone()
    conn.close()
    return row is not None

@bot.message_handler(func=lambda message: message.text == "📝 Записаться на процедуру")
def handle_booking_start(message):
    user_id = message.from_user.id
    # Проверка чёрного списка
    if db.is_blacklisted(user_id):
        bot.send_message(user_id, "⛔️ Вы не можете записаться на процедуру, так как находитесь в черном списке. Свяжитесь с мастером для уточнения.")
        return
    user_navigation[user_id] = 'main'
    if not is_survey_completed(user_id):
        bot.send_message(user_id, "📋 Сначала нужно заполнить анкету. Перейдите в профиль и нажмите «Заполнить анкету».")
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("👄 Губы", "✏️ Брови", "👁 Межресничка")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Выберите зону для перманентного макияжа:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_zone_choice)

def process_zone_choice(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        send_welcome(message)
        return
    zone_map = {
        "👄 Губы": "Губы",
        "✏️ Брови": "Брови",
        "👁 Межресничка": "Межресничка"
    }
    zone = zone_map.get(message.text)
    if not zone:
        bot.send_message(user_id, "❌ Пожалуйста, выберите зону из кнопок.")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("👄 Губы", "✏️ Брови", "👁 Межресничка")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Выберите зону:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_zone_choice)
        return

    if has_primary_done(user_id, zone):
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"Первичная ({PRICES[zone]['primary']}₽)", callback_data=f"type_primary_{zone}"),
            types.InlineKeyboardButton(f"Коррекция ({PRICES[zone]['correction']}₽)", callback_data=f"type_correction_{zone}"),
            types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_booking_zone")
        )
        bot.send_message(user_id, f"Выберите тип процедуры для зоны {zone}:", reply_markup=markup)
    else:
        user_states[user_id] = {'booking_zone': zone, 'is_primary': 1, 'price': PRICES[zone]['primary']}
        show_calendar(user_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('type_'))
def handle_procedure_type(call):
    user_id = call.from_user.id
    data = call.data.split('_')
    proc_type = data[1]
    zone = data[2]
    is_primary = 1 if proc_type == 'primary' else 0
    price = PRICES[zone]['primary'] if is_primary else PRICES[zone]['correction']
    user_states[user_id] = {'booking_zone': zone, 'is_primary': is_primary, 'price': price}
    bot.edit_message_text("🕐 Выберите дату:", chat_id=user_id, message_id=call.message.message_id)
    show_calendar(user_id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_booking_zone")
def back_to_booking_zone_callback(call):
    user_id = call.from_user.id
    bot.delete_message(user_id, call.message.message_id)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("👄 Губы", "✏️ Брови", "👁 Межресничка")
    markup.add("◀️ Назад")
    msg = bot.send_message(user_id, "Выберите зону для перманентного макияжа:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_zone_choice)

def show_calendar(user_id):
    now = datetime.now()
    markup = calendar.create_calendar(
        name=calendar_callback.prefix,
        year=now.year,
        month=now.month
    )
    bot.send_message(user_id, "Выберите дату:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith(calendar_callback.prefix))
def handle_calendar(call):
    user_id = call.from_user.id
    try:
        parts = call.data.split(calendar_callback.sep)
        if len(parts) >= 4:
            action = parts[1]
            year = int(parts[2])
            month = int(parts[3])
            day = int(parts[4]) if len(parts) > 4 else None
        else:
            now = datetime.now()
            bot.edit_message_text(
                "Выберите дату:",
                chat_id=user_id,
                message_id=call.message.message_id,
                reply_markup=calendar.create_calendar(
                    name=calendar_callback.prefix,
                    year=now.year,
                    month=now.month
                )
            )
            return

        if action == "DAY":
            selected_date = datetime(year, month, day)
            show_slots_for_date(user_id, selected_date)
        else:
            bot.edit_message_text(
                "Выберите дату:",
                chat_id=user_id,
                message_id=call.message.message_id,
                reply_markup=calendar.create_calendar(
                    name=calendar_callback.prefix,
                    year=year,
                    month=month
                )
            )
    except Exception:
        now = datetime.now()
        bot.edit_message_text(
            "Выберите дату:",
            chat_id=user_id,
            message_id=call.message.message_id,
            reply_markup=calendar.create_calendar(
                name=calendar_callback.prefix,
                year=now.year,
                month=now.month
            )
        )

def show_slots_for_date(user_id, date):
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id, slot_time FROM appointments
        WHERE status = 'free' AND date(slot_time) = date(?)
        ORDER BY slot_time
    ''', (date,))
    slots = cur.fetchall()
    conn.close()
    if not slots:
        bot.send_message(user_id, "На выбранную дату нет свободных слотов.")
        return
    markup = types.InlineKeyboardMarkup()
    for slot in slots:
        slot_time = slot['slot_time'].strftime('%H:%M')
        markup.add(types.InlineKeyboardButton(slot_time, callback_data=f"book_{slot['id']}"))
    markup.add(types.InlineKeyboardButton("◀️ Назад к календарю", callback_data="back_to_calendar"))
    bot.send_message(user_id, f"Доступное время на {date.strftime('%d.%m.%Y')}:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_calendar")
def back_to_calendar_callback(call):
    user_id = call.from_user.id
    now = datetime.now()
    markup = calendar.create_calendar(
        name=calendar_callback.prefix,
        year=now.year,
        month=now.month
    )
    bot.edit_message_text("Выберите дату:", chat_id=user_id, message_id=call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('book_'))
def handle_booking(call):
    user_id = call.from_user.id
    slot_id = int(call.data.split('_')[1])
    if user_id not in user_states or 'booking_zone' not in user_states[user_id]:
        bot.answer_callback_query(call.id, "❌ Сначала выберите зону.")
        return
    zone = user_states[user_id]['booking_zone']
    is_primary = user_states[user_id].get('is_primary', 1)
    price = user_states[user_id].get('price', PRICES[zone]['primary'])
    
    user = db.get_user(user_id)
    if user and user.get('phone'):
        success = db.book_slot(slot_id, user_id, zone)
        if success:
            conn = db.get_db()
            cur = conn.cursor()
            cur.execute("UPDATE appointments SET price = ?, is_primary = ? WHERE id = ?", (price, is_primary, slot_id))
            conn.commit()
            conn.close()
            bot.answer_callback_query(call.id, "✅ Вы успешно забронировали время!")
            slot = db.get_appointment_by_id(slot_id)
            bot.edit_message_text(
                f"✅ *Запись создана!*\n\nЗона: {zone}\nВремя: {slot['slot_time'].strftime('%d.%m.%Y %H:%M')}\nСтоимость: {price}₽\nАдрес: {OFFICE_ADDRESS}\nОжидайте подтверждения мастера.",
                chat_id=user_id,
                message_id=call.message.message_id,
                parse_mode='Markdown'
            )
            send_new_booking_to_master(user_id, zone, slot_id, price, is_primary)
            slot_time_formatted = slot['slot_time'].strftime('%d.%m.%Y %H:%M')
            send_to_channel(f"📅 *Новая запись*\n"
                            f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                            f"Зона: {zone}\n"
                            f"Дата/время: {slot_time_formatted}\n"
                            f"Тип: {'Первичная' if is_primary else 'Коррекция'}\n"
                            f"Стоимость: {price}₽")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Отменить и выбрать новое время", callback_data=f"reschedule_{slot_id}"))
            markup.add(types.InlineKeyboardButton("◀️ В главное меню", callback_data="back_to_main"))
            bot.send_message(user_id, "Вы можете изменить время позже, нажав кнопку ниже.", reply_markup=markup)
        else:
            bot.answer_callback_query(call.id, "😔 Это время уже занято. Выберите другое.", show_alert=True)
            show_calendar(user_id)
    else:
        user_states[user_id]['temp_slot_id'] = slot_id
        user_states[user_id]['temp_price'] = price
        user_states[user_id]['temp_is_primary'] = is_primary
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("❌ Отмена", "◀️ Назад")
        msg = bot.send_message(user_id, "📞 Введите ваш номер телефона для связи (например, +7XXXXXXXXXX):", reply_markup=markup)
        bot.register_next_step_handler(msg, process_phone_for_booking)
    bot.answer_callback_query(call.id)

def process_phone_for_booking(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        show_calendar(user_id)
        return
    if message.text == "❌ Отмена":
        if user_id in user_states:
            del user_states[user_id]
        bot.send_message(user_id, "❌ Действие отменено.", reply_markup=get_main_menu(user_id))
        return
    phone = message.text.strip()
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        bot.send_message(user_id, "❌ Номер телефона некорректен. Попробуйте ещё раз (или нажмите Отмена).")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("❌ Отмена", "◀️ Назад")
        msg = bot.send_message(user_id, "📞 Введите ваш номер телефона для связи (например, +7XXXXXXXXXX):", reply_markup=markup)
        bot.register_next_step_handler(msg, process_phone_for_booking)
        return
    db.update_user(user_id, phone=phone)
    user = db.get_user(user_id)
    send_to_channel(f"📞 *Клиент добавил телефон*\n"
                    f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                    f"Телефон: `{phone}`")
    slot_id = user_states[user_id].get('temp_slot_id')
    zone = user_states[user_id].get('booking_zone')
    price = user_states[user_id].get('temp_price')
    is_primary = user_states[user_id].get('temp_is_primary', 1)
    if not slot_id or not zone:
        bot.send_message(user_id, "❌ Ошибка данных. Начните заново.", reply_markup=get_main_menu(user_id))
        return
    success = db.book_slot(slot_id, user_id, zone)
    if success:
        conn = db.get_db()
        cur = conn.cursor()
        cur.execute("UPDATE appointments SET price = ?, is_primary = ? WHERE id = ?", (price, is_primary, slot_id))
        conn.commit()
        conn.close()
        bot.send_message(
            user_id,
            f"✅ *Запись создана!*\n\nЗона: {zone}\nВремя: {db.get_appointment_by_id(slot_id)['slot_time'].strftime('%d.%m.%Y %H:%M')}\nСтоимость: {price}₽\nАдрес: {OFFICE_ADDRESS}\nОжидайте подтверждения мастера.",
            parse_mode='Markdown',
            reply_markup=get_main_menu(user_id)
        )
        send_new_booking_to_master(user_id, zone, slot_id, price, is_primary)
        slot = db.get_appointment_by_id(slot_id)
        slot_time_formatted = slot['slot_time'].strftime('%d.%m.%Y %H:%M')
        send_to_channel(f"📅 *Новая запись*\n"
                        f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                        f"Зона: {zone}\n"
                        f"Дата/время: {slot_time_formatted}\n"
                        f"Тип: {'Первичная' if is_primary else 'Коррекция'}\n"
                        f"Стоимость: {price}₽")
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Отменить и выбрать новое время", callback_data=f"reschedule_{slot_id}"))
        markup.add(types.InlineKeyboardButton("◀️ В главное меню", callback_data="back_to_main"))
        bot.send_message(user_id, "Вы можете изменить время позже, нажав кнопку ниже.", reply_markup=markup)
    else:
        bot.send_message(user_id, "😔 Это время уже занято. Попробуйте другое.", reply_markup=get_main_menu(user_id))
        show_calendar(user_id)
    if user_id in user_states:
        del user_states[user_id]

def send_new_booking_to_master(user_id, zone, slot_id, price, is_primary):
    user = db.get_user(user_id)
    survey_data = user.get('survey_data')
    if survey_data:
        try:
            answers = json.loads(survey_data)
        except:
            answers = {}
    else:
        answers = {}
    slot = db.get_appointment_by_id(slot_id)
    if not slot:
        return
    slot_time_str = slot['slot_time'].strftime('%d.%m.%Y %H:%M')
    
    text = f"📋 *Новая запись*\n\n"
    text += f"👤 Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
    text += f"📞 Телефон: {user['phone'] or 'не указан'}\n"
    text += f"🎂 Возраст: {user['age'] or 'не указан'}\n"
    text += f"📍 Зона: {zone}\n"
    text += f"🕐 Время: {slot_time_str}\n"
    text += f"💰 Стоимость: {price}₽\n"
    text += f"🔹 Тип: {'Первичная' if is_primary else 'Коррекция'}\n\n"
    text += "*📋 Данные анкеты:*\n"
    for key in sorted(answers.keys()):
        try:
            q_num = int(key[1:])
            if q_num < len(survey_questions):
                q_text = survey_questions[q_num][0]
                text += f"▪ {q_text}: {answers[key]}\n"
            else:
                text += f"▪ {key}: {answers[key]}\n"
        except:
            text += f"▪ {key}: {answers[key]}\n"
    risk_warning = analyze_risks(answers)
    if risk_warning:
        text += f"\n⚠️ *Возможные противопоказания:*\n{risk_warning}"
    bot.send_message(MASTER_ID, text, parse_mode='Markdown')

def analyze_risks(answers):
    warnings = []
    absolute_risks = {
        5: "онкология в стадии обострения",
        6: "нарушение свертываемости крови",
        7: "психические заболевания",
        8: "обострение кожных заболеваний",
        9: "ВИЧ/СПИД",
        10: "гипертония",
        11: "прием кроворазжижающих",
        12: "беременность/лактация",
        13: "келоидные рубцы",
        14: "разрастания соединительной ткани",
        15: "сахарный диабет 1 типа",
        16: "аутоиммунные заболевания",
        17: "эпилепсия",
        18: "хроническое заболевание головного мозга"
    }
    for idx, desc in absolute_risks.items():
        if answers.get(f'q{idx}', '').lower() == 'да':
            warnings.append(f"• {desc}")
    relative_risks = {
        19: "неудовлетворительное самочувствие",
        20: "менструация",
        21: "воспаления/гнойнички",
        22: "прием антибиотиков",
        23: "бородавки/родинки в зоне",
    }
    for idx, desc in relative_risks.items():
        if answers.get(f'q{idx}', '').lower() == 'да':
            warnings.append(f"• {desc} (относительное)")
    return "\n".join(warnings) if warnings else None

@bot.callback_query_handler(func=lambda call: call.data.startswith('reschedule_'))
def reschedule_callback(call):
    user_id = call.from_user.id
    old_slot_id = int(call.data.split('_')[1])
    old_appointment = db.get_appointment_by_id(old_slot_id)
    if not old_appointment or old_appointment['user_id'] != user_id:
        bot.answer_callback_query(call.id, "❌ Запись не найдена или недоступна.")
        return
    user_states[user_id] = {
        'booking_zone': old_appointment['zone'],
        'is_primary': old_appointment['is_primary'],
        'price': old_appointment['price']
    }
    db.cancel_appointment(old_slot_id)
    bot.send_message(user_id, "🔄 Старая запись отменена. Выберите новое время.")
    show_calendar(user_id)
    bot.answer_callback_query(call.id)

# -------------------- ОТЗЫВЫ (КЛИЕНТ) --------------------
def can_leave_review(user_id):
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT id FROM appointments
        WHERE user_id = ? AND status = 'confirmed' AND slot_time < datetime('now')
    ''', (user_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None

@bot.message_handler(func=lambda message: message.text == "⭐️ Оставить отзыв")
def ask_review(message):
    user_id = message.from_user.id
    user_navigation[user_id] = 'main'
    if not can_leave_review(user_id):
        bot.send_message(user_id, "😔 Вы можете оставить отзыв только после выполненной процедуры.")
        return
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("👄 Губы", "✏️ Брови", "👁 Межресничка")
    markup.add("◀️ Назад")
    msg = bot.send_message(user_id, "Для какой зоны вы хотите оставить отзыв?", reply_markup=markup)
    bot.register_next_step_handler(msg, process_review_zone)

def process_review_zone(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        send_welcome(message)
        return
    zone_map = {
        "👄 Губы": "Губы",
        "✏️ Брови": "Брови",
        "👁 Межресничка": "Межресничка"
    }
    zone = zone_map.get(message.text)
    if not zone:
        bot.send_message(user_id, "❌ Пожалуйста, выберите зону из кнопок.")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("👄 Губы", "✏️ Брови", "👁 Межресничка")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Выберите зону:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_review_zone)
        return
    user_states[user_id] = {'review_zone': zone}
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("◀️ Назад")
    msg = bot.send_message(user_id, "✍️ Напишите ваш отзыв (можно сразу прикрепить фото):", reply_markup=markup)
    bot.register_next_step_handler(msg, process_review_text)

def process_review_text(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        ask_review(message)
        return
    if user_id not in user_states or 'review_zone' not in user_states[user_id]:
        bot.send_message(user_id, "❌ Ошибка. Начните заново.")
        return
    review_text = message.text
    zone = user_states[user_id]['review_zone']
    user_states[user_id]['review_text'] = review_text
    
    if message.photo:
        file_id = message.photo[-1].file_id
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        os.makedirs("reviews_photos", exist_ok=True)
        filename = f"reviews_photos/{user_id}_{int(time.time())}.jpg"
        with open(filename, 'wb') as f:
            f.write(downloaded_file)
        photo_path = filename
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("1", "2", "3", "4", "5")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Оцените процедуру от 1 до 5:", reply_markup=markup)
        bot.register_next_step_handler(msg, lambda m: process_review_rating(m, zone, review_text, photo_path))
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("📸 Добавить фото", "⏩ Пропустить")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Хотите добавить фото результата?", reply_markup=markup)
        bot.register_next_step_handler(msg, process_review_photo_choice)

def process_review_photo_choice(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        ask_review(message)
        return
    if user_id not in user_states or 'review_text' not in user_states[user_id] or 'review_zone' not in user_states[user_id]:
        bot.send_message(user_id, "❌ Ошибка. Начните заново.")
        return
    if message.text == "📸 Добавить фото":
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("❌ Отмена", "◀️ Назад")
        msg = bot.send_message(user_id, "Отправьте фото:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_review_photo_addition)
    else:
        zone = user_states[user_id]['review_zone']
        review_text = user_states[user_id]['review_text']
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("1", "2", "3", "4", "5")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Оцените процедуру от 1 до 5:", reply_markup=markup)
        bot.register_next_step_handler(msg, lambda m: process_review_rating(m, zone, review_text, None))

def process_review_photo_addition(message):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        ask_review(message)
        return
    if message.text == "❌ Отмена":
        zone = user_states[user_id]['review_zone']
        review_text = user_states[user_id]['review_text']
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("1", "2", "3", "4", "5")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Оцените процедуру от 1 до 5:", reply_markup=markup)
        bot.register_next_step_handler(msg, lambda m: process_review_rating(m, zone, review_text, None))
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        os.makedirs("reviews_photos", exist_ok=True)
        filename = f"reviews_photos/{user_id}_{int(time.time())}.jpg"
        with open(filename, 'wb') as f:
            f.write(downloaded_file)
        photo_path = filename
        zone = user_states[user_id]['review_zone']
        review_text = user_states[user_id]['review_text']
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("1", "2", "3", "4", "5")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Оцените процедуру от 1 до 5:", reply_markup=markup)
        bot.register_next_step_handler(msg, lambda m: process_review_rating(m, zone, review_text, photo_path))
    else:
        bot.send_message(user_id, "❌ Пожалуйста, отправьте фото или нажмите 'Отмена'.")
        bot.register_next_step_handler(message, process_review_photo_addition)

def process_review_rating(message, zone, review_text, photo_path):
    user_id = message.from_user.id
    if message.text == "◀️ Назад":
        ask_review(message)
        return
    try:
        rating = int(message.text)
        if rating < 1 or rating > 5:
            raise ValueError
    except:
        bot.send_message(user_id, "❌ Оценка должна быть числом от 1 до 5. Попробуйте еще раз.")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add("1", "2", "3", "4", "5")
        markup.add("◀️ Назад")
        msg = bot.send_message(user_id, "Оцените процедуру от 1 до 5:", reply_markup=markup)
        bot.register_next_step_handler(msg, lambda m: process_review_rating(m, zone, review_text, photo_path))
        return
    db.add_review(user_id, zone, review_text, rating, photo_path)
    user = db.get_user(user_id)
    send_to_channel(f"⭐️ *Новый отзыв*\n"
                    f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                    f"Зона: {zone}\n"
                    f"Оценка: {rating}/5\n"
                    f"Текст: {review_text}")
    bot.send_message(user_id, "✅ Спасибо за ваш отзыв!", reply_markup=get_main_menu(user_id))

@bot.message_handler(func=lambda message: message.text == "📖 Посмотреть отзывы")
def show_reviews(message):
    user_id = message.from_user.id
    reviews = db.get_all_reviews()
    if not reviews:
        bot.send_message(user_id, "😔 Пока нет ни одного отзыва.")
        return
    for rev in reviews[:10]:
        name = rev['first_name'] or rev['username'] or 'Аноним'
        date = rev['created_at'].strftime('%d.%m.%Y') if isinstance(rev['created_at'], datetime) else rev['created_at']
        text = f"⭐️ *{name}* ({date}) – {rev['zone']}\n"
        text += f"Оценка: {'⭐️' * rev['rating']}\n"
        text += f"“{rev['review_text']}”"
        if rev['photo']:
            try:
                with open(rev['photo'], 'rb') as photo:
                    bot.send_photo(user_id, photo, caption=text, parse_mode='Markdown')
            except:
                bot.send_message(user_id, text + "\n(Фото недоступно)", parse_mode='Markdown')
        else:
            bot.send_message(user_id, text, parse_mode='Markdown')

# -------------------- МАСТЕР: УПРАВЛЕНИЕ ОТЗЫВАМИ --------------------
def show_reviews_master(user_id):
    reviews = db.get_all_reviews()
    if not reviews:
        bot.send_message(user_id, "😔 Пока нет ни одного отзыва.")
        return
    for rev in reviews[:20]:
        name = rev['first_name'] or rev['username'] or 'Аноним'
        date = rev['created_at'].strftime('%d.%m.%Y') if isinstance(rev['created_at'], datetime) else rev['created_at']
        text = f"⭐️ *{name}* ({date}) – {rev['zone']}\n"
        text += f"Оценка: {'⭐️' * rev['rating']}\n"
        text += f"“{rev['review_text']}”"
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_review_{rev['id']}"),
            types.InlineKeyboardButton("❌ Удалить", callback_data=f"delete_review_{rev['id']}")
        )
        if rev['photo']:
            try:
                with open(rev['photo'], 'rb') as photo:
                    bot.send_photo(user_id, photo, caption=text, parse_mode='Markdown', reply_markup=markup)
            except:
                bot.send_message(user_id, text + "\n(Фото недоступно)", parse_mode='Markdown', reply_markup=markup)
        else:
            bot.send_message(user_id, text, parse_mode='Markdown', reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('edit_review_'))
def edit_review_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    review_id = int(call.data.split('_')[2])
    user_states[MASTER_ID] = {'editing_review': review_id}
    msg = bot.send_message(MASTER_ID, "✏️ Введите новый текст отзыва:")
    bot.register_next_step_handler(msg, process_edit_review)

def process_edit_review(message):
    if message.from_user.id != MASTER_ID:
        return
    review_id = user_states.get(MASTER_ID, {}).get('editing_review')
    if not review_id:
        return
    new_text = message.text.strip()
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute("UPDATE reviews SET review_text = ? WHERE id = ?", (new_text, review_id))
    conn.commit()
    conn.close()
    bot.send_message(MASTER_ID, "✅ Отзыв обновлён.")
    del user_states[MASTER_ID]

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_review_'))
def delete_review_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    review_id = int(call.data.split('_')[2])
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM reviews WHERE id = ?", (review_id,))
    conn.commit()
    conn.close()
    bot.answer_callback_query(call.id, "✅ Отзыв удалён.")
    bot.send_message(MASTER_ID, f"🗑 Отзыв #{review_id} удалён.")

# -------------------- МАСТЕР: ДОБАВЛЕНИЕ СЛОТОВ --------------------
@bot.message_handler(func=lambda message: message.text == "➕ Добавить слот" and message.from_user.id == MASTER_ID)
def add_slot_start(message):
    user_id = message.from_user.id
    msg = bot.send_message(user_id, "🕐 Введите дату и время слота в формате ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 25.12.2025 14:30")
    bot.register_next_step_handler(msg, process_add_slot)

def process_add_slot(message):
    user_id = message.from_user.id
    try:
        slot_time_str = message.text.strip()
        slot_time = datetime.strptime(slot_time_str, "%d.%m.%Y %H:%M")
        if slot_time < datetime.now():
            bot.send_message(user_id, "❌ Время должно быть в будущем. Попробуйте ещё раз.")
            return
        master_id = 1
        db.add_slot(master_id, None, slot_time)
        bot.send_message(user_id, f"✅ Слот на {slot_time.strftime('%d.%m.%Y %H:%M')} добавлен.", reply_markup=get_main_menu(user_id))
    except ValueError:
        bot.send_message(user_id, "❌ Неверный формат. Используйте ДД.ММ.ГГГГ ЧЧ:ММ. Попробуйте снова.")

# -------------------- АВТОГЕНЕРАЦИЯ СЛОТОВ --------------------
@bot.message_handler(func=lambda message: message.text == "⚙️ Сгенерировать слоты" and message.from_user.id == MASTER_ID)
def ask_slot_template(message):
    user_id = message.from_user.id
    msg = bot.send_message(user_id, "📋 Введите шаблон для генерации слотов.\n"
                                     "Формат:\n"
                                     "Будни: время1, время2 (через запятую)\n"
                                     "Выходные: начало-конец, шаг (в минутах)\n"
                                     "Например:\n"
                                     "будни: 15:15, 18:00\n"
                                     "выходные: 10:00-18:00, 120")
    bot.register_next_step_handler(msg, process_slot_template)

def process_slot_template(message):
    user_id = message.from_user.id
    lines = message.text.strip().split('\n')
    if len(lines) < 2:
        bot.send_message(user_id, "❌ Неверный формат. Попробуйте ещё раз.")
        return
    try:
        weekday_line = lines[0].replace("будни:", "").strip()
        weekday_times = [t.strip() for t in weekday_line.split(',')]
        weekend_line = lines[1].replace("выходные:", "").strip()
        if '-' in weekend_line and ',' in weekend_line:
            range_part, step_part = weekend_line.split(',')
            start_end = range_part.strip().split('-')
            start = datetime.strptime(start_end[0].strip(), "%H:%M").time()
            end = datetime.strptime(start_end[1].strip(), "%H:%M").time()
            step = int(step_part.strip())
        else:
            bot.send_message(user_id, "❌ Неверный формат выходных. Должно быть: начало-конец, шаг_минут")
            return
    except Exception as e:
        bot.send_message(user_id, f"❌ Ошибка парсинга: {e}")
        return

    generated = 0
    for i in range(7):
        day = datetime.now().date() + timedelta(days=i)
        if day.weekday() < 5:
            for t_str in weekday_times:
                t = datetime.strptime(t_str, "%H:%M").time()
                slot_time = datetime.combine(day, t)
                if slot_time > datetime.now():
                    db.add_slot(1, None, slot_time)
                    generated += 1
        else:
            current = datetime.combine(day, start)
            end_dt = datetime.combine(day, end)
            while current <= end_dt:
                if current > datetime.now():
                    db.add_slot(1, None, current)
                    generated += 1
                current += timedelta(minutes=step)

    bot.send_message(user_id, f"✅ Сгенерировано {generated} слотов на ближайшие 7 дней.", reply_markup=get_main_menu(user_id))

# -------------------- МАСТЕР: ПРОСМОТР И ПОДТВЕРЖДЕНИЕ ЗАПИСЕЙ --------------------
@bot.message_handler(func=lambda message: message.text == "✅ Подтвердить записи" and message.from_user.id == MASTER_ID)
def show_pending_appointments(message):
    user_id = message.from_user.id
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT a.id, a.zone, a.slot_time, a.is_primary, a.price, u.user_id, u.username, u.first_name, u.last_name, u.phone
        FROM appointments a
        JOIN users u ON a.user_id = u.user_id
        WHERE a.status = 'booked'
        ORDER BY a.slot_time
    ''')
    rows = cur.fetchall()
    conn.close()
    if not rows:
        bot.send_message(user_id, "📭 Нет записей, ожидающих подтверждения.")
        return
    for row in rows:
        appointment_id = row['id']
        zone = row['zone'] or "не указана"
        slot_time_str = row['slot_time'].strftime('%d.%m.%Y %H:%M')
        proc_type = "первичная" if row['is_primary'] else "коррекция"
        client_info = f"👤 {row['first_name']} {row['last_name']} (@{row['username']}), тел: {row['phone'] or 'не указан'}"
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{appointment_id}"),
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{appointment_id}"),
            types.InlineKeyboardButton("✏️ Изменить время", callback_data=f"edit_{appointment_id}")
        )
        bot.send_message(user_id, f"📅 Запись #{appointment_id}\n📍 Зона: {zone}\n🕐 Время: {slot_time_str}\n🔹 Тип: {proc_type}\n💰 Цена: {row['price']}₽\n{client_info}", reply_markup=markup)

@bot.message_handler(func=lambda message: message.text == "📅 Показать все записи" and message.from_user.id == MASTER_ID)
def show_all_appointments(message):
    user_id = message.from_user.id
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT a.id, a.zone, a.slot_time, a.status, a.is_primary, a.price, u.user_id, u.username, u.first_name, u.last_name, u.phone
        FROM appointments a
        LEFT JOIN users u ON a.user_id = u.user_id
        WHERE a.status IN ('booked', 'confirmed')
        ORDER BY a.slot_time
    ''')
    rows = cur.fetchall()
    conn.close()
    if not rows:
        bot.send_message(user_id, "📭 Нет активных записей.")
        return
    for row in rows:
        appointment_id = row['id']
        zone = row['zone'] or "не указана"
        status = "⏳ ожидает" if row['status'] == 'booked' else "🔒 подтверждена"
        slot_time_str = row['slot_time'].strftime('%d.%m.%Y %H:%M')
        proc_type = "первичная" if row['is_primary'] else "коррекция"
        client_info = f"👤 {row['first_name']} {row['last_name']} (@{row['username']}), тел: {row['phone'] or 'не указан'}"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✏️ Изменить время", callback_data=f"edit_{appointment_id}"))
        if row['status'] == 'booked':
            markup.add(
                types.InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{appointment_id}"),
                types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{appointment_id}")
            )
        bot.send_message(user_id, f"📅 Запись #{appointment_id} ({status})\n📍 Зона: {zone}\n🕐 Время: {slot_time_str}\n🔹 Тип: {proc_type}\n💰 Цена: {row['price']}₽\n{client_info}", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('edit_') and call.from_user.id == MASTER_ID)
def handle_edit_time(call):
    master_id = call.from_user.id
    try:
        appointment_id = int(call.data.split('_')[1])
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "❌ Ошибка: некорректные данные.")
        return
    user_states[master_id] = {'editing_appointment': appointment_id}
    msg = bot.send_message(master_id, "✏️ Введите новую дату и время в формате ДД.ММ.ГГГГ ЧЧ:ММ")
    bot.register_next_step_handler(msg, process_new_time)
    bot.answer_callback_query(call.id)

def process_new_time(message):
    master_id = message.from_user.id
    if master_id != MASTER_ID:
        return
    appointment_id = user_states.get(master_id, {}).get('editing_appointment')
    if not appointment_id:
        bot.send_message(master_id, "❌ Ошибка: не найден ID записи.")
        return
    try:
        new_time_str = message.text.strip()
        new_time = datetime.strptime(new_time_str, "%d.%m.%Y %H:%M")
        if new_time < datetime.now():
            bot.send_message(master_id, "❌ Время должно быть в будущем. Попробуйте ещё раз.")
            return
        appointment = db.get_appointment_by_id(appointment_id)
        if not appointment:
            bot.send_message(master_id, "❌ Запись не найдена.")
            del user_states[master_id]
            return
        client_id = appointment['user_id']
        zone = appointment['zone']
        status = appointment['status']
        new_time_iso = new_time.isoformat()
        conn = db.get_db()
        cur = conn.cursor()
        cur.execute("UPDATE appointments SET slot_time = ? WHERE id = ?", (new_time_iso, appointment_id))
        conn.commit()
        conn.close()
        new_time_str_formatted = new_time.strftime('%d.%m.%Y %H:%M')
        if client_id:
            if status == 'confirmed':
                bot.send_message(client_id, f"⚠️ Время вашей записи изменено мастером. Новое время: {new_time_str_formatted} (зона: {zone}).")
            else:
                bot.send_message(client_id, f"🕒 Время вашей записи обновлено: {new_time_str_formatted} (зона: {zone}). Ожидайте подтверждения.")
        bot.send_message(master_id, f"✅ Время записи #{appointment_id} успешно изменено на {new_time_str_formatted}.")
    except ValueError:
        bot.send_message(master_id, "❌ Неверный формат. Используйте ДД.ММ.ГГГГ ЧЧ:ММ. Попробуйте снова.")
        msg = bot.send_message(master_id, "✏️ Введите новую дату и время в формате ДД.ММ.ГГГГ ЧЧ:ММ")
        bot.register_next_step_handler(msg, process_new_time)
        return
    finally:
        if master_id in user_states:
            del user_states[master_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_') or call.data.startswith('reject_'))
def handle_appointment_confirmation(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    action, appointment_id = call.data.split('_')
    appointment_id = int(appointment_id)
    if action == 'confirm':
        db.confirm_appointment(appointment_id)
        appointment = db.get_appointment_by_id(appointment_id)
        if appointment:
            client_id = appointment['user_id']
            zone = appointment['zone']
            slot_time = appointment['slot_time'].strftime('%d.%m.%Y %H:%M')
            send_preparation_guide(client_id, zone)
            maps_url = f"https://yandex.ru/maps/?text={OFFICE_ADDRESS.replace(' ', '+')}"
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🗺 Построить маршрут", url=maps_url))
            bot.send_message(client_id, f"✅ Ваша запись на {slot_time} подтверждена!\n📍 Адрес: {OFFICE_ADDRESS}", reply_markup=markup)
            ics_data = generate_ics(appointment['slot_time'], zone, OFFICE_ADDRESS)
            bot.send_document(client_id, ('appointment.ics', ics_data), caption="📅 Добавьте событие в календарь")
            bot.send_message(MASTER_ID, f"✅ Запись #{appointment_id} подтверждена.")
            user = db.get_user(client_id)
            send_to_channel(f"✅ *Запись подтверждена*\n"
                            f"Клиент: {user['first_name']} {user['last_name']} (@{user['username']})\n"
                            f"Зона: {zone}\n"
                            f"Дата/время: {slot_time}")
            reminder_date = datetime.now() + timedelta(days=35)
            db.add_correction_reminder(client_id, appointment_id, reminder_date)
        else:
            bot.send_message(MASTER_ID, "❌ Ошибка: запись не найдена.")
    elif action == 'reject':
        db.cancel_appointment(appointment_id)
        appointment = db.get_appointment_by_id(appointment_id)
        if appointment and appointment['user_id']:
            client_id = appointment['user_id']
            bot.send_message(client_id, "❌ К сожалению, ваша запись была отклонена мастером. Свяжитесь с мастером для уточнения.")
        bot.send_message(MASTER_ID, f"🗑 Запись #{appointment_id} отклонена, слот освобожден.")
    bot.answer_callback_query(call.id)

def generate_ics(appointment_time, zone, address):
    cal = icalendar.Calendar()
    cal.add('prodid', '-//Permanent Bot//mxm.dk//')
    cal.add('version', '2.0')
    event = icalendar.Event()
    event.add('summary', f'Перманентный макияж: {zone}')
    event.add('dtstart', appointment_time)
    event.add('dtend', appointment_time + timedelta(hours=2))
    event.add('location', address)
    event.add('description', f'Процедура в студии. Адрес: {address}')
    cal.add_component(event)
    return cal.to_ical()

def send_preparation_guide(user_id, zone):
    if zone == "Губы":
        text = (
            "💋 *ПОДГОТОВКА К ПЕРМАНЕНТНОМУ МАКИЯЖУ ГУБ*\n\n"
            "1️⃣ Обильно увлажняйте губы бальзамом.\n"
            "2️⃣ На ночь перед процедурой нанесите мазь метилурацил.\n"
            "3️⃣ Используйте мягкий скраб для губ.\n\n"
            "🚫 За сутки исключите:\n"
            "▪ кофе (некрепкий растворимый можно утром)\n"
            "▪ энергетики, колу\n"
            "▪ алкоголь"
        )
    elif zone == "Брови":
        text = (
            "✏️ *ПОДГОТОВКА К ПЕРМАНЕНТНОМУ МАКИЯЖУ БРОВЕЙ*\n\n"
            "За 1-2 недели:\n"
            "▪ не окрашивайте брови\n"
            "▪ не делайте ламинирование\n"
            "▪ не выщипывайте волоски\n"
            "▪ не делайте пилинги\n\n"
            "🚫 За сутки исключите:\n"
            "▪ кофе, энергетики, алкоголь"
        )
    elif zone == "Межресничка":
        text = (
            "👁 *ПОДГОТОВКА К ПЕРМАНЕНТНОМУ МАКИЯЖУ ВЕК*\n\n"
            "🚫 За сутки:\n"
            "▪ снимите нарощенные ресницы\n"
            "▪ исключите кофе, алкоголь, энергетики\n\n"
            "👓 Если носите линзы – возьмите контейнер."
        )
    else:
        text = "Подготовка: уточните у мастера."
    bot.send_message(user_id, text, parse_mode='Markdown')

# -------------------- МАСТЕР: ПРОСМОТР ОТЗЫВОВ --------------------
@bot.message_handler(func=lambda message: message.text == "📋 Посмотреть отзывы" and message.from_user.id == MASTER_ID)
def handle_show_reviews_master(message):
    show_reviews_master(message.from_user.id)

# -------------------- ВОПРОС МАСТЕРУ --------------------
@bot.message_handler(func=lambda message: message.text == "📩 Задать вопрос мастеру")
def ask_master(message):
    user_id = message.from_user.id
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("🏠 Главное меню")
    msg = bot.send_message(user_id, "✍️ Напишите ваш вопрос, и мастер ответит вам в ближайшее время:", reply_markup=markup)
    bot.register_next_step_handler(msg, forward_question_to_master)

def forward_question_to_master(message):
    user_id = message.from_user.id
    if message.text == "🏠 Главное меню":
        send_welcome(message)
        return
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✏️ Ответить", callback_data=f"reply_to_{user_id}"))
    bot.send_message(MASTER_ID, f"📩 Вопрос от клиента {user_id} (@{message.from_user.username}):", reply_markup=markup)
    bot.forward_message(MASTER_ID, user_id, message.message_id)
    bot.send_message(user_id, "✅ Ваш вопрос отправлен мастеру. Ожидайте ответа.", reply_markup=get_main_menu(user_id))

@bot.callback_query_handler(func=lambda call: call.data.startswith('reply_to_'))
def reply_to_user_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    user_id = int(call.data.split('_')[2])
    user_states[MASTER_ID] = {'replying_to': user_id}
    bot.edit_message_text("✍️ Введите ответ клиенту:", chat_id=MASTER_ID, message_id=call.message.message_id)
    bot.answer_callback_query(call.id)
    msg = bot.send_message(MASTER_ID, "Напишите ваш ответ:")
    bot.register_next_step_handler(msg, process_master_reply)

def process_master_reply(message):
    if message.from_user.id != MASTER_ID:
        return
    user_id = user_states.get(MASTER_ID, {}).get('replying_to')
    if not user_id:
        bot.send_message(MASTER_ID, "❌ Ошибка: не найден ID клиента.")
        return
    reply_text = message.text.strip()
    bot.send_message(user_id, f"📨 Ответ от мастера:\n{reply_text}")
    bot.send_message(MASTER_ID, f"✅ Ответ отправлен пользователю {user_id}.")
    del user_states[MASTER_ID]

@bot.message_handler(commands=['reply'])
def master_reply_command(message):
    if message.from_user.id != MASTER_ID:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.send_message(MASTER_ID, "Использование: /reply user_id текст")
        return
    user_id = int(parts[1])
    reply_text = parts[2]
    bot.send_message(user_id, f"📨 Ответ от мастера:\n{reply_text}")
    bot.send_message(MASTER_ID, f"✅ Ответ отправлен пользователю {user_id}.")

# -------------------- РУЧНОЕ ДОБАВЛЕНИЕ КЛИЕНТА --------------------
@bot.message_handler(func=lambda message: message.text == "➕ Ручное добавление клиента" and message.from_user.id == MASTER_ID)
def manual_add_start(message):
    msg = bot.send_message(MASTER_ID, "Введите данные в формате:\nИмя Телефон Зона(Губы/Брови/Межресничка) ГГГГ-ММ-ДД ЧЧ:ММ\nПример: Анна +79991234567 Губы 2025-03-20 15:30")
    bot.register_next_step_handler(msg, process_manual_add)

def process_manual_add(message):
    if message.from_user.id != MASTER_ID:
        return
    parts = message.text.split()
    if len(parts) < 5:
        bot.send_message(MASTER_ID, "❌ Неверный формат. Повторите ввод.")
        return
    name, phone, zone, date_str, time_str = parts[0], parts[1], parts[2], parts[3], parts[4]
    try:
        slot_time = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    except ValueError:
        bot.send_message(MASTER_ID, "❌ Неверный формат даты/времени.")
        return
    conn = db.get_db()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO manual_entries (master_id, user_name, phone, zone, appointment_time)
        VALUES (?, ?, ?, ?, ?)
    ''', (MASTER_ID, name, phone, zone, slot_time))
    conn.commit()
    conn.close()
    bot.send_message(MASTER_ID, f"✅ Ручная запись добавлена: {name}, {zone}, {slot_time.strftime('%d.%m.%Y %H:%M')}")

# -------------------- ЧЁРНЫЙ СПИСОК --------------------
@bot.message_handler(func=lambda message: message.text == "⛔️ Черный список" and message.from_user.id == MASTER_ID)
def blacklist_menu(message):
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("➕ Добавить в ЧС", callback_data="blacklist_add"),
        types.InlineKeyboardButton("➖ Удалить из ЧС", callback_data="blacklist_remove"),
        types.InlineKeyboardButton("📋 Просмотреть ЧС", callback_data="blacklist_view"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
    )
    bot.send_message(message.from_user.id, "⛔️ Управление черным списком:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "blacklist_add")
def blacklist_add_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    bot.edit_message_text("Введите ID пользователя (и, через пробел, причину блокировки, если нужно):", 
                          chat_id=call.from_user.id, message_id=call.message.message_id)
    bot.register_next_step_handler_by_chat_id(call.from_user.id, process_blacklist_add)
    bot.answer_callback_query(call.id)

def process_blacklist_add(message):
    if message.from_user.id != MASTER_ID:
        return
    text = message.text.strip()
    parts = text.split(maxsplit=1)
    try:
        user_id = int(parts[0])
        reason = parts[1] if len(parts) > 1 else None
    except:
        bot.send_message(MASTER_ID, "❌ Некорректный формат. Введите ID пользователя и, если хотите, причину.")
        return
    db.add_to_blacklist(user_id, reason)
    bot.send_message(MASTER_ID, f"✅ Пользователь {user_id} добавлен в чёрный список. Причина: {reason or 'не указана'}")

@bot.callback_query_handler(func=lambda call: call.data == "blacklist_remove")
def blacklist_remove_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    bot.edit_message_text("Введите ID пользователя для удаления из чёрного списка:", 
                          chat_id=call.from_user.id, message_id=call.message.message_id)
    bot.register_next_step_handler_by_chat_id(call.from_user.id, process_blacklist_remove)
    bot.answer_callback_query(call.id)

def process_blacklist_remove(message):
    if message.from_user.id != MASTER_ID:
        return
    try:
        user_id = int(message.text.strip())
    except:
        bot.send_message(MASTER_ID, "❌ Введите корректный ID.")
        return
    db.remove_from_blacklist(user_id)
    bot.send_message(MASTER_ID, f"✅ Пользователь {user_id} удалён из чёрного списка.")

@bot.callback_query_handler(func=lambda call: call.data == "blacklist_view")
def blacklist_view_callback(call):
    if call.from_user.id != MASTER_ID:
        bot.answer_callback_query(call.id, "❌ У вас нет прав.")
        return
    blacklist = db.get_blacklist()
    if not blacklist:
        bot.send_message(MASTER_ID, "📭 Чёрный список пуст.")
        return
    text = "⛔️ *Чёрный список*\n\n"
    for row in blacklist:
        user_id = row['user_id']
        name = f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "—"
        username = f"@{row['username']}" if row['username'] else "—"
        reason = row['reason'] or "не указана"
        added = row['added_at'].strftime('%d.%m.%Y %H:%M') if isinstance(row['added_at'], datetime) else row['added_at']
        text += f"• ID: `{user_id}`\n  Имя: {name}\n  Username: {username}\n  Причина: {reason}\n  Добавлен: {added}\n\n"
    bot.send_message(MASTER_ID, text, parse_mode='Markdown')
    bot.answer_callback_query(call.id)

@bot.message_handler(commands=['blacklist_add'])
def cmd_blacklist_add(message):
    if message.from_user.id != MASTER_ID:
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        bot.send_message(MASTER_ID, "Использование: /blacklist_add user_id [причина]")
        return
    try:
        user_id = int(parts[1])
    except:
        bot.send_message(MASTER_ID, "❌ Некорректный ID.")
        return
    reason = parts[2] if len(parts) > 2 else None
    db.add_to_blacklist(user_id, reason)
    bot.send_message(MASTER_ID, f"✅ Пользователь {user_id} добавлен в чёрный список.")

@bot.message_handler(commands=['blacklist_remove'])
def cmd_blacklist_remove(message):
    if message.from_user.id != MASTER_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(MASTER_ID, "Использование: /blacklist_remove user_id")
        return
    try:
        user_id = int(parts[1])
    except:
        bot.send_message(MASTER_ID, "❌ Некорректный ID.")
        return
    db.remove_from_blacklist(user_id)
    bot.send_message(MASTER_ID, f"✅ Пользователь {user_id} удалён из чёрного списка.")

@bot.message_handler(commands=['blacklist_view'])
def cmd_blacklist_view(message):
    if message.from_user.id != MASTER_ID:
        return
    blacklist = db.get_blacklist()
    if not blacklist:
        bot.send_message(MASTER_ID, "📭 Чёрный список пуст.")
        return
    text = "⛔️ *Чёрный список*\n\n"
    for row in blacklist:
        user_id = row['user_id']
        name = f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "—"
        username = f"@{row['username']}" if row['username'] else "—"
        reason = row['reason'] or "не указана"
        added = row['added_at'].strftime('%d.%m.%Y %H:%M') if isinstance(row['added_at'], datetime) else row['added_at']
        text += f"• ID: `{user_id}`\n  Имя: {name}\n  Username: {username}\n  Причина: {reason}\n  Добавлен: {added}\n\n"
    bot.send_message(MASTER_ID, text, parse_mode='Markdown')

# -------------------- РЕЗЕРВНОЕ КОПИРОВАНИЕ --------------------
def backup_worker():
    while True:
        time.sleep(24 * 3600)
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        try:
            shutil.copyfile('bot_database.db', backup_name)
            with open(backup_name, 'rb') as f:
                bot.send_document(BACKUP_CHANNEL, f, caption=f"📀 Резервная копия {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            os.remove(backup_name)
        except Exception as e:
            print(f"Backup error: {e}")

# -------------------- ФОНОВЫЕ ЗАДАЧИ --------------------
def reminder_worker():
    while True:
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        conn = db.get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT a.user_id, a.zone, a.slot_time FROM appointments a
            WHERE a.status = 'confirmed' AND date(a.slot_time) = date(?)
        ''', (tomorrow.date(),))
        rows = cur.fetchall()
        conn.close()
        for row in rows:
            client_id = row['user_id']
            zone = row['zone']
            slot_str = row['slot_time'].strftime('%d.%m.%Y %H:%M')
            bot.send_message(client_id, f"🔔 Напоминаем: завтра в {slot_str} у вас запись на {zone}.")
        time.sleep(3600)

def correction_reminder_worker():
    while True:
        now = datetime.now()
        conn = db.get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT id, user_id FROM correction_reminders
            WHERE sent = 0 AND reminder_date <= ?
        ''', (now,))
        rows = cur.fetchall()
        for row in rows:
            reminder_id, user_id = row['id'], row['user_id']
            bot.send_message(user_id, "⏰ Напоминание: прошло около месяца после процедуры. Возможно, пора записаться на коррекцию!")
            cur.execute("UPDATE correction_reminders SET sent = 1 WHERE id = ?", (reminder_id,))
        conn.commit()
        conn.close()
        time.sleep(3600)

threading.Thread(target=reminder_worker, daemon=True).start()
threading.Thread(target=correction_reminder_worker, daemon=True).start()
threading.Thread(target=backup_worker, daemon=True).start()

# -------------------- ОСНОВНОЙ ОБРАБОТЧИК --------------------
@bot.message_handler(func=lambda message: True)
def handle_buttons(message):
    pass

# -------------------- ЗАПУСК --------------------
if __name__ == '__main__':
    print("Бот запущен...")
    bot.infinity_polling()