import requests
from config import BOT_TOKEN
requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook")
print("Webhook deleted")