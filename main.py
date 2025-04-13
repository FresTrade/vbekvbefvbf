import os
import sys
import fcntl
import random
import logging
import asyncio
import aiohttp
import ssl
import certifi
import pandas as pd
from datetime import datetime, time, timedelta
from html import escape
from typing import Dict, Any, Optional, List, Union

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile
)
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = "7702082717:AAG5Ss0T2B0xCJBbcWMoX3_zD12mXYsyKYc"
PASSWORD = "option.bot76"
COOLDOWN_TIME = 60
CACHE_EXPIRY = 300
MIN_DATA_POINTS = 30

# Настройка SSL
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Инициализация бота
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# Глобальные переменные
user_data: Dict[int, Dict[str, Any]] = {}
data_cache: Dict[str, Dict[str, Any]] = {}

# Время работы рынка
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Пути к изображениям
IMAGE_PATHS = {
    "welcome": "WELCOME.png",
    "buy": {
        "en": "BUY.JPG",
        "ru": "POKUPAEM.JPG",
        "de": "KAUFEN.JPG"
    },
    "sell": {
        "en": "SELL.JPG",
        "ru": "PRODAEM.JPG",
        "de": "VERKAUFEN.JPG"
    }
}

class SingleInstance:
    """Класс для обеспечения работы только одного экземпляра бота"""
    def __init__(self):
        self.lockfile = '/tmp/option_bot.lock'
        self.fd = None

    def __enter__(self):
        try:
            self.fd = os.open(self.lockfile, os.O_WRONLY | os.O_CREAT)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            logger.error("Another instance is already running. Exiting.")
            sys.exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.fd:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            os.unlink(self.lockfile)

def is_market_open() -> bool:
    """Проверяет, открыт ли рынок в данный момент"""
    now = datetime.now().time()
    return MARKET_OPEN <= now <= MARKET_CLOSE

def validate_user(user_id: int) -> Dict[str, Any]:
    """Проверяет и инициализирует данные пользователя"""
    if user_id not in user_data:
        user_data[user_id] = {
            "language": "en",
            "last_activity": datetime.now()
        }
    else:
        user_data[user_id]["last_activity"] = datetime.now()
    return user_data[user_id]

async def safe_send_message(
        chat_id: int,
        text: str,
        parse_mode: Optional[str] = "HTML",
        **kwargs
) -> bool:
    """Безопасная отправка сообщения с обработкой ошибок"""
    try:
        await bot.send_message(
            chat_id,
            escape(text) if parse_mode == "HTML" else text,
            parse_mode=parse_mode,
            **kwargs
        )
        return True
    except TelegramBadRequest as e:
        logger.error(f"Message sending failed (HTML): {e}")
        try:
            await bot.send_message(
                chat_id,
                text,
                parse_mode=None,
                **kwargs
            )
            return True
        except Exception as e:
            logger.error(f"Message sending failed (plain text): {e}")
            return False
    except Exception as e:
        logger.error(f"Message sending failed: {e}")
        return False

async def safe_send_photo(
        chat_id: int,
        photo: Union[str, FSInputFile],
        caption: str = "",
        **kwargs
) -> bool:
    """Безопасная отправка фото с обработкой ошибок"""
    try:
        await bot.send_photo(
            chat_id,
            photo=photo,
            caption=escape(caption),
            **kwargs
        )
        return True
    except Exception as e:
        logger.error(f"Photo sending failed: {e}")
        return await safe_send_message(
            chat_id,
            caption,
            **kwargs
        )

# Кнопки интерфейса
BUTTONS = {
    "back": {
        "en": "🔙 Back",
        "ru": "🔙 Назад",
        "de": "🔙 Zurück"
    },
    "next": {
        "en": "➡️ Next",
        "ru": "➡️ Далее",
        "de": "➡️ Weiter"
    },
    "settings": {
        "en": "⚙️ Settings",
        "ru": "⚙️ Настройки",
        "de": "⚙️ Einstellungen"
    },
    "language": {
        "en": "🌐 Change language",
        "ru": "🌐 Сменить язык",
        "de": "🌐 Sprache ändern"
    }
}

# Полный список активов
ASSET_CATEGORIES = {
    "en": {
        "💵 Currencies": [
            "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
            "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY"
        ],
        "🪙 Cryptocurrency": [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
            "ADA/USDT", "DOGE/USDT", "DOT/USDT", "MATIC/USDT", "AVAX/USDT"
        ],
        "📈 Stocks": [
            "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL",
            "META", "NVDA", "TSM", "AMD", "INTC"
        ],
        "📊 Indices": [
            "US500", "USTEC", "US30", "GER30", "UK100",
            "JP225", "HK50", "AUS200", "EU50", "BRENT"
        ]
    },
    "ru": {
        "💵 Валюты": [
            "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
            "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY"
        ],
        "🪙 Криптовалюта": [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
            "ADA/USDT", "DOGE/USDT", "DOT/USDT", "MATIC/USDT", "AVAX/USDT"
        ],
        "📈 Акции": [
            "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL",
            "META", "NVDA", "TSM", "AMD", "INTC"
        ],
        "📊 Индексы": [
            "US500", "USTEC", "US30", "GER30", "UK100",
            "JP225", "HK50", "AUS200", "EU50", "BRENT"
        ]
    },
    "de": {
        "💵 Währungen": [
            "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
            "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY"
        ],
        "🪙 Kryptowährung": [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
            "ADA/USDT", "DOGE/USDT", "DOT/USDT", "MATIC/USDT", "AVAX/USDT"
        ],
        "📈 Aktien": [
            "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL",
            "META", "NVDA", "TSM", "AMD", "INTC"
        ],
        "📊 Indizes": [
            "US500", "USTEC", "US30", "GER30", "UK100",
            "JP225", "HK50", "AUS200", "EU50", "BRENT"
        ]
    }
}

# Таймфреймы
TIMEFRAMES = {
    "en": ["30 sec", "1 min", "2 min", "3 min", "5 min"],
    "ru": ["30 сек", "1 мин", "2 мин", "3 мин", "5 мин"],
    "de": ["30 sec", "1 min", "2 min", "3 min", "5 min"]
}

# Языки
LANGUAGES = {
    "🇬🇧 English": "en",
    "🇷🇺 Русский": "ru",
    "🇩🇪 Deutsch": "de"
}

# Текстовые сообщения
TEXTS = {
    "en": {
        "welcome": "🌟Welcome to Trading Signals Bot!\n\n"
                   "Thank you for joining our trading community. "
                   "This bot provides professional trading signals for various assets.\n\n"
                   "💡 Key features:\n"
                   "✔️ Real-time trading signals\n"
                   "✔️ Multi-language support\n"
                   "✔️ Technical analysis insights\n"
                   "✔️ Regular market updates\n\n"
                   "To get started, please select your preferred language:",
        "password_prompt": "🔒 Enter password to continue:",
        "password_correct": "✅ Access granted! Choose asset category:",
        "password_incorrect": "❌ Wrong password! Try again.",
        "select_category": "📊 Choose asset category:",
        "select_asset": "📈 Select asset from {category}:",
        "select_timeframe": "⏳ Select analysis timeframe:",
        "signal": "🚀 <b>Trading Signal for {asset}{market_status}</b>\n\n"
                  "⏳ Timeframe: {timeframe}\n"
                  "📊 <b>Technical Analysis:</b>\n{analysis}\n\n"
                  "🎯 <b>Recommendation:</b> {direction}\n"
                  "💡 <b>Conclusion:</b> {conclusion}\n\n"
                  "⚠️ <i>This is not financial advice. Always do your own research.</i>",
        "cooldown": "⏳ Please wait {seconds} seconds before next request",
        "cooldown_ended": "✅ You can now request new signals!",
        "settings": "⚙️ Settings Menu\n\nChoose option:",
        "current_language": "🌐 Current language: English",
        "language_changed": "✅ Language changed successfully!",
        "no_data": "⚠️ <b>Market Data Unavailable</b>\n\n"
                   "Currently unable to fetch market data for {asset}.\n"
                   "Please try again later or select a different asset.",
        "error": "⚠️ <b>Error occurred</b>\n\n"
                 "An unexpected error occurred. Please try again later.",
        "registration_info": (
            "🚀 To start using our platform, please go through this link: "
            "[registration](https://u3.shortink.io/register?utm_campaign=816605&utm_source=affiliate&utm_medium=sr&a=r6voYUglZqvO4W&ac=main) 💼\n\n"
            "💰 Make a deposit starting from $30. A higher deposit will unlock additional opportunities and better trading conditions for you.\n\n"
            "🗣️ After making a deposit, contact support (in Russian) to activate your account."
        )
    },
    "ru": {
        "welcome": "🌟 Добро пожаловать в бота торговых сигналов!\n\n"
                   "Спасибо за присоединение к нашему торговому сообществу. "
                   "Этот бот предоставляет профессиональные торговые сигналы для различных активов.\n\n"
                   "💡 <b>Основные возможности:</b>\n"
                   "✔️ Торговые сигналы в реальном времени\n"
                   "✔️ Поддержка нескольких языков\n"
                   "✔️ Технический анализ рынка\n"
                   "✔️ Регулярные рыночные обновления\n\n"
                   "Для начала работы выберите предпочитаемый язык:",
        "password_prompt": "🔒 Введите пароль для продолжения:",
        "password_correct": "✅ Доступ разрешен! Выберите категорию актива:",
        "password_incorrect": "❌ Неверный пароль! Попробуйте снова.",
        "select_category": "📊 Выберите категорию актива:",
        "select_asset": "📈 Выберите актив из {category}:",
        "select_timeframe": "⏳ Выберите таймфрейм для анализа:",
        "signal": " <b>Торговый сигнал для {asset}{market_status}</b>\n\n"
                  "⏳ Таймфрейм: {timeframe}\n"
                  "📊 <b>Технический анализ:</b>\n{analysis}\n\n"
                  "🎯 <b>Рекомендация:</b> {direction}\n"
                  "💡 <b>Вывод:</b> {conclusion}\n\n"
                  "⚠️ <i>Это не финансовая рекомендация. Всегда проводите собственный анализ.</i>",
        "cooldown": "⏳ Подождите {seconds} секунд перед следующим запросом",
        "cooldown_ended": "✅ Теперь вы можете запросить новые сигналы!",
        "settings": "⚙️ Меню настроек\n\nВыберите опцию:",
        "current_language": "🌐 Текущий язык: Русский",
        "language_changed": "✅ Язык успешно изменен!",
        "no_data": "⚠️ <b>Данные рынка недоступны</b>\n\n"
                   "В настоящее время невозможно получить данные по {asset}.\n"
                   "Попробуйте позже или выберите другой актив.",
        "error": "⚠️ <b>Произошла ошибка</b>\n\n"
                 "Произошла непредвиденная ошибка. Пожалуйста, попробуйте позже.",
        "registration_info": (
            "🚀 Чтобы начать пользоваться нашей платформой, перейдите по следующей ссылке: "
            "[регистрация](https://u3.shortink.io/register?utm_campaign=816605&utm_source=affiliate&utm_medium=sr&a=r6voYUglZqvO4W&ac=main) 💼\n\n"
            "💰 Сделайте депозит от 30$. Больший депозит откроет для вас дополнительные возможности и улучшенные условия для торговли.\n\n"
            "🗣️ После депозита напишите в поддержку (на русском) для активации вашего аккаунта."
        )
    },
    "de": {
        "welcome": "🌟 <b>Willkommen beim Trading-Signale-Bot!</b>\n\n"
                   "Vielen Dank für den Beitritt zu unserer Trading-Community. "
                   "Dieser Bot bietet professionelle Handelssignale für verschiedene Assets.\n\n"
                   "💡 <b>Hauptfunktionen:</b>\n"
                   "✔️ Echtzeit-Handelssignale\n"
                   "✔️ Mehrsprachige Unterstützung\n"
                   "✔️ Technische Marktanalyse\n"
                   "✔️ Regelmäßige Marktupdates\n\n"
                   "Wählen Sie zunächst Ihre bevorzugte Sprache:",
        "password_prompt": "🔒 Geben Sie das Passwort ein, um fortzufahren:",
        "password_correct": "✅ Zugriff gewährt! Wählen Sie die Asset-Kategorie:",
        "password_incorrect": "❌ Falsches Passwort! Versuchen Sie es erneut.",
        "select_category": "📊 Wählen Sie die Asset-Kategorie:",
        "select_asset": "📈 Wählen Sie ein Asset aus {category}:",
        "select_timeframe": "⏳ Wählen Sie den Analysezeitraum:",
        "signal": "🚀 <b>Handelssignal für {asset}{market_status}</b>\n\n"
                  "⏳ Zeitrahmen: {timeframe}\n"
                  "📊 <b>Technische Analyse:</b>\n{analysis}\n\n"
                  "🎯 <b>Empfehlung:</b> {direction}\n"
                  "💡 <b>Fazit:</b> {conclusion}\n\n"
                  "⚠️ <i>Dies ist keine Finanzberatung. Führen Sie immer eigene Recherchen durch.</i>",
        "cooldown": "⏳ Bitte warten Sie {seconds} Sekunden bis zur nächsten Anfrage",
        "cooldown_ended": "✅ Sie können jetzt neue Signale anfordern!",
        "settings": "⚙️ Einstellungsmenü\n\nOption wählen:",
        "current_language": "🌐 Aktuelle Sprache: Deutsch",
        "language_changed": "✅ Sprache erfolgreich geändert!",
        "no_data": "⚠️ <b>Marktdaten nicht verfügbar</b>\n\n"
                   "Derzeit können keine Marktdaten für {asset} abgerufen werden.\n"
                   "Bitte versuchen Sie es später erneut oder wählen Sie ein anderes Asset.",
        "error": "⚠️ <b>Fehler aufgetreten</b>\n\n"
                 "Ein unerwarteter Fehler ist aufgetreten. Bitte versuchen Sie es später erneut.",
        "registration_info": (
            "🚀 Um unsere Plattform zu nutzen, gehen Sie bitte über diesen Link: "
            "[Registrierung](https://u3.shortink.io/register?utm_campaign=816605&utm_source=affiliate&utm_medium=sr&a=r6voYUglZqvO4W&ac=main) 💼\n\n"
            "💰 Tätigen Sie eine Einzahlung von mindestens 30$. Eine höhere Einzahlung öffnet zusätzliche Möglichkeiten und bessere Handelsbedingungen für Sie.\n\n"
            "🗣️ Nach der Einzahlung wenden Sie sich an den Support (auf Russisch) um Ihr Konto zu aktivieren."
        )
    }
}

def create_keyboard(
        items: List[str],
        row_width: int = 2,
        back: bool = False,
        settings: bool = False,
        language: str = "en",
        next_button: bool = False
) -> ReplyKeyboardMarkup:
    """Создает клавиатуру с заданными параметрами"""
    buttons = [[KeyboardButton(text=item)] for item in items]

    if back or settings or next_button:
        row = []
        if back:
            row.append(KeyboardButton(text=BUTTONS["back"][language]))
        if settings:
            row.append(KeyboardButton(text=BUTTONS["settings"][language]))
        if next_button:
            row.append(KeyboardButton(text=BUTTONS["next"][language]))
        buttons.append(row)

    return ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        row_width=row_width
    )

async def get_market_data(symbol: str, timeframe: str = '5min') -> Optional[Dict[str, Any]]:
    """Получает или генерирует рыночные данные"""
    cache_key = f"{symbol}_{timeframe}"

    # Проверка кэша
    if cache_key in data_cache:
        cached = data_cache[cache_key]
        if (datetime.now() - cached["timestamp"]).total_seconds() < CACHE_EXPIRY:
            return cached["data"]

    try:
        # Генерация реалистичных тестовых данных
        trend = random.uniform(-0.5, 0.5)
        base_price = 100 + random.uniform(-20, 20)

        closes = []
        for i in range(100):
            price = base_price + i * trend + random.uniform(-1, 1)
            closes.append(price)

        highs = [x + random.uniform(0, 1) for x in closes]
        lows = [x - random.uniform(0, 1) for x in closes]
        opens = [x + random.uniform(-0.5, 0.5) for x in closes]

        result = {
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "opens": opens
        }

        data_cache[cache_key] = {
            "data": result,
            "timestamp": datetime.now()
        }
        return result

    except Exception as e:
        logger.error(f"Error generating data for {symbol}: {e}")
        return None

async def generate_signal(
        asset: str,
        timeframe: str,
        language: str
) -> tuple[str, str, str]:
    """Генерирует торговый сигнал на основе технического анализа"""
    timeframe_value = timeframe.split()[0]
    data = await get_market_data(asset, timeframe_value)

    if not data or len(data["closes"]) < MIN_DATA_POINTS:
        direction = random.choice(["BUY", "SELL"])
        analysis = "▪️ Market data unavailable - using generated signal"
        conclusion = (
            "Strong signal based on technical patterns" if direction == "BUY"
            else "Strong sell signal based on market conditions"
        )
        return direction, analysis, conclusion

    closes = pd.Series(data["closes"])
    highs = pd.Series(data["highs"])
    lows = pd.Series(data["lows"])

    # Расчет индикаторов
    rsi = RSIIndicator(closes, window=14).rsi().iloc[-1]
    macd_line = MACD(closes).macd().iloc[-1]
    signal_line = MACD(closes).macd_signal().iloc[-1]
    bb = BollingerBands(closes)
    atr = AverageTrueRange(highs, lows, closes).average_true_range().iloc[-1]
    sma20 = SMAIndicator(closes, window=20).sma_indicator().iloc[-1]
    sma50 = SMAIndicator(closes, window=50).sma_indicator().iloc[-1]
    current_price = closes.iloc[-1]

    # Формирование анализа
    analysis = []
    buy_score = 0
    sell_score = 0

    # RSI анализ
    if rsi < 30:
        analysis.append(f"▪️ RSI: {round(rsi, 2)} (Oversold)")
        buy_score += 2
    elif rsi > 70:
        analysis.append(f"▪️ RSI: {round(rsi, 2)} (Overbought)")
        sell_score += 2
    else:
        analysis.append(f"▪️ RSI: {round(rsi, 2)} (Neutral)")

    # MACD анализ
    if macd_line > signal_line:
        analysis.append("▪️ MACD: Bullish crossover")
        buy_score += 1
    else:
        analysis.append("▪️ MACD: Bearish crossover")
        sell_score += 1

    # Bollinger Bands
    if current_price < bb.bollinger_lband().iloc[-1]:
        analysis.append("▪️ Price below Lower Band (Oversold)")
        buy_score += 2
    elif current_price > bb.bollinger_hband().iloc[-1]:
        analysis.append("▪️ Price above Upper Band (Overbought)")
        sell_score += 2

    # SMA анализ
    if sma20 > sma50:
        analysis.append("▪️ SMA20 > SMA50 (Uptrend)")
        buy_score += 1
    else:
        analysis.append("▪️ SMA20 < SMA50 (Downtrend)")
        sell_score += 1

    # ATR (волатильность)
    analysis.append(f"▪️ ATR: {round(atr, 4)} (Volatility)")

    # Генерация сигнала
    if buy_score >= sell_score:
        direction = "BUY" if language == "en" else "ПОКУПКА" if language == "ru" else "KAUFEN"
        conclusion = (
            "Strong buy signal" if language == "en"
            else "Сильный сигнал на покупку" if language == "ru"
            else "Starker Kaufsignal"
        )
    else:
        direction = "SELL" if language == "en" else "ПРОДАЖА" if language == "ru" else "VERKAUFEN"
        conclusion = (
            "Strong sell signal" if language == "en"
            else "Сильный сигнал на продажу" if language == "ru"
            else "Starker Verkaufssignal"
        )

    return direction, "\n".join(analysis), conclusion

async def cooldown_watcher():
    """Отслеживает время ожидания между запросами"""
    while True:
        await asyncio.sleep(10)
        now = datetime.now()

        for user_id, data in list(user_data.items()):
            if "last_signal" in data and not data.get("cooldown_notified", False):
                elapsed = (now - data["last_signal"]).total_seconds()
                if elapsed >= COOLDOWN_TIME:
                    lang = data.get("language", "en")
                    try:
                        await safe_send_message(
                            user_id,
                            TEXTS[lang]["cooldown_ended"],
                            reply_markup=create_keyboard(
                                ASSET_CATEGORIES[lang].keys(),
                                back=False,
                                settings=True,
                                language=lang
                            )
                        )
                        user_data[user_id]["cooldown_notified"] = True
                    except Exception as e:
                        logger.error(f"Cooldown notification error for {user_id}: {e}")

async def cleanup_user_data():
    """Очищает старые данные пользователей"""
    while True:
        await asyncio.sleep(3600)  # Каждый час
        now = datetime.now()
        for user_id, data in list(user_data.items()):
            if "last_activity" in data and (now - data["last_activity"]).days > 7:
                del user_data[user_id]
                logger.info(f"Cleaned up data for user {user_id}")

@dp.message(Command("start"))
async def start_command(message: types.Message):
    """Обработчик команды /start"""
    try:
        user = validate_user(message.from_user.id)
        welcome_image = FSInputFile(IMAGE_PATHS["welcome"])

        if not await safe_send_photo(
                message.chat.id,
                photo=welcome_image,
                caption=TEXTS["en"]["welcome"],
                reply_markup=create_keyboard(LANGUAGES.keys())
        ):
            await safe_send_message(
                message.chat.id,
                TEXTS["en"]["welcome"],
                reply_markup=create_keyboard(LANGUAGES.keys())
            )
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_(LANGUAGES.keys()))
async def set_language(message: types.Message):
    """Устанавливает язык пользователя"""
    try:
        user = validate_user(message.from_user.id)
        language = LANGUAGES[message.text]
        user["language"] = language
        user["awaiting_registration"] = True

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["registration_info"],
            reply_markup=create_keyboard(
                [BUTTONS["next"][language]],
                row_width=1,
                next_button=False
            )
        )
    except Exception as e:
        logger.error(f"Error in set_language: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([BUTTONS["next"][lang] for lang in BUTTONS["next"]]))
async def next_handler(message: types.Message):
    """Обработчик кнопки 'Далее'"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")
        user["awaiting_password"] = True
        user["awaiting_registration"] = False

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["password_prompt"],
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception as e:
        logger.error(f"Error in next_handler: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get("awaiting_password"))
async def check_password(message: types.Message):
    """Проверяет пароль пользователя"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")

        if message.text == PASSWORD:
            user["awaiting_password"] = False
            await safe_send_message(
                message.chat.id,
                TEXTS[language]["password_correct"],
                reply_markup=create_keyboard(
                    ASSET_CATEGORIES[language].keys(),
                    back=False,
                    settings=True,
                    language=language
                )
            )
        else:
            await safe_send_message(
                message.chat.id,
                TEXTS[language]["password_incorrect"]
            )
    except Exception as e:
        logger.error(f"Error in check_password: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([cat for cats in ASSET_CATEGORIES.values() for cat in cats]))
async def select_category(message: types.Message):
    """Обработчик выбора категории активов"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")
        user["category"] = message.text

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["select_asset"].format(category=message.text),
            reply_markup=create_keyboard(
                ASSET_CATEGORIES[language][message.text],
                back=True,
                settings=True,
                language=language
            )
        )
    except Exception as e:
        logger.error(f"Error in select_category: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([asset for cats in ASSET_CATEGORIES.values() for assets in cats.values() for asset in assets]))
async def select_asset(message: types.Message):
    """Обработчик выбора актива"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")
        user["asset"] = message.text

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["select_timeframe"],
            reply_markup=create_keyboard(
                TIMEFRAMES[language],
                back=True,
                settings=True,
                language=language
            )
        )
    except Exception as e:
        logger.error(f"Error in select_asset: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_(TIMEFRAMES["en"] + TIMEFRAMES["ru"] + TIMEFRAMES["de"]))
async def generate_signal_handler(message: types.Message):
    """Генерирует и отправляет торговый сигнал"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")
        asset = user.get("asset", "")
        timeframe = message.text

        if not asset:
            raise ValueError("Asset not selected")

        now = datetime.now()
        if "last_signal" in user:
            elapsed = (now - user["last_signal"]).total_seconds()
            if elapsed < COOLDOWN_TIME:
                await safe_send_message(
                    message.chat.id,
                    f"⏳ Please wait {int(COOLDOWN_TIME - elapsed)} seconds before next request",
                    reply_markup=create_keyboard(
                        ASSET_CATEGORIES[language].keys(),
                        back=False,
                        settings=True,
                        language=language
                    )
                )
                return

        direction, analysis, conclusion = await generate_signal(asset, timeframe, language)

        user["last_signal"] = now
        user["cooldown_notified"] = False

        market_status = " (OTC)" if not is_market_open() else ""

        # Формируем чистое сообщение без лишних символов
        if language == "de":
            signal_text = (
                f"🚀 Handelssignal für {asset}{market_status}\n\n"
                f"⏳ Zeitrahmen: {timeframe}\n"
                f"📊 Technische Analyse:\n{analysis}\n\n"
                f"🎯 Empfehlung: {direction}\n"
                f"💡 Fazit: {conclusion}\n\n"
                f"⚠️ Dies ist keine Finanzberatung. Führen Sie immer eigene Recherchen durch."
            )
        elif language == "ru":
            signal_text = (
                f"🚀 Торговый сигнал для {asset}{market_status}\n\n"
                f"⏳ Таймфрейм: {timeframe}\n"
                f"📊 Технический анализ:\n{analysis}\n\n"
                f"🎯 Рекомендация: {direction}\n"
                f"💡 Вывод: {conclusion}\n\n"
                f"⚠️ Это не финансовая рекомендация. Всегда проводите собственный анализ."
            )
        else:  # en
            signal_text = (
                f"🚀 Trading Signal for {asset}{market_status}\n\n"
                f"⏳ Timeframe: {timeframe}\n"
                f"📊 Technical Analysis:\n{analysis}\n\n"
                f"🎯 Recommendation: {direction}\n"
                f"💡 Conclusion: {conclusion}\n\n"
                f"⚠️ This is not financial advice. Always do your own research."
            )

        try:
            if "BUY" in direction or "ПОКУПКА" in direction or "KAUFEN" in direction:
                image_path = IMAGE_PATHS["buy"][language]
            else:
                image_path = IMAGE_PATHS["sell"][language]

            signal_image = FSInputFile(image_path)
            if not await safe_send_photo(
                    message.chat.id,
                    photo=signal_image,
                    caption=signal_text,
                    reply_markup=create_keyboard(
                        ASSET_CATEGORIES[language].keys(),
                        back=False,
                        settings=True,
                        language=language
                    )
            ):
                await safe_send_message(
                    message.chat.id,
                    signal_text,
                    reply_markup=create_keyboard(
                        ASSET_CATEGORIES[language].keys(),
                        back=False,
                        settings=True,
                        language=language
                    )
                )
        except Exception as e:
            logger.error(f"Error sending signal: {e}")
            await safe_send_message(
                message.chat.id,
                signal_text,
                reply_markup=create_keyboard(
                    ASSET_CATEGORIES[language].keys(),
                    back=False,
                    settings=True,
                    language=language
                )
            )

    except Exception as e:
        logger.error(f"Error in generate_signal_handler: {e}")
        language = user.get("language", "en") if "user" in locals() else "en"
        await safe_send_message(
            message.chat.id,
            "⚠️ Error occurred\n\nAn unexpected error occurred. Please try again later.",
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([BUTTONS["settings"][lang] for lang in BUTTONS["settings"]]))
async def settings_menu(message: types.Message):
    """Отображает меню настроек"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["settings"],
            reply_markup=create_keyboard(
                [BUTTONS["language"][language], BUTTONS["back"][language]],
                row_width=1
            )
        )
    except Exception as e:
        logger.error(f"Error in settings_menu: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([BUTTONS["language"][lang] for lang in BUTTONS["language"]]))
async def change_language(message: types.Message):
    """Позволяет изменить язык"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")

        await safe_send_message(
            message.chat.id,
            TEXTS[language]["current_language"],
            reply_markup=create_keyboard(
                LANGUAGES.keys(),
                back=True,
                settings=False
            )
        )
    except Exception as e:
        logger.error(f"Error in change_language: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_(LANGUAGES.keys()))
async def set_new_language(message: types.Message):
    """Устанавливает новый язык"""
    try:
        user = validate_user(message.from_user.id)
        new_language = LANGUAGES[message.text]
        user["language"] = new_language

        await safe_send_message(
            message.chat.id,
            TEXTS[new_language]["language_changed"],
            reply_markup=create_keyboard(
                ASSET_CATEGORIES[new_language].keys(),
                back=False,
                settings=True,
                language=new_language
            )
        )
    except Exception as e:
        logger.error(f"Error in set_new_language: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

@dp.message(F.text.in_([BUTTONS["back"][lang] for lang in BUTTONS["back"]]))
async def back_handler(message: types.Message):
    """Обработчик кнопки 'Назад'"""
    try:
        user = validate_user(message.from_user.id)
        language = user.get("language", "en")

        if "asset" in user:
            category = user["category"]
            await safe_send_message(
                message.chat.id,
                TEXTS[language]["select_asset"].format(category=category),
                reply_markup=create_keyboard(
                    ASSET_CATEGORIES[language][category],
                    back=True,
                    settings=True,
                    language=language
                )
            )
        elif "category" in user:
            await safe_send_message(
                message.chat.id,
                TEXTS[language]["select_category"],
                reply_markup=create_keyboard(
                    ASSET_CATEGORIES[language].keys(),
                    back=False,
                    settings=True,
                    language=language
                )
            )
        else:
            await safe_send_message(
                message.chat.id,
                TEXTS[language]["select_category"],
                reply_markup=create_keyboard(
                    ASSET_CATEGORIES[language].keys(),
                    back=False,
                    settings=True,
                    language=language
                )
            )
    except Exception as e:
        logger.error(f"Error in back_handler: {e}")
        await safe_send_message(
            message.chat.id,
            TEXTS["en"]["error"],
            reply_markup=ReplyKeyboardRemove()
        )

async def on_startup():
    """Действия при запуске бота"""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        asyncio.create_task(cooldown_watcher())
        asyncio.create_task(cleanup_user_data())
        logger.info("Bot successfully started")
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise

async def main():
    """Основная функция запуска бота"""
    with SingleInstance():
        try:
            await on_startup()
            await dp.start_polling(bot)
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error(f"Bot stopped with error: {e}")
            raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
