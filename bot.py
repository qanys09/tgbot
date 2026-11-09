import asyncio
import json
import logging
import os
import re
from typing import Any

import asyncpg
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ChatAction
from aiogram.filters import Command
from dotenv import load_dotenv
from google import genai

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

bot: Bot | None = None
pool: asyncpg.pool.Pool | None = None

def ensure_env() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Add it to .env")
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not set. Add it to .env")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. Add it to .env")


async def init_db() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                orig_title VARCHAR(255),
                type VARCHAR(50),
                year INT,
                genres TEXT,
                actors TEXT,
                plot TEXT,
                tags TEXT,
                mood TEXT,
                url TEXT
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_query TEXT,
                bot_response TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

    await seed_catalog_if_empty()


async def seed_catalog_if_empty() -> None:
    if pool is None:
        raise RuntimeError("Database pool is not initialized")

    total = await pool.fetchval("SELECT COUNT(*) FROM catalog")
    if total and int(total) > 0:
        return

    seed_records = [
        {
            "title": "Прайм-тайм",
            "orig_title": "Primetime",
            "type": "Фильм",
            "year": 2026,
            "genres": "Биография, Драма, Криминал",
            "actors": "Роберт Паттинсон, Мерритт Уивер, Скайлер Гизондо, Мэттью Мэер, Фиби Бриджерс",
            "plot": "2006 год. Журналист и телеведущий Крис Хансен разоблачает педофилов, меняя облик телевидения.",
            "tags": "журналист, разоблачение, медиа, коррупция, власть, расследование, право, скандал",
            "mood": "напряжённый, острый, социально-реалистичный, драматичный, антигеройский",
            "url": "https://kinobase.org/film/327041-praym-taym",
        },
        {
            "title": "Сердце зверя",
            "orig_title": "Heart of the Beast",
            "type": "Фильм",
            "year": 2026,
            "genres": "Боевик, Драма, Приключения, Триллер",
            "actors": "Брэд Питт, Дж.К. Симмонс, Анна Лэмб, Джастин Мелник, Шон ДеЗерн",
            "plot": "После авиакатастрофы офицер спецназа Джеймс Белмонт и его боевая собака Один оказываются в глуши Аляски. Вместе они вынуждены вести жестокую борьбу за выживание против стихии.",
            "tags": "выживание, собака, аляска, дикая природа, спецназ, хоррор-экстрем, приключение, выжить любой ценой",
            "mood": "жесткий, суровый, атмосферный, напряжённый, арктический, безжалостный",
            "url": "https://kinobase.org/film/326660-serdtse-zverya",
        },
        {
            "title": "Король Талсы",
            "orig_title": "Tulsa King",
            "type": "Сериал",
            "year": 2022,
            "genres": "Драма, Комедия, Криминал",
            "actors": "Сильвестр Сталлоне, Андреа Сэвадж, Мартин Старр, Джей Уилл, Макс Казелла",
            "plot": "Осужденный нью-йоркский мафиози Дуайт Манфреди по прозвищу Генерал выходит на свободу и его отправляют в Талсу, где он строит новую криминальную империю.",
            "tags": "мафия, криминал, бандитский мир, бизнес, власть, новая жизнь, закон и порядок, американа",
            "mood": "сочный, уверенный, криминально-уютный, живой, брутальный, мудрый",
            "url": "https://kinobase.org/serial/203601-korol-talsy",
        },
        {
            "title": "Невский",
            "orig_title": "Nevskiy",
            "type": "Сериал",
            "year": 2018,
            "genres": "Детектив, Драма, Криминал",
            "actors": "Антон Васильев, Андрей Гульнев, Мария Капустинская, Сергей Кошонин, Дмитрий Паламарчук",
            "plot": "После роковой ошибки в перестрелке и борьбы с бандитским миром полицейский сталкивается с мощной сетью преступников и личными потерями в Санкт-Петербурге.",
            "tags": "детектив, город, полиция, криминал, расследование, петербург, бандиты, напряжение",
            "mood": "суровый, мрачный, городской, напряжённый, реальный, жесткий",
            "url": "https://kinobase.org/serial/239253-nevskiy",
        },
        {
            "title": "Купертино",
            "orig_title": "Cupertino",
            "type": "Сериал",
            "year": 2026,
            "genres": "Драма",
            "actors": "Рэйчел Келлер, Рене Голдсберри, Элла Оливия Стиллер, Майк Колтер, Бизи ФилиппС",
            "plot": "Адвокат, измученный корпоративной несправедливостью, вместе с напарницей вступает в борьбу с влиятельной элитой и сложными судебными процессами.",
            "tags": "суд, справедливость, корпорации, адвокат, бизнес, моральный выбор, манипуляции",
            "mood": "интеллектуальный, напряжённый, юридический, острый, социально-политический",
            "url": "https://kinobase.org/serial/326668-kupertino",
        },
        {
            "title": "Академия ведьм",
            "orig_title": "Coven Academy",
            "type": "Сериал",
            "year": 2026,
            "genres": "Детектив, Драма, Комедия, Мелодрама, Семейный, Триллер, Ужасы, Фэнтези",
            "actors": "Lucia Morris, Сэйлор Макферсон, Эверетт Андрес, Mia Parsonage, Harlequin Nott",
            "plot": "В мистическом Новом Орлеане Брайар поступает в закрытую школу для магически одаренных подростков, где ей предстоит учиться магии и бороться с темными силами.",
            "tags": "магия, школа, подростки, ведьмы, тайна, фэнтези, романтика, борьба со злом",
            "mood": "волшебный, легкий, мистический, подростковый, интригующий, эмоциональный",
            "url": "https://kinobase.org/serial/326599-akademiya-vedm",
        },
        {
            "title": "Горнолыжка",
            "orig_title": "Gornolyzhka",
            "type": "Сериал",
            "year": 2026,
            "genres": "Комедия",
            "actors": "Николай Наумов, Полина Максимова, Олег Савостюк, Давид Манукян, Сергей Федоров",
            "plot": "Миша, бывший крановщик, меняет жизнь, устроившись на элитный горнолыжный курорт, где сталкивается с любовным треугольником, конфликтами и трудовой дисциплиной.",
            "tags": "комедия, работа, романтика, курорт, семья, отношения, форс-мажоры, бытовая драма",
            "mood": "добрый, смешной, уютный, живой, неформальный, легкий",
            "url": "https://kinobase.org/serial/327049-gornolyzhka",
        },
        {
            "title": "Щенячий патруль: Динофильм",
            "orig_title": "PAW Patrol: The Dino Movie",
            "type": "Мультфильм",
            "year": 2026,
            "genres": "Боевик, Комедия, Криминал, Мультфильм, Приключения, Семейный, Фантастика",
            "actors": "Шон Мерфи, Ким Коллинз, Брайан Кранстон, Ален Стивенсон, Питер Краус",
            "plot": "Команда героев-щенков отправляется в путешествие в мир динозавров и сталкивается с неожиданными опасностями и новым приключением.",
            "tags": "дети, мультфильм, динозавры, приключения, дружба, семья, фантастика, юмор",
            "mood": "весёлый, яркий, семейный, динамичный, фантастический, позитивный",
            "url": "https://kinobase.org/film/326262-schenyachiy-patrul-dinofilm",
        },
        {
            "title": "Симпсоны",
            "orig_title": "The Simpsons",
            "type": "Мультсериал",
            "year": 1989,
            "genres": "Комедия, Семейный, Сатира",
            "actors": "Дэн Кастелланета, Джулия Кавнер, Нэнси Картрайт, Трейси Ульман, Хайме Кайм",
            "plot": "Семья Симпсонов живёт в Спрингфилде и сталкивается с повседневными бытовыми случайностями, шутками и семейными конфликтами.",
            "tags": "сатира, семья, шутки, город, повседневность, комедия, абсурд, баддиум",
            "mood": "ироничный, лёгкий, смешной, домашний, знакомый, остроумный",
            "url": "https://kinobase.org/serial/244654-simpsony",
        },
    ]

    async with pool.acquire() as conn:
        for item in seed_records:
            await conn.execute(
                """
                INSERT INTO catalog (
                    title, orig_title, type, year, genres, actors, plot, tags, mood, url
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                item["title"],
                item["orig_title"],
                item["type"],
                item["year"],
                item["genres"],
                item["actors"],
                item["plot"],
                item["tags"],
                item["mood"],
                item["url"],
            )


async def fetch_catalog() -> list[dict[str, Any]]:
    if pool is None:
        return []
    rows = await pool.fetch(
        """
        SELECT id, title, orig_title, type, year, genres, actors, plot, tags, mood, url
        FROM catalog
        ORDER BY title ASC
        """
    )
    return [dict(row) for row in rows]


async def fetch_recent_user_history(user_id: int, limit: int = 3) -> list[str]:
    if pool is None:
        return []
    rows = await pool.fetch(
        "SELECT user_query FROM history WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2",
        user_id,
        limit,
    )
    return [str(r["user_query"]) for r in rows]


def extract_json_from_text(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.IGNORECASE)

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def detect_preferred_type(user_query: str) -> str | None:
    normalized_query = user_query.lower()
    if any(word in normalized_query for word in ["мультфильм", "мульт", "анимация", "cartoon", "мультик", "детский мульт"]):
        return "мульт"
    if any(word in normalized_query for word in ["сериал", "сериалы", "шоу", "series"]):
        return "сериал"
    if any(word in normalized_query for word in ["фильм", "кино", "movie"]):
        return "фильм"
    return None


def filter_catalog_by_type(catalog: list[dict[str, Any]], preferred_type: str | None) -> list[dict[str, Any]]:
    if preferred_type is None:
        return catalog

    if preferred_type == "мульт":
        filtered = [
            item for item in catalog
            if "мульт" in str(item.get("type", "")).lower()
        ]
        return filtered if filtered else catalog

    if preferred_type == "сериал":
        filtered = [
            item for item in catalog
            if "сериал" in str(item.get("type", "")).lower() or "мультсериал" in str(item.get("type", "")).lower()
        ]
        return filtered if filtered else catalog

    if preferred_type == "фильм":
        filtered = [
            item for item in catalog
            if "фильм" in str(item.get("type", "")).lower() and "мульт" not in str(item.get("type", "")).lower()
        ]
        return filtered if filtered else catalog

    return catalog


async def generate_recommendations(user_id: int, user_query: str) -> str:
    catalog = await fetch_catalog()
    if not catalog:
        return "В каталоге пока нет доступных тайтлов. Попробуйте позже."

    preferred_type = detect_preferred_type(user_query)
    catalog_for_prompt = filter_catalog_by_type(catalog, preferred_type)

    history = await fetch_recent_user_history(user_id, limit=3)
    catalog_context = []
    for item in catalog_for_prompt:
        catalog_context.append(
            "- "
            f"title={item['title']}; orig_title={item['orig_title']}; type={item['type']}; year={item['year']}; "
            f"genres={item['genres']}; actors={item['actors']}; plot={item['plot']}; tags={item['tags']}; "
            f"mood={item['mood']}; url={item['url']}"
        )

    history_context = "\n".join(f"{index + 1}. {entry}" for index, entry in enumerate(history)) if history else "Нет предыдущих запросов."

    type_priority = ""
    if preferred_type == "мульт":
        type_priority = "МАКСИМАЛЬНОЕ ПРАВИЛО: в запросе про мультфильм/мультсериал использовать только записи с типом Мультфильм или Мультсериал. Фильмы и сериалы запрещены, даже если тема похожа."
    elif preferred_type == "сериал":
        type_priority = "Если пользователь просит сериал, приоритет у записей с типом Сериал или Мультсериал; обычные фильмы выбирать только если подходящих сериалов нет."
    elif preferred_type == "фильм":
        type_priority = "Если пользователь просит фильм, приоритет у записей с типом Фильм; сериалы выбирать только если подходящего фильма нет."

    prompt = f"""
Ты — эксперт по рекомендациям фильмов, сериалов и мультфильмов.

Задача: выбрать 1–2 наиболее подходящих фильма/сериала/мультфильма ТОЛЬКО из приведённого каталога.
Если подходящего варианта нет, выбери ближайший по настроению, атмосфере и смыслу.
Не придумывай новые названия и не выходи за рамки каталога.

Важное правило: {type_priority}

Требования к результату:
1) Верни ТОЛЬКО валидный JSON без комментариев, без Markdown, без блоков кода.
2) Структура JSON:
{{
  "recommendations": [
    {{
      "title": "Название из каталога",
      "year": 2026,
      "type": "Фильм/Сериал/Мультфильм/Мультсериал",
      "why": "Краткое и чёткое объяснение, почему именно это подходит под запрос пользователя",
      "plot": "Сюжет в двух словах без спойлеров",
      "url": "Ссылка на просмотр из каталога"
    }}
  ]
}}
3) Максимум 2 рекомендации.
4) Не используй слишком длинные тексты.
5) Если запрос туманен — выбери наиболее близкий по настроению из каталога.
6) В ответе обязательно используй только те ссылки, что есть в каталоге.

Каталог:
{chr(10).join(catalog_context) if catalog_context else 'В каталоге нет подходящих записей по типу запроса.'}

История последних 3 запросов пользователя:
{history_context}

Запрос пользователя: {user_query}
"""

    models_to_try = ["gemini-1.5-flash", "gemini-1.5-flash-latest", "gemini-2.0-flash"]
    last_error = None

    for model_name in models_to_try:
        try:
            gemini_client = genai.Client(api_key=GEMINI_API_KEY)
            response = gemini_client.models.generate_content(
                model=model_name,
                contents=prompt,
                config={
                    "temperature": 0.6,
                    "max_output_tokens": 600,
                },
            )

            raw_text = getattr(response, "text", None) or str(response)
            payload = extract_json_from_text(raw_text)
            if not payload or "recommendations" not in payload or not isinstance(payload["recommendations"], list):
                raise ValueError("Gemini returned invalid JSON")

            recommendations = payload["recommendations"][:2]
            formatted = []
            for item in recommendations:
                title = str(item.get("title") or "Неизвестно")
                year = item.get("year") or "—"
                kind = item.get("type") or "—"
                why = item.get("why") or "Подходит по настроению и тематике."
                plot = item.get("plot") or "Напряжённый и запоминающийся сюжет."
                url = item.get("url") or "https://kinobase.org"

                formatted.append(
                    f"🎬 {title} ({year}, {kind})\n"
                    f"💡 Почему подходит под запрос: {why}\n"
                    f"📖 Сюжет в двух словах: {plot}\n"
                    f"🔗 Ссылка на просмотр: {url}"
                )

            return "\n\n".join(formatted)
        except Exception as exc:
            last_error = exc
            logger.warning("Gemini model %s failed: %s", model_name, exc)

    logger.exception("All Gemini models failed: %s", last_error)
    return fallback_recommendation(catalog, user_query)


def fallback_recommendation(catalog: list[dict[str, Any]], user_query: str) -> str:
    text = user_query.lower()
    preferred_type = detect_preferred_type(text)
    catalog = filter_catalog_by_type(catalog, preferred_type)
    scored: list[tuple[float, dict[str, Any]]] = []

    for item in catalog:
        score = 0.0
        item_type = str(item['type']).lower()
        haystack = f"{item['title']} {item['orig_title']} {item['type']} {item['genres']} {item['actors']} {item['plot']} {item['tags']} {item['mood']}".lower()

        if preferred_type == "мульт":
            if "мульт" in item_type:
                score += 12.0
            else:
                score -= 20.0
        elif preferred_type == "сериал":
            if "сериал" in item_type:
                score += 9.0
            elif "мультсериал" in item_type:
                score += 8.5
            else:
                score -= 5.0
        elif preferred_type == "фильм":
            if "фильм" in item_type and "мульт" not in item_type:
                score += 9.0
            elif "сериал" in item_type:
                score -= 4.0

        for keyword in [
            "суровый", "выживание", "лес", "мафия", "криминал", "комедия", "работяга", "драма",
            "семья", "магия", "дети", "приключения", "романтика", "суд", "полиция", "детектив",
            "ужасы", "фэнтези", "удар", "дружба", "сатира"
        ]:
            if keyword in text and keyword in haystack:
                score += 2.0

        if score > 0:
            scored.append((score, item))

    if not scored:
        best = catalog[0]
        return (
            f"🎬 {best['title']} ({best['year']}, {best['type']})\n"
            f"💡 Почему подходит под запрос: Это самый близкий по атмосфере и настроению вариант в каталоге.\n"
            f"📖 Сюжет в двух словах: {best['plot']}\n"
            f"🔗 Ссылка на просмотр: {best['url']}"
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    best = scored[0][1]
    return (
        f"🎬 {best['title']} ({best['year']}, {best['type']})\n"
        f"💡 Почему подходит под запрос: По настроению, тематике и типу контента это наиболее близкое совпадение.\n"
        f"📖 Сюжет в двух словах: {best['plot']}\n"
        f"🔗 Ссылка на просмотр: {best['url']}"
    )


async def save_history(user_id: int, user_query: str, bot_response: str) -> None:
    if pool is None:
        return
    await pool.execute(
        "INSERT INTO history (user_id, user_query, bot_response) VALUES ($1, $2, $3)",
        user_id,
        user_query,
        bot_response,
    )


async def show_history(user_id: int) -> str:
    if pool is None:
        return "История недоступна."

    rows = await pool.fetch(
        "SELECT user_query, created_at FROM history WHERE user_id = $1 ORDER BY created_at DESC LIMIT 3",
        user_id,
    )
    if not rows:
        return "📭 История запросов пуста."

    lines = ["🕘 Последние 3 запроса:"]
    for index, row in enumerate(rows, start=1):
        lines.append(f"{index}. {row['user_query']}")
    return "\n".join(lines)


async def handle_start(message: types.Message) -> None:
    text = (
        "Привет! Я помогу найти фильм, сериал или мультфильм под твоё настроение.\n\n"
        "Просто напиши мне любую просьбу в свободной форме, например:\n"
        "• хочу суровый фильм про выживание в лесу\n"
        "• посоветуй комедию про работягу\n"
        "• что-то про мафию\n\n"
        "Также доступна команда /history, чтобы посмотреть последние запросы."
    )
    await message.answer(text)


async def handle_history(message: types.Message) -> None:
    history_text = await show_history(message.from_user.id)
    await message.answer(history_text)


async def handle_recommendation(message: types.Message) -> None:
    if not message.text:
        return

    query = message.text.strip()
    if not query:
        return

    await bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)

    recommendation = await generate_recommendations(message.from_user.id, query)
    await save_history(message.from_user.id, query, recommendation)
    await message.answer(recommendation)


async def main() -> None:
    ensure_env()
    try:
        await init_db()

        global bot
        bot = Bot(token=BOT_TOKEN, parse_mode=None)
        dp = Dispatcher()

        dp.message.register(handle_start, Command("start"))
        dp.message.register(handle_history, Command("history"))
        dp.message.register(handle_recommendation, F.text)

        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bot shutdown requested by user.")
    finally:
        if pool is not None:
            await pool.close()
            logger.info("Database pool closed.")
        if bot is not None:
            await bot.session.close()
            logger.info("Telegram bot session closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped.")
