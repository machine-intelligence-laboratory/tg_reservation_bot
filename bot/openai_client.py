import json
import re
from typing import Dict, Any, List, Optional

from openai import OpenAI

from .db import get_recent_messages

# Поля брони (date, time, guests — обязательно; остальные опционально)
BOOKING_FIELDS = [
    "date_text",
    "time_text",
    "guests_count_text",
    "floor_text",
    "certificate_needed_text",
]
# Поля, которые спрашиваем по очереди (без имени и телефона)
REQUESTED_FIELDS = ["date_text", "time_text", "guests_count_text", "floor_text", "certificate_needed_text"]

RESTAURANT_DATA = {
    "ADDRESS": """restaurant_name: Звезды
street: Курортный проспект, 18
district: —
city: Зеленоградск""",
    "DIRECTIONS": """nearest_metro: —
landmarks: МФЦ Зеленоградска
parking: Собственной нет. Ближайшая парковка возле МФЦ Зеленоградска. В выходные часто занята. Ориентируйтесь по геосервисам.""",
    "OPENING_HOURS": "Уточняйте актуальное расписание.",
    "SEATING_INFO": """terrace_available: нет
window_tables: зависит от загрузки, можно указать при брони
romantic_tables: —
large_tables: есть, для компании >6 лучше предупредить заранее
quiet_area: —""",
    "EVENTS": """live_music: —
special_events: —
holiday_dinners: банкеты — менеджер свяжется с предложением""",
    "MENU_DATA": """
1. В чем разница двух этажей?
- на первом этаже располагаются два концепта: Азиатское бистро «Нами», итальянский ресторан «Чело».
- на втором этаже находится основной зал ресторана «Звезды».

2. Можно ли спустить еду со второго этажа на первый? И наоборот?
Нет. На первом этаже работает меню азиатского бистро «Нами» и итальянского ресторана «Чело».

3. Есть ли у вас детское меню?
Детского меню у нас нет, но официант предложит блюда, которые уже оценили многие маленькие гости.

4. Есть ли парковка?
Собственной нет, ближайшая парковка возле МФЦ Зеленоградска, но в выходные она часто занята. Ориентируйтесь по геосервисам.

5. Можно ли с собаками?
Да. Мы рады гостям и их воспитанным друзьям.

6. Есть ли подарочные сертификаты?
Да, вы можете приобрести их на сайте wegosty: https://wegosty.site

7. Есть ли детский стульчик?
Да, есть.

8. Можно заказать у вас банкет?
Да. Наш менеджер свяжется с гостем с предложением по предзаказу.

9. Можно столик с окошком или диваном?
Мы не бронируем конкретные столы, их распределяет система автоматически.
Если во время визита будет возможность организовать место у окна или стол с диваном — мы обязательно это сделаем.

10. Почему действует ограничение по времени пребывания (2 часа)?
Чтобы мы могли планировать бронирование столиков и гости могли быть уверены в своей брони.

11. Видно ли море из ресторана?
Нет, до моря нужно немного прогуляться, но с балкона второго этажа отличный вид на Курортный проспект.

---
МЕНЮ (используй для ответов о блюдах, ценах и рекомендациях)

Паста (CIELO)
Мафальдини со страчателлой и соусом из рукколы — 795 ₽. Паста из твёрдых сортов пшеницы собственного приготовления, сыр из местной сыроварни, соус из рукколы.
Спагетти с моллюсками вонголе — 895 ₽
Канкильони с морепродуктами на двоих — 1890 ₽
Ритате с лангустинами с соусом биск — 1100 ₽
Кампанелле с лисичками и соусом из лесных грибов — 720 ₽
Мафольдини al tartufo — 795 ₽ (паста с трюфелем)
Спагетти с копченым окороком и пармезаном — 850 ₽
Ритате 4 сыра — 795 ₽
Канкильони с мясным рагу и печёной паприкой (на двоих) — 1490 ₽
Лазанья с рагу по-Болонски — 850 ₽
Равиоли с лангустами и соусом аква пацца — 680 ₽

Антипасти (CIELO)
Домашняя фоккача — 350 ₽ (с оливковым маслом и бальзамиком или со взбитым маслом с анчоусами)
Рийет из лосося с маринованными огурцами — 550 ₽
Пате из печени цесарки с вареньем из инжира — 680 ₽
Тартар из говядины с кремом parmigiano — 695 ₽
Вителло тоннато — 850 ₽
Карпаччо из говядины с кремом Pecorino Romano — 770 ₽
Большой салат цезарь на двоих — 800 ₽
Буррата с печеными черри и трюфельным бальзамиком — 1200 ₽
Хумус Italiano со страчателлой — 595 ₽

Сицилийская пицца (тесто 48 ч ферментации, дровяная печь)
Маргарита — 590 ₽
Пепперони — 920 ₽
Мортаделла с фисташкой — 960 ₽
Прошутто Фунги — 880 ₽
5 сыров — 1350 ₽
Пицца с анчоусом и страчетеллой — 1350 ₽
Пицца с бурратой и чёрным трюфелем — 1350 ₽

Лапша (Nami)
Лапша со свининой Чар Су — 595 ₽
Лапша с мраморной говядиной — 650 ₽
Лапша с креветками Лакса — 659 ₽

Ризотто (CIELO)
Ризотто неро с лангустинами — 995 ₽
Ризотто Porcini с белыми грибами и трюфелем — 895 ₽
Ризотто al miso с жареным кальмаром — 720 ₽

Роллы (NAMI)
Филадельфия — 1190 ₽
Ролл с угрём — 1190 ₽
Кранч ролл XL size — 1395 ₽

Гедза / Гёдза (Nami) — японские пельмени
Азиатские грибы — 495 ₽
Мраморная говядина — 495 ₽
Гёдза с цыплёнком и чили чесноком — 495 ₽

Горячие овощи (NAMI)
Картофель Фри — 395 ₽

Супы (Nami)
Пайтан рамен со свининой Чар су — 620 ₽

Сладкое
Шоколадный торт с цветочной солью — 550 ₽
Тирамису с орехами пекан — 675 ₽
""".strip(),
}

# Единый компактный промпт (без загрузки skills — быстрее)
HOST_SYSTEM_PROMPT = """Ты — дружелюбный хост ресторана «Звезды». Стиль: короткие ответы, естественно.

Цель: помочь забронировать. Для брони нужны только: дата, время, число гостей. Имя и телефон не спрашивай.
Спрашивай параметры по одному: дата → время → гости → этаж (1/2) → подарочный сертификат. Никогда не проси несколько полей сразу.

Если гость спрашивает о ресторане — отвечай по данным ниже. Не говори «не знаю» — предложи указать в комментарии к брони.

Если вопрос касается бронирования конкретного столика - отвечай, что такой опции нет, но можно занять понравившийся если он будет доступен.
"""

# Словарь для «восемь вечера», «девять утра» и т.п.
_WORD_TO_HOUR: Dict[str, int] = {
    "один": 1, "два": 2, "три": 3, "четыре": 4, "пять": 5, "шесть": 6,
    "семь": 7, "восемь": 8, "девять": 9, "десять": 10, "одиннадцать": 11, "двенадцать": 12,
}


def _parse_time_fallback(text: str) -> Optional[str]:
    """Извлекает время из текста (15:30, 4 часа, 5 вечера, восемь вечера, 17)."""
    if not text or not text.strip():
        return None
    t = text.strip().lower()
    # HH:MM или H:MM
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", t)
    if m:
        h, mi = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mi <= 59:
            return f"{h:02d}:{mi:02d}"
    # N час(ов), N часов, в N
    m = re.search(r"(?:в\s+)?(\d{1,2})\s*(?:час(?:а|ов)?|ч\.?)", t)
    if m:
        h = int(m.group(1))
        if 0 <= h <= 23:
            return f"{h:02d}:00"
    # 5 вечера, 6 утра, восемь вечера
    m = re.search(r"(\d{1,2})\s*(?:часов?|ч\.?)\s*(?:вечера|вечером|пополудни)", t)
    if m:
        h = int(m.group(1))
        if 1 <= h <= 11:
            h += 12
        if 12 <= h <= 23:
            return f"{h:02d}:00"
    for word, h in _WORD_TO_HOUR.items():
        if word in t and any(x in t for x in ("вечера", "вечером", "пополудни")):
            if 1 <= h <= 11:
                return f"{h + 12:02d}:00"
            if h == 12:
                return "12:00"
        if word in t and any(x in t for x in ("утра", "утром")):
            return f"{h:02d}:00"
    m = re.search(r"(\d{1,2})\s*(?:часов?|ч\.?)\s*(?:утра|утром)", t)
    if m:
        h = int(m.group(1))
        if 1 <= h <= 12:
            return f"{h:02d}:00"
    # Только число 17 или 17 00
    m = re.search(r"\b(\d{1,2})\b", t)
    if m and len(t) < 15:
        h = int(m.group(1))
        if 0 <= h <= 23:
            return f"{h:02d}:00"
    return None


RESTAURANT_INFO = (
    "\n\nДанные ресторана:\n"
    + RESTAURANT_DATA.get("ADDRESS", "")
    + "\nПарковка: "
    + RESTAURANT_DATA.get("DIRECTIONS", "")
    + "\n\nFAQ + меню:\n"
    + RESTAURANT_DATA.get("MENU_DATA", "")
)

PROCESS_TURN_FORMAT = """
## OUTPUT FORMAT

Ответь СТРОГО в формате JSON (без markdown, без комментариев):
{
  "intent": "booking_param" | "faq" | "off_topic",
  "booking_update": {"field": "имя_поля", "value": "значение"} или [{"field":"...","value":"..."}, ...] или null,
  "response": "твой ответ гостю (всегда заполни)"
}

Правила:
- intent "booking_param": гость указывает параметр брони. ИЗВЛЕКИ ВСЕ УПОМЯНУТЫЕ параметры. Поля: date_text (YYYY-MM-DD), time_text (HH:MM), guests_count_text (число), floor_text (1/2), certificate_needed_text (true/false). Имя и телефон не извлекай. booking_update может быть объектом или массивом.
- intent "faq": вопрос о ресторане — ответь по имеющейся информации, мягко верни к брони.
- intent "off_topic": сообщение не о ресторане и не о брони — вежливо отклонь и предложи помощь с бронированием.
- response: всегда заполняй, пиши от лица оператора, коротко и дружелюбно. Спрашивай всегда только ОДИН следующий параметр (см. «Сейчас запрашиваем»). Не пиши «почти готово».
"""


def process_turn(
    user_message: str,
    history: List[Dict[str, str]],
    booking_state: Dict[str, Optional[str]],
    today_iso: str,
    api_key: str,
    base_url: Optional[str] = None,
    model: str = "gpt-4.1",
) -> Dict[str, Any]:
    """
    Единый LLM-вызов: определение намерения, обновление брони, генерация ответа.
    Возвращает {intent, booking_updates: [...], response}.
    """
    client_kwargs: Dict[str, Any] = {"api_key": api_key, "timeout": 45.0}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)

    state_str = json.dumps(
        {k: v for k, v in booking_state.items() if v is not None},
        ensure_ascii=False,
    )
    next_field = next((f for f in REQUESTED_FIELDS if not (booking_state.get(f) or "").strip()), None)
    next_hint = f" Сейчас запрашиваем: {next_field}." if next_field else ""
    context = (
        f"Сегодня: {today_iso}. Текущие данные брони: {state_str or 'пусто'}.\n"
        f"Ожидаемые поля: {', '.join(BOOKING_FIELDS)}.{next_hint}"
    )

    system_content = HOST_SYSTEM_PROMPT + RESTAURANT_INFO + "\n\n" + PROCESS_TURN_FORMAT
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": context},
    ]
    for msg in history[-30:]:
        if msg.get("role") in ("user", "assistant") and msg.get("content"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
    )
    content = (completion.choices[0].message.content or "").strip()

    try:
        # Убираем markdown-обёртку если есть
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        parsed = json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end > start:
            try:
                parsed = json.loads(content[start: end + 1])
            except json.JSONDecodeError:
                parsed = {"intent": "faq", "booking_update": None, "response": content}
        else:
            parsed = {"intent": "faq", "booking_update": None, "response": content}

    intent = parsed.get("intent", "faq")
    raw_updates = parsed.get("booking_update")
    response = parsed.get("response", "").strip() or "Чем ещё могу помочь?"

    booking_updates: List[Dict[str, str]] = []
    if raw_updates is not None:
        items = raw_updates if isinstance(raw_updates, list) else [raw_updates]
        for u in items:
            if not isinstance(u, dict):
                continue
            field = u.get("field")
            value = u.get("value")
            if value is not None and not isinstance(value, str):
                value = str(value)
            if field in BOOKING_FIELDS and value and str(value).strip():
                booking_updates.append({"field": field, "value": str(value).strip()})

    # Фолбек: если ждём время и LLM его не вернул — попробуем распарсить вручную
    next_field = next((f for f in BOOKING_FIELDS if not (booking_state.get(f) or "").strip()), None)
    if next_field == "time_text" and not any(u.get("field") == "time_text" for u in booking_updates):
        parsed_time = _parse_time_fallback(user_message)
        if parsed_time:
            booking_updates.append({"field": "time_text", "value": parsed_time})

    return {
        "intent": intent,
        "booking_updates": booking_updates,
        "response": response,
    }


def build_structured_request(
    api_key: str,
    db_path: str,
    conversation_id: int,
    booking_row: Any,
    base_url: Optional[str] = None,
    model: str = "gpt-4.1",
) -> Dict[str, Any]:
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url

    client = OpenAI(**client_kwargs)

    history = get_recent_messages(db_path, conversation_id, limit=40)

    system_prompt = (
        "Ты помощник менеджера ресторана «Звезды» в Зеленоградске. "
        "На основе истории диалога и уже сохраненных ответов гостя "
        "сформируй СТРУКТУРИРОВАННЫЙ JSON-запрос для системы бронирования. "
        "Всегда отвечай только валидным JSON, без комментариев и пояснений.\n\n"
        "Текущий год: 2026. Если гость не указывает год явно, "
        "подставляй в дату бронирования именно 2026 год.\n\n"
        "Структура объекта:\n"
        "{\n"
        '  \"date\": \"строка, дата визита с годом (например, 2026-05-25 или в ином понятном менеджеру виде)\",\n'
        '  \"time\": \"строка, время визита\",\n'
        '  \"guests_count\": число гостей (int),\n'
        '  \"floor\": \"1\" или \"2\" или другая формулировка,\n'
        '  \"certificate_needed\": true/false,\n'
        '  \"notes\": \"опциональные комментарии\"\n'
        "}\n\n"
        "Используй нижеописанную информацию о ресторане как контекст.\n\n"
        f"Ресторан: {RESTAURANT_DATA.get('ADDRESS', '')}\n"
        f"Парковка: {RESTAURANT_DATA.get('DIRECTIONS', '')}\n"
        f"Меню и FAQ: {RESTAURANT_DATA.get('MENU_DATA', '')}"
    )

    row = dict(booking_row) if booking_row else {}
    user_summary = {
        "already_collected": {
            "date_text": row.get("date_text"),
            "time_text": row.get("time_text"),
            "guests_count_text": row.get("guests_count_text"),
            "name_text": row.get("name_text"),
            "phone_text": row.get("phone_text"),
            "floor_text": row.get("floor_text"),
            "certificate_needed_text": row.get("certificate_needed_text"),
        }
    }

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": "Ниже история диалога с гостем и собранные ответы. "
            "Пожалуйста, сформируй JSON-запрос для системы бронирования.",
        },
    ]

    for msg in history:
        if msg["role"] not in {"user", "assistant"}:
            continue
        messages.append(
            {"role": msg["role"], "content": msg["content"]},
        )

    messages.append(
        {
            "role": "user",
            "content": "Сводка уже собранных ответов гостя (JSON): "
            + json.dumps(user_summary, ensure_ascii=False),
        }
    )

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.0,
    )

    content = completion.choices[0].message.content or ""

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        # На всякий случай ещё одна попытка: найти JSON в тексте
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(content[start: end + 1])
            except json.JSONDecodeError:
                raise
        else:
            raise

    return parsed


FAQ_SYSTEM_PROMPT = (
    "Ты бот ресторана «Звезды» в Зеленоградске. "
    "У тебя есть только информация из FAQ ниже.\n\n"
    "Задача: определить, задаёт ли гость вопрос о ресторане, "
    "на который можно ответить по FAQ.\n"
    "- Если ДА: ответь строго на основе FAQ. "
    "Не придумывай факты.\n"
    "- Если НЕТ (вопрос не о ресторане или это ответ по бронированию — "
    "дата, время, гости): не отвечай по FAQ.\n\n"
    "Формат ответа — только валидный JSON:\n"
    "- если отвечаешь по FAQ: {\"answer_faq\": true, \"text\": \"...\"}\n"
    "- если не отвечаешь по FAQ: {\"answer_faq\": false}\n\n"
    "FAQ ресторана:\n\n"
)

FAQ_USER_PROMPT = "Сообщение гостя: {message}"


def answer_faq_or_reject(
    user_message: str,
    api_key: str,
    base_url: Optional[str] = None,
) -> Optional[str]:
    """
    Релевантен ли вопрос FAQ: при да — вернуть ответ по FAQ, иначе None.
    """
    if not user_message or not user_message.strip():
        return None

    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)

    system_content = FAQ_SYSTEM_PROMPT + RESTAURANT_INFO
    user_content = FAQ_USER_PROMPT.format(message=user_message.strip())

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        temperature=0.0,
    )

    content = (completion.choices[0].message.content or "").strip()

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(content[start: end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None

    if not isinstance(parsed, dict):
        return None
    ok = parsed.get("answer_faq") is True and isinstance(parsed.get("text"), str)
    if ok:
        return parsed["text"].strip() or None
    return None


def _parse_via_llm(
    user_text: str,
    task: str,
    today_iso: str,
    api_key: str,
    base_url: Optional[str] = None,
) -> Optional[str]:
    """Универсальный парсинг даты/времени через LLM."""
    if not user_text or not user_text.strip():
        return None
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)
    prompt = (
        f"Сегодня: {today_iso}. Задача: {task}\n"
        f"Текст от гостя: «{user_text.strip()}»\n"
        "Ответь ТОЛЬКО одной строкой — нормализованное значение, без пояснений. "
        "Если не удаётся понять — верни пустую строку."
    )
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        content = (completion.choices[0].message.content or "").strip()
        return content if content else None
    except Exception:
        return None


def parse_date_with_llm(
    user_text: str,
    today_iso: str,
    api_key: str,
    base_url: Optional[str] = None,
) -> Optional[str]:
    """
    Преобразует ввод гостя (завтра, 25.05, пятница) в дату с годом.
    today_iso — сегодня в формате YYYY-MM-DD.
    """
    task = (
        "Нормализуй дату визита в ресторан. Верни дату в формате YYYY-MM-DD "
        "(например 2026-05-25). Учитывай: «завтра», «послезавтра», «25.05», "
        "«15 июня», дни недели и т.п. Если год не указан — подставь текущий."
    )
    return _parse_via_llm(user_text, task, today_iso, api_key, base_url)


def parse_time_with_llm(
    user_text: str,
    today_iso: str,
    api_key: str,
    base_url: Optional[str] = None,
) -> Optional[str]:
    """
    Преобразует ввод гостя (19:00, в 7 вечера, обед) в нормализованное время.
    """
    task = (
        "Нормализуй время визита. Верни время в формате HH:MM (24 часа). "
        "Примеры: «19:00», «в 7 вечера» → 19:00, «обед» → 13:00."
    )
    return _parse_via_llm(user_text, task, today_iso, api_key, base_url)
