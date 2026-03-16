import asyncio
import json
import logging
import sys
import os
from datetime import date
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from .config import load_settings
from .db import (
    init_db,
    get_or_create_conversation,
    add_message,
    get_or_create_booking,
    get_booking,
    get_last_completed_booking,
    copy_booking_fields,
    update_booking_field,
    mark_booking_completed,
    reset_booking,
)
from .openai_client import (
    build_structured_request,
    generate_booking_confirmation,
    process_turn,
    BOOKING_FIELDS,
)


def booking_state_from_row(booking_row) -> dict:
    """Текущее состояние брони для LLM."""
    d = dict(booking_row) if booking_row else {}
    return {f: d.get(f) for f in BOOKING_FIELDS}


def all_booking_fields_filled(booking_row) -> bool:
    """Проверка, что собраны date, time, guests."""
    d = dict(booking_row) if booking_row else {}
    required = ["date_text", "time_text", "guests_count_text"]
    return all(d.get(f) for f in required)


async def handle_booking_complete(
    message: Message,
    db_path: str,
    conversation_id: int,
    booking_id: int,
    booking_row,
    api_key: str,
    base_url: Optional[str],
    model: str = os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
) -> None:
    """Формирует JSON, сохраняет, отправляет итог и JSON."""
    try:
        structured = build_structured_request(
            api_key=api_key,
            db_path=db_path,
            conversation_id=conversation_id,
            booking_row=dict(booking_row) if booking_row else {},
            base_url=base_url,
            model=model,
        )
        structured_json = json.dumps(structured, ensure_ascii=False, indent=2)
        mark_booking_completed(db_path, booking_id, structured_json)

        reply = generate_booking_confirmation(
            structured,
            api_key,
            base_url,
            model,
        )

        await message.answer(reply)
        add_message(db_path, conversation_id, "assistant", reply)

        try:
            await message.answer(structured_json)
            add_message(db_path, conversation_id, "assistant", structured_json)
        except Exception as json_err:
            log.exception("Failed to send JSON: %s", json_err)
    except Exception as exc:
        err = (
            "Не удалось сформировать запрос. Данные сохранены, "
            "менеджер свяжется с вами."
        )
        await message.answer(err)
        add_message(db_path, conversation_id, "assistant", err)


async def main() -> None:
    log.info("Starting bot...")
    settings = load_settings()
    init_db(settings.database_path)
    log.info("DB initialized")

    session = AiohttpSession(timeout=90.0)
    bot = Bot(token=settings.telegram_token, session=session)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        log.info("Received /start from chat_id=%s", message.chat.id)
        chat_id = message.chat.id
        conversation_id = get_or_create_conversation(
            settings.database_path, chat_id
        )
        add_message(
            settings.database_path, conversation_id, "user", "[Начало диалога]"
        )

        booking_id = get_or_create_booking(
            settings.database_path, conversation_id
        )
        booking_row = get_booking(settings.database_path, booking_id)
        state = booking_state_from_row(booking_row or {})
        history = []

        try:
            log.info("Calling process_turn for /start...")
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    process_turn,
                    "[Гость только что начал диалог. Поприветствуй и мягко начни сбор данных для брони: дата, время, гости, имя.]",
                    history,
                    state,
                    date.today().isoformat(),
                    settings.openai_api_key,
                    settings.openai_base_url or None,
                    settings.openai_model,
                ),
                timeout=60.0,
            )
            reply = (result.get("response") or "").strip() or "Здравствуйте! Чем могу помочь?"
            log.info("process_turn done, sending reply (len=%d)", len(reply))
        except asyncio.TimeoutError:
            log.warning("process_turn timed out after 60s")
            reply = (
                "Здравствуйте! Я помогу забронировать столик в «Звезды». "
                "На какую дату планируете визит?"
            )
        except Exception as e:
            log.exception("process_turn failed: %s", e)
            reply = (
                "Здравствуйте! Я помогу забронировать столик в «Звезды». "
                "На какую дату планируете визит?"
            )
        await message.answer(reply)
        add_message(settings.database_path, conversation_id, "assistant", reply)
        log.info("Replied to /start")

    @dp.message(Command("reset"))
    async def cmd_reset(message: Message) -> None:
        chat_id = message.chat.id
        conversation_id = get_or_create_conversation(
            settings.database_path, chat_id
        )
        reset_booking(settings.database_path, conversation_id)
        add_message(settings.database_path, conversation_id, "user", "/reset")

        booking_id = get_or_create_booking(
            settings.database_path, conversation_id
        )
        booking_row = get_booking(settings.database_path, booking_id)
        state = booking_state_from_row(booking_row or {})
        history = [{"role": "user", "content": "/reset"}]

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    process_turn,
                    "[Гость сбросил бронь. Подтверди сброс и предложи начать заново.]",
                    history,
                    state,
                    date.today().isoformat(),
                    settings.openai_api_key,
                    settings.openai_base_url or None,
                    settings.openai_model,
                ),
                timeout=60.0,
            )
            reply = result.get("response", "Бронь сброшена. Можем начать заново.")
        except asyncio.TimeoutError:
            log.warning("process_turn timed out (reset)")
            reply = "Бронь сброшена. На какую дату планируете визит?"
        except Exception as e:
            log.exception("cmd_reset failed: %s", e)
            reply = "Бронь сброшена. Можем начать заново."
        await message.answer(reply)
        add_message(settings.database_path, conversation_id, "assistant", reply)

    @dp.message(Command("last_booking"))
    async def cmd_last_booking(message: Message) -> None:
        """Запросить последнее бронирование (по user id = chat_id)."""
        chat_id = message.chat.id
        row = get_last_completed_booking(settings.database_path, chat_id)
        if not row:
            await message.answer("У вас пока нет завершённых бронирований.")
            return
        json_str = row.get("structured_request_json") if row else None
        if json_str:
            try:
                data = json.loads(json_str)
                lines = [
                    "Ваше последнее бронирование:",
                    f"Дата: {data.get('date', '—')}",
                    f"Время: {data.get('time', '—')}",
                    f"Гостей: {data.get('guests_count', '—')}",
                    f"Этаж: {data.get('floor', '—')}",
                ]
                if data.get("notes"):
                    lines.append(f"Пожелания: {data['notes']}")
                await message.answer("\n".join(lines))
            except json.JSONDecodeError:
                await message.answer("Бронирование найдено, но данные в неверном формате.")
        else:
            d = dict(row)
            await message.answer(
                f"Последнее бронирование: {d.get('date_text', '—')} "
                f"{d.get('time_text', '—')}, {d.get('guests_count_text', '—')} гостей, "
                f"этаж {d.get('floor_text', '—')}."
            )

    @dp.message(Command("change_booking"))
    async def cmd_change_booking(message: Message) -> None:
        """Изменить последнее бронирование: копируем его в новую черновик и продолжаем диалог."""
        chat_id = message.chat.id
        conversation_id = get_or_create_conversation(settings.database_path, chat_id)
        last = get_last_completed_booking(settings.database_path, chat_id)
        if not last:
            await message.answer("У вас нет завершённого бронирования для изменения.")
            return
        reset_booking(settings.database_path, conversation_id)
        add_message(settings.database_path, conversation_id, "user", "[Изменить последнее бронирование]")
        booking_id = get_or_create_booking(settings.database_path, conversation_id)
        copy_booking_fields(settings.database_path, booking_id, last)
        await message.answer(
            "Создана копия вашего последнего бронирования. Напишите, что хотите изменить "
            "(дата, время, число гостей, этаж или сертификат), или отправьте /reset чтобы начать с нуля."
        )
        add_message(
            settings.database_path, conversation_id, "assistant",
            "Создана копия последнего бронирования. Что хотите изменить?",
        )

    @dp.message(F.text)
    async def handle_text(message: Message) -> None:
        log.info("Received text from chat_id=%s: %r", message.chat.id, (message.text or "")[:80])
        chat_id = message.chat.id
        text = (message.text or "").strip()

        # Изменить последнее бронирование по фразе
        if text and text.lower() in ("изменить последнее бронирование", "изменить бронирование"):
            conversation_id = get_or_create_conversation(settings.database_path, chat_id)
            add_message(settings.database_path, conversation_id, "user", text)
            last = get_last_completed_booking(settings.database_path, chat_id)
            if not last:
                await message.answer("У вас нет завершённого бронирования для изменения.")
            else:
                reset_booking(settings.database_path, conversation_id)
                booking_id = get_or_create_booking(settings.database_path, conversation_id)
                copy_booking_fields(settings.database_path, booking_id, last)
                await message.answer(
                    "Создана копия вашего последнего бронирования. Напишите, что хотите изменить "
                    "(дата, время, число гостей, этаж или сертификат), или отправьте /reset чтобы начать с нуля."
                )
                add_message(
                    settings.database_path, conversation_id, "assistant",
                    "Создана копия последнего бронирования. Что хотите изменить?",
                )
            return

        if len(text) > 500:
            await message.answer(
                "Сообщение слишком длинное. Напишите покороче, пожалуйста."
            )
            return

        if not text:
            return

        conversation_id = get_or_create_conversation(
            settings.database_path, chat_id
        )
        add_message(settings.database_path, conversation_id, "user", text)

        booking_id = get_or_create_booking(
            settings.database_path, conversation_id
        )
        booking_row = get_booking(settings.database_path, booking_id)
        if booking_row is None:
            booking_row = {}

        state = booking_state_from_row(booking_row)
        from .db import get_recent_messages
        history = get_recent_messages(
            settings.database_path, conversation_id, limit=30
        )

        last_row = get_last_completed_booking(settings.database_path, chat_id)
        if not last_row:
            last_booking_summary = "нет завершённых бронирований"
        else:
            last_booking_summary = None
            json_str = last_row.get("structured_request_json")
            if json_str:
                try:
                    data = json.loads(json_str)
                    last_booking_summary = (
                        f"{data.get('date', '—')} в {data.get('time', '—')}, "
                        f"{data.get('guests_count', '—')} гостей, этаж {data.get('floor', '—')}"
                    )
                    if data.get("notes"):
                        last_booking_summary += f", пожелания: {data['notes']}"
                except json.JSONDecodeError:
                    d = dict(last_row)
                    last_booking_summary = (
                        f"{d.get('date_text', '—')} {d.get('time_text', '—')}, "
                        f"{d.get('guests_count_text', '—')} гостей, этаж {d.get('floor_text', '—')}"
                    )
            if last_booking_summary is None:
                d = dict(last_row)
                last_booking_summary = (
                    f"{d.get('date_text', '—')} {d.get('time_text', '—')}, "
                    f"{d.get('guests_count_text', '—')} гостей, этаж {d.get('floor_text', '—')}"
                )

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    process_turn,
                    text,
                    history,
                    state,
                    date.today().isoformat(),
                    settings.openai_api_key,
                    settings.openai_base_url or None,
                    settings.openai_model,
                    last_booking_summary,
                ),
                timeout=60.0,
            )
        except asyncio.TimeoutError:
            log.warning("process_turn timed out")
            result = {"response": "Извините, сервис временно недоступен. Попробуйте позже."}

        booking_updates = result.get("booking_updates") or []
        if result.get("booking_update") and not booking_updates:
            booking_updates = [result["booking_update"]]
        if booking_updates:
            log.info("booking_updates: %s", booking_updates)
        for upd in booking_updates:
            field = upd.get("field")
            value = upd.get("value")
            if field and value:
                update_booking_field(
                    settings.database_path, booking_id, field, str(value)
                )
        if booking_updates:
            booking_row = get_booking(settings.database_path, booking_id)
            state = booking_state_from_row(booking_row or {})

        booking_row = get_booking(settings.database_path, booking_id)
        if booking_row:
            d = dict(booking_row)
            log.info(
                "Booking state: date=%r time=%r guests=%r",
                d.get("date_text"),
                d.get("time_text"),
                d.get("guests_count_text"),
            )
        if booking_row and all_booking_fields_filled(booking_row):
            log.info("All required fields filled, calling handle_booking_complete")
            await handle_booking_complete(
                message,
                settings.database_path,
                conversation_id,
                booking_id,
                booking_row,
                settings.openai_api_key,
                settings.openai_base_url or None,
                settings.openai_model,
            )
        else:
            response = result.get("response", "Чем ещё могу помочь?")
            await message.answer(response)
            add_message(settings.database_path, conversation_id, "assistant", response)

    log.info("Starting polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
