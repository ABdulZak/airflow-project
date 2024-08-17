from datetime import datetime, timedelta
from telegram import Bot
from telegram.constants import ParseMode
from airflow.models import Variable
import asyncio

def format_text(text: str) -> str:
    markdown_format = {
        "*": "\*",
        "_": "\_",
        "`": "\`",
        ".": "\.",
        "(": "\(",
        ")": "\)",
        "=": "\=",
        "-": "\-",
        "~": "\~",
        "{": "\{",
        "}": "\}",
        "<": "\<",
        ">": "\>",
        "%": "\%",
    }
    for k, v in markdown_format.items():
        text = text.replace(k, v)
    return text

def _bot():
    token = Variable.get('AIRFLOW_TELEGRAM_TOKEN')
    chat_id = Variable.get('AIRFLOW_TELEGRAM_CHAT_ID')
    bot = Bot(token=token)
    return bot, chat_id

def send_on_failure(context):
    bot, chat_id = _bot()
    message = "*Error*  \n"
    message += "*DAG:*    " + format_text(context.get('dag').dag_id) + "\n"
    message += "*Task:*   " + format_text(context.get('task').task_id) + "\n"
    message += "*Time:*   " + format_text(datetime.now().strftime('%B %-d %Y, %H:%M:%S')) + "\n"
    message += "*Error:*  " + format_text(str(context.get('exception'))) + "\n"
    asyncio.run(bot.send_message(text=message, chat_id=chat_id, parse_mode=ParseMode.MARKDOWN_V2))

def send_on_success(context):
    bot, chat_id = _bot()
    message = "*Success*  \n"
    message += "*DAG:*    " + format_text(context.get('dag').dag_id) + "\n"
    message += "*Time:*   " + format_text(datetime.now().strftime('%B %-d %Y, %H:%M:%S')) + "\n"
    asyncio.run(bot.send_message(text=message, chat_id=chat_id, parse_mode=ParseMode.MARKDOWN_V2))


default_arg = {
    'owner': 'airflow',
    'on_failure_callback': send_on_failure,
    'max_active_runs': 1,
    'catchup': False,
}