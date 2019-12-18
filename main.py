import re
import os
import datetime
import pickle
import json
import subprocess

from functools import lru_cache

import tabula
import requests
import pandas as pd

from bs4 import BeautifulSoup as bs

from flask import Flask, request, abort, jsonify

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)

import cek

app = Flask(__name__)

line_bot_api = LineBotApi(os.environ["LineBotAccessToken"])
handler = WebhookHandler(os.environ["LineBotHandler"])

clova = cek.Clova(
    application_id="com.dorm-menu.nagata",
    default_language="ja",
    debug_mode=True)

URL_HEAD = "https://dorm-menu.herokuapp.com/"

@app.route("/health")
def health():
    return "ok"


# /clova に対してのPOSTリクエストを受け付けるサーバーを立てる
@app.route('/clova', methods=['POST'])
def my_service():
    body_dict = clova.route(body=request.data, header=request.headers)
    response = jsonify(body_dict)
    response.headers['Content-Type'] = 'application/json;charset-UTF-8'
    return response


@clova.handle.launch
def launch_request_handler(clova_request):
    welcome_japanese = cek.Message(message="寮飯を教えてしんぜよう", language="ja")
    response = clova.response([welcome_japanese])
    return response


@clova.handle.intent("Both_Info")
def wife_status_handler(clova_request):
    slots = clova_request.slots
    date = slots.get("datetime")
    lunch = slots.get("Lunch")
    if not date:
        date = datetime.date.today()
    else:
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
    date_tuple = (date.month, date.day)
    if not lunch:
        lunch = "ごはん"
    try:
        data = flow(*date_tuple)
        date_str = f"{date.month}月{date.day}日"
        space = " "
        if lunch == "ごはん":
            text = f"{date_str}の朝ごはんは、{space.join(data[0])}。昼ごはんは、{space.join(data[1])}。晩ごはんは{space.join(data[2])}です。"
        else:
            time = lunch[0]
            time_num = "朝昼夕".index(time)
            text = f"{date_str}の{time}ごはんは、{space.join(data[time_num])}です。"
        message_japanese = cek.Message(message=text, language="ja")
        response = clova.response([message_japanese])
    except:
        message_japanese = cek.Message(message="データがないっぽいです ", language="ja")
        response = clova.response([message_japanese])
    return response


@clova.handle.intent("Only_Time")
def wife_status_handler(clova_request):
    slots = clova_request.slots
    date = slots.get("datetime")
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    date_tuple = (date.month, date.day)
    try:
        data = flow(*date_tuple)
        date_str = f"{date.month}月{date.day}日"
        space = " "
        text = f"{date_str}の朝ごはんは、{space.join(data[0])}。昼ごはんは、{space.join(data[1])}。晩ごはんは{space.join(data[2])}です。"
        message_japanese = cek.Message(message=text, language="ja")
        response = clova.response([message_japanese])
    except:
        message_japanese = cek.Message(message="データがないっぽいです ", language="ja")
        response = clova.response([message_japanese])
    return response


@clova.handle.intent("Only_Lunch")
def wife_status_handler(clova_request):
    slots = clova_request.slots
    lunch = slots.get("Lunch")
    date = datetime.date.today()
    date_tuple = (date.month, date.day)
    try:
        data = flow(*date_tuple)
        date_str = f"{date.month}月{date.day}日"
        space = " "
        if lunch == "ごはん":
            text = f"{date_str}の朝ごはんは、{space.join(data[0])}。昼ごはんは、{space.join(data[1])}。晩ごはんは{space.join(data[2])}です。"
        else:
            time = lunch[0]
            time_num = "朝昼夕".index(time)
            text = f"{date_str}の{time}ごはんは、{space.join(data[time_num])}です。"
        message_japanese = cek.Message(message=text, language="ja")
        response = clova.response([message_japanese])
    except:
        message_japanese = cek.Message(message="データがないっぽいです ", language="ja")
        response = clova.response([message_japanese])
    return response


@clova.handle.end
def end_handler(clova_request):
    print("session end")


@clova.handle.default
def default_handler(request):
    return clova.response("Sorry I don't understand! Could you please repeat?")


pattern = re.compile("([0-9]+/[0-9]+)|([0-9]+月[0-9]+日)")
pattern_slash = re.compile("([0-9]+/[0-9]+)")
date_pattern = re.compile("([0-9]+月[0-9]+日)")


def near_year(month):
    today = datetime.date.today()
    if month < 4:
        if today.month > 11:
            return today.year + 1
        return today.year
    if month > 10:
        if today.month < 4:
            return today.year - 1
        return today.year
    return today.year


def month_to_pdf(month):
    if os.path.exists(os.path.join("static", datetime.date(near_year(month), month, 1).strftime("%y-%m.pdf"))):
        return URL_HEAD + "static/" + datetime.date(near_year(month), month, 1).strftime("%y-%m.pdf")
    raise ValueError


def download_dorm_menu(month):
    file_name = datetime.date(near_year(month), month, 1).strftime("%y-%m.pdf")
    file_path = os.path.join("static", file_name)

    if os.path.exists(file_path):
        return
    main_page = bs(requests.get("http://www.akashi.ac.jp/dormitory/").text, "lxml")
    anc = main_page.find("a", string="{}月メニュー".format({1: "１", 2: "２", 3: "３", 4: "４", 5: "５", 6: "６",
                                                       7: "７", 8: "８", 9: "９", 10: "１０", 11: "１１", 12: "１２"}[month]))

    month_page = bs(requests.get(anc["href"]).text, "lxml")
    try:
        file_anc = month_page.find("a", string="{}月メニュー".format({1: "１", 2: "２", 3: "３", 4: "４", 5: "５", 6: "６",
                                                             7: "７", 8: "８", 9: "９", 10: "１０", 11: "１１", 12: "１２"}[
                                                                month]))
        if file_anc is None:
            raise ValueError
    except:
        file_anc = month_page.find("a", string="{}月メニュー".format(month))

    response = requests.get(file_anc["href"])

    with open(file_path, "wb") as f:
        f.write(response.content)


def org(month):
    file_name = datetime.date(near_year(month), month, 1).strftime("%y-%m.pdf")
    file_path = os.path.join("static", file_name)

    pickle_name = datetime.date(near_year(month), month, 1).strftime("%y-%m.pickle")
    pickle_path = os.path.join("static", pickle_name)

    if os.path.exists(pickle_path):
        return

    data = {}

    for i in range(8):
        try:
            c_data = tabula.read_pdf(file_path, pages=i)
            for key in c_data.columns:
                if date_pattern.search(key):
                    data[date_pattern.search(key).group()] = c_data[key]
        except subprocess.CalledProcessError:
            break

    with open(pickle_path, "wb") as f:
        pickle.dump(data, f)


def get_date(month, day):
    pickle_name = datetime.date(near_year(month), month, 1).strftime("%y-%m.pickle")
    pickle_path = os.path.join("static", pickle_name)

    with open(pickle_path, "rb") as f:
        data = pickle.load(f)
    return data[f"{month}月{day}日"]


def parse_data(data):
    ret = []
    c_data = []
    for dat in data:
        if not pd.isnull(dat):
            if is_splitter(dat):
                if c_data:
                    ret.append(c_data)
                c_data = []
            else:
                c_data.append(dat)
    return ret


def is_splitter(text):
    if text == "kcal g g g":
        return True
    if "蛋白質" in text and "熱量" in text:
        return True
    for weekday in "月火水木金土日":
        if f"({weekday})" in text:
            return True
    if text.translate(str.maketrans({key: "" for key in "1234567890 ."})) == "":
        return True
    return False


@lru_cache(maxsize=None)
def flow(month, day):
    download_dorm_menu(month)
    org(month)
    return parse_data(get_date(month, day))


def date_to_str(date):
    weekdays = "月火水木金土日"
    weekday = date.weekday()
    return f"{date.month}月{date.day}日 ({weekdays[weekday]})"


@app.route("/callback", methods=["POST"])
def callback():
    body = json.loads(request.get_data(as_text=True))

    text = body["events"][0]["message"]["text"]
    nl = "\n"
    try:
        if text in {"今日", "飯", "めし"}:
            dat = flow(datetime.date.today().month, datetime.date.today().day)
            response = f"{date_to_str(datetime.date.today())}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"朝", "今朝", "あさ", "朝食", "ちょうしょく"}:
            dat = flow(datetime.date.today().month, datetime.date.today().day)
            response = f"{date_to_str(datetime.date.today())}\n\n**--[朝]--**\n{nl.join(dat[0])}"
        elif text in {"昼", "ひる", "ちゅうしょく", "昼食"}:
            dat = flow(datetime.date.today().month, datetime.date.today().day)
            response = f"{date_to_str(datetime.date.today())}\n\n**--[昼]--**\n{nl.join(dat[1])}"
        elif text in {"夜", "晩", "よる", "ばん", "ゆうしょく", "夕食"}:
            dat = flow(datetime.date.today().month, datetime.date.today().day)
            response = f"{date_to_str(datetime.date.today())}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif pattern.search(text):
            if pattern_slash.search(text):
                dat = flow(*map(int, text.split("/")))
                date = datetime.date(near_year(int(text.split("/")[0])), *map(int, text.split("/")))
            else:
                dat = flow(*map(int, text[:-1].split("月")))
                date = datetime.date(near_year(int(text[:-1].split("月")[0])), *map(int, text[:-1].split("月")))
            response = f"{date_to_str(date)}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"明日", "あした"}:
            date = datetime.date.today() + datetime.timedelta(days=1)
            dat = flow(date.month, date.day)
            response = f"{date_to_str(date)}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"明後日", "あさって"}:
            date = datetime.date.today() + datetime.timedelta(days=2)
            dat = flow(date.month, date.day)
            response = f"{date_to_str(date)}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"昨日", "きのう"}:
            date = datetime.date.today() + datetime.timedelta(days=-1)
            dat = flow(date.month, date.day)
            response = f"{date_to_str(date)}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"一昨日", "おととい"}:
            date = datetime.date.today() + datetime.timedelta(days=-1)
            dat = flow(date.month, date.day)
            response = f"{date_to_str(date)}\n\n**--[朝]--**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text in {"月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日", "月曜", "火曜", "水曜", "木曜", "金曜", "土曜", "日曜"}:
            inter = "月火水木金土日".index(text[0]) - datetime.date.today().weekday()
            if inter < 0:
                inter += 7
            date = datetime.date.today() + datetime.timedelta(days=inter)
            dat = flow(date.month, date.day)
            response = f"{date_to_str(date)}\n\n**--[朝]-**\n{nl.join(dat[0])}\n\n**--[昼]--**\n{nl.join(dat[1])}\n\n**--[晩]--**\n{nl.join(dat[2])}"
        elif text.endswith("url"):
            month = text.replace("url", "").replace("月", "").replace("の", "")
            if not month:
                month = datetime.date.today().strftime("%m")
            response = month_to_pdf(int(month))
        else:
            response = "\n".join(("---対応するメッセージ---",
                                  "今日, 飯, めし: 本日の寮食メニュー",
                                  "朝, 朝食: 本日の朝食",
                                  "昼, 昼食: 本日の昼食",
                                  "晩, 夕食: 本日の夕食",
                                  "明日, 明後日, 昨日, 一昨日: 対応する日のメニュー",
                                  "〇曜日, 〇曜: 最も近い将来の対応する曜日のメニュー",
                                  "(月)/(日), 〇月〇日: 対応する日付のメニュー",
                                  "〇月のurl, url: 〇月のメニューのpdfデータ、与えられなければ今月"
                                  "\n\n全部自動化してるからそりゃエラーを吐いたり間違ったデータを送ることだってあるけど、気にしたら負けだと思う。\n初回のデータダウンロード・解析は時間がかかる(30秒くらい)から、メッセージを送っても反応が無いときはちょっとだけ待って、もういっかい話しかけてね。"))
    except:
        response = "(データが)ないです。"

    app.logger.info("Request body: " + repr(body))
    try:
        line_bot_api.reply_message(body["events"][0]["replyToken"], TextSendMessage(text=response))
    except InvalidSignatureError:
        abort(400)

    return "OK"


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
