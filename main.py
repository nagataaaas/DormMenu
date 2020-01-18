import re
import os
import datetime
import json
import subprocess
import threading

from functools import lru_cache

import tabula
import requests
import pandas as pd
from PIL import Image
from bs4 import BeautifulSoup as bs
from flask import Flask, request, abort, jsonify, send_file
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage
from pdf2image import convert_from_path

app = Flask(__name__)

MenuData = dict()

line_bot_api = LineBotApi(os.environ.get("LineBotAccessToken", "none"))
handler = WebhookHandler(os.environ.get("LineBotHandler", "none"))

URL_HEAD = "https://dorm-menu.herokuapp.com/"
app.config["JSON_AS_ASCII"] = False

pattern = re.compile("([0-9]+/[0-9]+)|([0-9]+月[0-9]+日)")
pattern_slash = re.compile("([0-9]+/[0-9]+)")
date_pattern = re.compile("([0-9]+月[0-9]+日)")
day_pattern = re.compile("[月火水木金土日]曜日?")


@app.route("/health")
def health():
    return "ok"


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
    return None


def download_dorm_menu(month):
    file_name = datetime.date(near_year(month), month, 1).strftime("%y-%m.pdf")
    file_path = os.path.join("static", file_name)

    key = datetime.date(near_year(month), month, 1).strftime("%y-%m")

    if key in MenuData or os.path.exists(file_path):
        return

    full_width_num = {1: "１", 2: "２", 3: "３", 4: "４", 5: "５", 6: "６",
                      7: "７", 8: "８", 9: "９", 10: "１０", 11: "１１", 12: "１２"}

    main_page = bs(requests.get("http://www.akashi.ac.jp/dormitory/").text, "lxml")
    full_width = main_page.find("a", string="{}月メニュー".format(full_width_num[month]))
    half_width = main_page.find("a", string="{}月メニュー".format(month))
    if not full_width and not half_width:
        return

    pdf = requests.get((full_width or half_width)["href"])

    with open(file_path, "wb") as f:
        f.write(pdf.content)


def org(month):
    file_name_body = datetime.date(near_year(month), month, 1).strftime("%y-%m")
    file_name = file_name_body + ".pdf"
    file_image_directory = os.path.join("static", file_name_body)
    file_path = os.path.join("static", file_name)

    if file_name_body in MenuData or not os.path.exists(file_path):
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

    MenuData[file_name_body] = data

    os.makedirs(file_image_directory, exist_ok=True)
    if not os.listdir(file_image_directory):
        images = convert_from_path(file_path)
        for ind, img in enumerate(images):
            img.save(os.path.join(file_image_directory, "{}.jpeg".format(ind)), "jpeg")


def fetch_data(month, day):
    key = datetime.date(near_year(month), month, 1).strftime("%y-%m")

    return MenuData[key][f"{month}月{day}日"]


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
    if all(t in "kcalg1234567890. " for t in text.strip()) or text.strip() in ("栄養価",):
        return True
    if "蛋白質" in text and "熱量" in text:
        return True
    if re.compile(r"[AB]?定食 ([\d.\s]+)+").match(text):
        return True
    for weekday in "月火水木金土日":
        if f"({weekday})" in text:
            return True
    return False


@lru_cache(maxsize=None)
def flow(month, day):
    download_dorm_menu(month)
    org(month)
    data = fetch_data(month, day)
    return parse_data(data)


def date_to_str(date):
    weekday = "月火水木金土日"[date.weekday()]
    return f"{date.month}月{date.day}日 ({weekday})"


format_mon_lun_din = "{}\n\n**--[朝]--**\n{}\n\n**--[昼]--**\n{}\n\n**--[晩]--**\n{}"

format_mon = "{}\n\n**--[朝]--**\n{}"

format_lun = "{}\n\n**--[昼]--**\n{}"

format_din = "{}\n\n**--[晩]--**\n{}"""


def get_data(text):
    nl = "\n"
    month = False
    response = False
    is_image_needed = text.endswith("画像")
    if is_image_needed:
        text = text[:-2]
    try:
        if text in {"今日", "飯", "めし"} | {"朝", "今朝", "あさ", "朝食", "ちょうしょく"} | \
                {"昼", "ひる", "ちゅうしょく", "昼食"} | {"夜", "晩", "よる", "ばん", "ゆうしょく", "夕食"}:
            date_str = datetime.date.today().strftime("%y-%m-%d")
            month = datetime.date.today().month
            data = flow(datetime.date.today().month, datetime.date.today().day)
            if text in {"今日", "飯", "めし"}:
                response = format_mon_lun_din.format(date_str, *map(nl.join, data))
            elif text in {"朝", "今朝", "あさ", "朝食", "ちょうしょく"}:
                response = format_mon.format(date_str, nl.join(data[0]))
            elif text in {"昼", "ひる", "ちゅうしょく", "昼食"}:
                response = format_lun.format(date_str, nl.join(data[1]))
            elif text in {"夜", "晩", "よる", "ばん", "ゆうしょく", "夕食"}:
                response = format_din.format(date_str, nl.join(data[2]))
        elif pattern.search(text):
            if pattern_slash.search(text):
                sptxt = text.split("/")
            else:
                sptxt = text[:-1].split("月")
            date_str = datetime.date(near_year(int(sptxt[0])), int(sptxt[0]), int(sptxt[1])).strftime("%y-%m-%d")
            month = int(sptxt[1])
            data = flow(*map(int, sptxt))
            date = datetime.date(near_year(int(sptxt[0])), *map(int, sptxt))
            response = format_mon_lun_din.format(date_to_str(date), *map(nl.join, data))

        elif text in {"明日", "あした", "あす"} | {"明後日", "あさって"} | {"昨日", "きのう"} | {"一昨日", "おととい"}:
            if text in {"明日", "あした", "あす"}:
                date = datetime.date.today() + datetime.timedelta(days=1)
            elif text in {"明後日", "あさって"}:
                date = datetime.date.today() + datetime.timedelta(days=2)
            elif text in {"昨日", "きのう"}:
                date = datetime.date.today() + datetime.timedelta(days=-1)
            else:
                date = datetime.date.today() + datetime.timedelta(days=-2)

            date_str = datetime.date(near_year(date.month), date.month, date.day).strftime("%y-%m-%d")
            month = date.month
            data = flow(date.month, date.day)
            response = format_mon_lun_din.format(date_to_str(date), *map(nl.join, data))
        elif day_pattern.search(text):
            inter = "月火水木金土日".index(text[0]) - datetime.date.today().weekday()
            if inter < 0:
                inter += 7
            date = datetime.date.today() + datetime.timedelta(days=inter)
            date_str = datetime.date(near_year(date.month), date.month, date.day).strftime("%y-%m-%d")
            month = date.month
            data = flow(date.month, date.day)
            response = format_mon_lun_din.format(date_to_str(date), *map(nl.join, data))

        elif text.endswith("url"):
            month = re.sub("[url月の]", "", text)
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
                                  "\n\n全部自動化してるからそりゃエラーを吐いたり間違ったデータを送ることだってあるけど、気にしたら負けだと思う。",
                                  "そういう時には、送りたいコマンド+\"画像\" (例えば、 『めし画像』)っていう風にすると、画像で返信するから間違いないね。うん。"))
        if is_image_needed:
            raise InterruptedError

    except:
        if date_str.rsplit("-", 1)[0] in MenuData.keys():
            return {"is_image": True, "text": "{}image/{}".format(URL_HEAD, date_str)}
        else:
            response = "(データが)ないです。"
            try:
                if month:
                    response += "なので、代わりにpdfみてみてください…\n" + month_to_pdf(int(month))
            except ValueError:
                pass
    return {"is_image": False, "text": response}


@app.route("/image/<key>")
def image(key):
    year, month, day = map(int, key.split("-"))
    year += 2000
    month_date_str = datetime.date(year, month, 1).strftime("%y-%m")
    if not os.path.exists(
            os.path.join("static", month_date_str, "per_day", "{}.jpeg".format(day))):
        os.makedirs(os.path.join("static", month_date_str, "per_day"), exist_ok=True)
        first_day = datetime.date(year, month, 1).weekday()
        day_with_offset = first_day + day - 1
        page = day_with_offset // 7
        number = day_with_offset % 7
        im = Image.open(os.path.join("static", month_date_str, "{}.jpeg".format(page)))
        im = im.crop((186 + int(287.7 * number), 188, 186 + int(287.7 * (number + 1)) + 13, 1579))
        im.save(
            os.path.join("static", month_date_str, "per_day", "{}.jpeg".format(day)),
            "jpeg")
    return send_file(
        os.path.join("static", month_date_str, "per_day", "{}.jpeg".format(day)))


@app.route("/callback", methods=["POST"])
def callback():
    body = json.loads(request.get_data(as_text=True))
    text = body["events"][0]["message"]["text"].strip()
    result = get_data(text)

    try:
        if result["is_image"]:
            response = ImageSendMessage(
                original_content_url=result["text"],
                preview_image_url=result["text"]
            )
            line_bot_api.reply_message(
                body["events"][0]["replyToken"],
                response)
        else:
            response = result["text"]
            line_bot_api.reply_message(body["events"][0]["replyToken"], TextSendMessage(text=response))
    except InvalidSignatureError:
        abort(400)

    return "OK"


@app.route("/api", methods=["POST"])
def api():
    body = request.get_json()
    text = body["text"]
    result = get_data(text)

    app.logger.info("Request body: " + repr(body))
    try:
        return jsonify(result)
    except InvalidSignatureError:
        abort(400)

    return "OK"


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text)
    )


def init_process():
    for i in range(2):
        download_dorm_menu(datetime.datetime.now(tz=datetime.timezone(offset=datetime.timedelta(hours=+9),
                                                                      name="JST")).month + i)
        org(datetime.datetime.now(tz=datetime.timezone(offset=datetime.timedelta(hours=+9), name="JST")).month + i)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    thread = threading.Thread(target=init_process)
    thread.start()
    app.run(host="0.0.0.0", port=port)
