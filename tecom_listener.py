#!/usr/bin/env python3
"""
Tecom / Challenger TCP listener with multi-backend notifications:
- Pushover
- MQTT
- Telegram
- Home Assistant (REST events)

Uses a YAML config file (config.yaml) for all settings and rules.
"""

import os
import time
import socket
import threading
import re
import json
from typing import Dict, Any, List

import requests
import yaml
import paho.mqtt.client as mqtt

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


# ==========================
# CONFIG LOAD
# ==========================

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Server
    cfg.setdefault("server", {})
    cfg["server"].setdefault("listen_host", "0.0.0.0")
    cfg["server"].setdefault("listen_port", 5000)
    cfg["server"].setdefault("poll_interval_seconds", 5)
    cfg["server"].setdefault("base_dir", "/var/lib/tecom-listener")

    # Pushover
    cfg.setdefault("pushover", {})
    cfg["pushover"].setdefault("enabled", False)
    cfg["pushover"].setdefault("user_key", "")
    cfg["pushover"].setdefault("api_token", "")
    cfg["pushover"].setdefault("default_priority", 0)

    # MQTT
    cfg.setdefault("mqtt", {})
    cfg["mqtt"].setdefault("enabled", False)
    cfg["mqtt"].setdefault("host", "localhost")
    cfg["mqtt"].setdefault("port", 1883)
    cfg["mqtt"].setdefault("username", "")
    cfg["mqtt"].setdefault("password", "")
    cfg["mqtt"].setdefault("client_id", "tecom-listener")
    cfg["mqtt"].setdefault("topic", "tecom/events")

    # Telegram
    cfg.setdefault("telegram", {})
    cfg["telegram"].setdefault("enabled", False)
    cfg["telegram"].setdefault("bot_token", "")
    cfg["telegram"].setdefault("chat_id", "")

    # Home Assistant
    cfg.setdefault("homeassistant", {})
    cfg["homeassistant"].setdefault("enabled", False)
    cfg["homeassistant"].setdefault("base_url", "http://homeassistant.local:8123")
    cfg["homeassistant"].setdefault("long_lived_token", "")
    cfg["homeassistant"].setdefault("event_name", "tecom_event")

    cfg.setdefault("zones", {})
    cfg.setdefault("inputs", {})
    cfg.setdefault("notifications", [])

    return cfg


config = load_config(CONFIG_PATH)

BASE_DIR = config["server"]["base_dir"]
QUEUE_FILE = os.path.join(BASE_DIR, "events_queue.txt")
LOG_FILE = os.path.join(BASE_DIR, "events.log")

LISTEN_HOST = config["server"]["listen_host"]
LISTEN_PORT = int(config["server"]["listen_port"])
POLL_INTERVAL_SECONDS = int(config["server"]["poll_interval_seconds"])

ZONES: Dict[str, str] = config.get("zones", {})
INPUTS: Dict[str, str] = config.get("inputs", {})

# Pushover
PUSHOVER_ENABLED = bool(config["pushover"]["enabled"])
PUSHOVER_USER_KEY = config["pushover"]["user_key"]
PUSHOVER_API_TOKEN = config["pushover"]["api_token"]
PUSHOVER_DEFAULT_PRIORITY = int(config["pushover"]["default_priority"])

# MQTT
MQTT_ENABLED = bool(config["mqtt"]["enabled"])
MQTT_HOST = config["mqtt"]["host"]
MQTT_PORT = int(config["mqtt"]["port"])
MQTT_USERNAME = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_CLIENT_ID = config["mqtt"]["client_id"]
MQTT_TOPIC_DEFAULT = config["mqtt"]["topic"]

# Telegram
TELEGRAM_ENABLED = bool(config["telegram"]["enabled"])
TELEGRAM_BOT_TOKEN = config["telegram"]["bot_token"]
TELEGRAM_CHAT_ID = str(config["telegram"]["chat_id"])

# Home Assistant
HA_ENABLED = bool(config["homeassistant"]["enabled"])
HA_BASE_URL = config["homeassistant"]["base_url"].rstrip("/")
HA_TOKEN = config["homeassistant"]["long_lived_token"]
HA_EVENT_DEFAULT = config["homeassistant"]["event_name"]

NOTIFICATION_RULES: List[Dict[str, Any]] = config.get("notifications", [])

os.makedirs(BASE_DIR, exist_ok=True)
file_lock = threading.Lock()

# MQTT client globals
mqtt_client = None
mqtt_connected = False


# ==========================
# UTIL / LOGGING
# ==========================

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    line = f"{now_ts()} {message}"
    with file_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    print(line)


def queue_event(raw_event: str) -> None:
    with file_lock:
        with open(QUEUE_FILE, "a", encoding="utf-8") as f:
            f.write(raw_event + "\n")


# ==========================
# PARSING & RULES
# ==========================

ZONE_RE = re.compile(r"ZONE\s+(\d+)")
INPUT_RE = re.compile(r"INPUT\s+(\d+)")


def extract_context(raw_line: str) -> Dict[str, Any]:
    """
    Pull out zone/input numbers & names for template rendering.
    """
    ctx: Dict[str, Any] = {
        "raw": raw_line,
        "zone_number": "",
        "zone_name": "",
        "input_number": "",
        "input_name": "",
    }

    m_zone = ZONE_RE.search(raw_line)
    if m_zone:
        zn = m_zone.group(1).zfill(3)
        ctx["zone_number"] = zn
        ctx["zone_name"] = ZONES.get(zn, "")

    m_input = INPUT_RE.search(raw_line)
    if m_input:
        inp = m_input.group(1).zfill(3)
        ctx["input_number"] = inp
        ctx["input_name"] = INPUTS.get(inp, "")

    return ctx


def render_template(template: str, ctx: Dict[str, Any]) -> str:
    """
    Safe-ish formatter: if a key is missing, just return the template unchanged.
    """
    try:
        return template.format(**ctx)
    except Exception:
        return template


def get_matching_rule(raw_line: str) -> Dict[str, Any]:
    """
    Return the first rule whose regex matches, or a built-in default rule.
    """
    for rule in NOTIFICATION_RULES:
        pattern = rule.get("match_regex", ".*")
        try:
            if re.search(pattern, raw_line):
                return rule
        except re.error as e:
            log(f"Invalid regex in rule '{rule.get('name','unnamed')}': {e}")
            continue

    # default rule if nothing matched
    return {
        "name": "Default",
        "match_regex": ".*",
        "send_pushover": True,
        "send_mqtt": True,
        "send_telegram": True,
        "send_homeassistant": True,
        "title": "Challenger Event",
        "message": "{raw}",
    }


# ==========================
# PUSHOVER
# ==========================

def send_pushover(title: str, message: str, priority: int = 0) -> bool:
    if not PUSHOVER_ENABLED:
        return True  # treated as success if backend disabled

    if not PUSHOVER_USER_KEY or not PUSHOVER_API_TOKEN:
        log("Pushover enabled but user_key or api_token not configured.")
        return False

    try:
        resp = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_API_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title[:250],
                "message": message[:1024],
                "priority": priority,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return True
        log(f"Pushover error {resp.status_code}: {resp.text}")
        return False
    except Exception as e:
        log(f"Pushover exception: {e}")
        return False


# ==========================
# MQTT
# ==========================

def ensure_mqtt_connected():
    """
    Lazily create and connect the MQTT client.
    """
    global mqtt_client, mqtt_connected

    if not MQTT_ENABLED:
        return

    if mqtt_client is None:
        mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID or "", clean_session=True)
        if MQTT_USERNAME:
            mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    if not mqtt_connected:
        try:
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            mqtt_client.loop_start()
            mqtt_connected = True
            log(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        except Exception as e:
            mqtt_connected = False
            log(f"MQTT connect failed: {e}")


def send_mqtt(ctx: Dict[str, Any], title: str, message: str, rule: Dict[str, Any]) -> bool:
    if not MQTT_ENABLED:
        return True

    ensure_mqtt_connected()
    if not mqtt_connected:
        return False

    topic_t = rule.get("mqtt_topic", MQTT_TOPIC_DEFAULT)
    topic = render_template(topic_t, ctx)

    payload = {
        "title": title,
        "message": message,
        "zone_number": ctx.get("zone_number"),
        "zone_name": ctx.get("zone_name"),
        "input_number": ctx.get("input_number"),
        "input_name": ctx.get("input_name"),
        "rule": rule.get("name", "Unnamed"),
        "timestamp": now_ts(),
    }

    try:
        res = mqtt_client.publish(topic, json.dumps(payload), qos=0, retain=False)
        if res.rc == mqtt.MQTT_ERR_SUCCESS:
            return True
        log(f"MQTT publish error rc={res.rc}")
        return False
    except Exception as e:
        log(f"MQTT publish exception: {e}")
        return False


# ==========================
# TELEGRAM
# ==========================

def send_telegram(title: str, message: str) -> bool:
    if not TELEGRAM_ENABLED:
        return True

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram enabled but bot_token or chat_id not set.")
        return False

    try:
        text = f"{title}\n{message}"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4096]},
            timeout=10,
        )
        if resp.status_code == 200:
            return True
        log(f"Telegram error {resp.status_code}: {resp.text}")
        return False
    except Exception as e:
        log(f"Telegram exception: {e}")
        return False


# ==========================
# HOME ASSISTANT
# ==========================

def send_homeassistant(ctx: Dict[str, Any], title: str, message: str, rule: Dict[str, Any]) -> bool:
    if not HA_ENABLED:
        return True

    if not HA_TOKEN or not HA_BASE_URL:
        log("Home Assistant enabled but base_url or long_lived_token not set.")
        return False

    event_name_t = rule.get("ha_event", HA_EVENT_DEFAULT)
    event_name = render_template(event_name_t, ctx)
    url = f"{HA_BASE_URL}/api/events/{event_name}"

    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "title": title,
        "message": message,
        "zone_number": ctx.get("zone_number"),
        "zone_name": ctx.get("zone_name"),
        "input_number": ctx.get("input_number"),
        "input_name": ctx.get("input_name"),
        "rule": rule.get("name", "Unnamed"),
        "timestamp": now_ts(),
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code in (200, 201):
            return True
        log(f"Home Assistant error {resp.status_code}: {resp.text}")
        return False
    except Exception as e:
        log(f"Home Assistant exception: {e}")
        return False


# ==========================
# QUEUE PROCESSOR
# ==========================

def process_queue_loop():
    log("Queue processor started")

    while True:
        time.sleep(POLL_INTERVAL_SECONDS)

        with file_lock:
            if not os.path.exists(QUEUE_FILE):
                continue
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f if ln.strip()]

        if not lines:
            continue

        log(f"Processing {len(lines)} queued event(s)")
        remaining: List[str] = []

        for raw in lines:
            ctx = extract_context(raw)
            rule = get_matching_rule(raw)

            title_t = rule.get("title", "Challenger Event")
            msg_t = rule.get("message", "{raw}")
            title = render_template(title_t, ctx)
            message = render_template(msg_t, ctx)
            priority = int(rule.get("priority", PUSHOVER_DEFAULT_PRIORITY))

            ok_all = True

            if rule.get("send_pushover", True):
                if not send_pushover(title, message, priority):
                    ok_all = False

            if rule.get("send_mqtt", True):
                if not send_mqtt(ctx, title, message, rule):
                    ok_all = False

            if rule.get("send_telegram", True):
                if not send_telegram(title, message):
                    ok_all = False

            if rule.get("send_homeassistant", True):
                if not send_homeassistant(ctx, title, message, rule):
                    ok_all = False

            if ok_all:
                log(f"Delivered event via rule '{rule.get('name', 'Default')}'")
            else:
                remaining.append(raw)

        # Rewrite queue with any events that failed delivery
        with file_lock:
            with open(QUEUE_FILE, "w", encoding="utf-8") as f:
                for r in remaining:
                    f.write(r + "\n")


# ==========================
# TCP SERVER
# ==========================

def handle_client(conn: socket.socket, addr):
    log(f"New connection from {addr[0]}:{addr[1]}")
    try:
        with conn:
            f = conn.makefile("r", encoding="utf-8", errors="ignore")
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event_line = f"{now_ts()} {line}"
                log(f"RX: {event_line}")
                queue_event(event_line)
    except Exception as e:
        log(f"Connection error from {addr[0]}: {e}")
    finally:
        log(f"Connection closed from {addr[0]}")


def server_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((LISTEN_HOST, LISTEN_PORT))
    sock.listen(5)
    log(f"Listening on {LISTEN_HOST}:{LISTEN_PORT}")

    while True:
        conn, addr = sock.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()


def main():
    log("Tecom listener starting")
    t = threading.Thread(target=process_queue_loop, daemon=True)
    t.start()
    server_loop()


if __name__ == "__main__":
    main()
