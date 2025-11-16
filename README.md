	 Tecom / Challenger Panel
                ||
                ||  events / alarms 
                \/
     .-----------------------.
     |   Debian Listener     |
     |   + Python Script     |
     '-----------------------'
                ||
                ||  webhook / API
                \/
      ____________________________
     |  🔔  Notification Service  |
     |____________________________|

Tecom Listener – Multi-Output Event Gateway
Challenger 10 / 10+ / XR → TCP → Pushover / MQTT / Telegram / Home Assistant

A powerful, fully local event listener for Tecom Challenger panels, supporting:

🔔 Pushover notifications

📡 MQTT output (Home Assistant compatible)

💬 Telegram alerts

📁 Local logging + reliable event queue

⚙️ Customizable notification rules (regex-based)

🏠 Home Assistant automation via MQTT JSON payloads

This replaces or augments UltraSync with something faster, local, private, and completely customizable.



🚀 Features

Works with Challenger 10, 10+, XR, V8 (with IP module)

Supports Computer Event Driven or Printer formats

No cloud required

YAML config file for EVERYTHING

Per-rule selection of outputs (Pushover / MQTT / Telegram)

Home Assistant-friendly MQTT JSON

Reliable delivery queue

Fully open-source Python

Lightweight — runs perfect on Raspberry Pi or Debian server



📦 Installation
1. Clone repository
git clone https://github.com/Shifteth-187/TECOM-CHALLENGER-PUSH.git
cd TECOM-CHALLENGER-PUSH

2. Install dependencies
pip3 install -r requirements.txt

3. Create storage directory

The script stores queue + log files here:
/var/lib/tecom-listener

Create it:
sudo mkdir -p /var/lib/tecom-listener
sudo chown $USER:$USER /var/lib/tecom-listener


🛠 Configuration (config.yaml)

All settings live in config.yaml.

You can enable/disable any output:
Pushover
MQTT
Telegram

You can also control outputs per rule.
Example config.yaml
server:
  listen_host: "0.0.0.0"
  listen_port: 5000
  poll_interval_seconds: 5
  base_dir: "/var/lib/tecom-listener"

pushover:
  enabled: true
  user_key: "YOUR_PUSHOVER_USER_KEY"
  api_token: "YOUR_PUSHOVER_API_TOKEN"
  default_priority: 0

zones:
  "001": "Front Door PIR"
  "002": "Lounge PIR"
  "003": "Gun Safe PIR"

inputs:
  "005": "Safe Reed"
  "006": "Garage Door"


📡 Tecom Panel Setup

In CTPlus → Comms Paths → Add New:

Interface location : Onboard
Format: Printer
interface port: Ethernet
connection control : Connect on event ticked
Enabled	✔

IP/Encryption settings
IP Address	Your Linux server
TCP/IP - CLIENT
SEND Port	5000
Subformat: None
Retries	3
Timeout	5 sec

Enable desired event types (alarms, inputs, tamper, menu access, etc.)

🚀 Running

Run manually:

python3 tecom_listener.py


Logs are stored in:

/var/lib/tecom-listener/events.log


Queue (events waiting for delivery):

/var/lib/tecom-listener/events_queue.txt


🔧 Systemd Service (Optional)

Create:

/etc/systemd/system/tecom-listener.service

[Unit]
Description=Tecom Listener Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/tecom/tecom_listener.py
WorkingDirectory=/opt/tecom
Restart=always
User=tecom
Group=tecom

[Install]
WantedBy=multi-user.target


Enable:

sudo systemctl daemon-reload
sudo systemctl enable tecom-listener
sudo systemctl start tecom-listener



🏡 Home Assistant Integration (MQTT)

If MQTT output is enabled:

Home Assistant will receive JSON payloads like:

{
  "title": "GUN SAFE ALARM",
  "message": "Gun safe triggered!",
  "raw": "2025-11-16 23:14:55 EVENT 3101 ZONE 003 ALARM",
  "zone_number": "003",
  "zone_name": "Gun Safe PIR",
  "input_number": "",
  "input_name": "",
  "timestamp": "2025-11-16 23:14:55"
}


Use HA automations like:

alias: Gun Safe Alarm
trigger:
  - platform: mqtt
    topic: tecom/events
condition:
  - condition: template
    value_template: "{{ trigger.payload_json.zone_number == '003' }}"
action:
  - service: notify.mobile_app_yourphone
    data:
      title: "Gun Safe Alarm"
      message: "{{ trigger.payload_json.message }}"





🧠 How It Works

The panel sends event lines like:

2025-11-16 22:14:55 EVENT 3101 ZONE 003 ALARM


The script:

Saves events to events.log

Adds them to events_queue.txt

Applies notification rules (regex + templates)

Sends via Pushover

Removes successful events from the queue

If sending fails, lines remain queued and retry automatically.






📜 License

MIT License.
