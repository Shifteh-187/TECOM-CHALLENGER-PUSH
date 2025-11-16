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
     |  📱  Pushover / Mobile App |
     |____________________________|


Tecom Listener
Challenger V8 10 / 10+ / XR → TCP → Local Server → Custom Notifications via Pushover

This requires a linux system on the same local network as the TECOM alarm panel ,  The Tecom alarm panel when set correctly in compaths, will send a packet to a printer, we simulate this printer as a python script and intercept the information
and place it in a que to be used as you wish, as a pushover notification your phone and many other ideas

A lightweight Python daemon for Tecom Challenger panels that listens for TCP event data (Computer Event Driven / Printer formats) and delivers custom notifications using a rule-based engine.

Supports Pushover, custom regex rules, zone/input naming, and reliable queued delivery.

Perfect for home labs, property monitoring, and replacing UltraSync cloud notifications with something faster, local, and fully customizable.

This could also be modified to work work with home automation etc

⭐ Features

Works with all Tecom Ethernet panels (Challenger 10, 10+, XR, V8 with IP module)

Local-only operation — no cloud needed

Custom notifications using regex rules

Queue system ensures no events are lost

Pushover support (optional)

Human-friendly YAML configuration

Permanent logging

Systemd service support

Zero coding required for users

📦 Installation
1. Clone repository
git clone https://github.com/shifteh-187/tecom-listener.git
cd tecom-listener

2. Install requirements
pip3 install -r requirements.txt

3. Create storage directory
sudo mkdir -p /var/lib/tecom-listener
sudo chown $USER:$USER /var/lib/tecom-listener

🛠 Configuration

All settings live in config.yaml.
This is the only file normal users will edit.

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

📜 Requirements

requirements.txt:

requests
PyYAML

🤝 Contributing

PRs welcome!

Ideas:

Add MQTT output

Add Telegram support

Add Webhooks

Add Grafana/InfluxDB export

Build a web dashboard

📜 License

MIT License.
