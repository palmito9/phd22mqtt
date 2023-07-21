from guider import Guider
import copy
import logging
import math
import sys
import time
import json
import paho.mqtt.client as mqtt


class MQTTEventHandler:
    """The main class for handling PHD2 events and send them to MQTT"""

    def __init__(self, host, defaultPixelScale = 1):
        self.host = host
        self.pixelScale = defaultPixelScale

    def __enter__(self):
        return self

    def on_event(self, ev):
        e = ev["Event"]
        state, avgDist = self.guider.GetStatus()
        data = copy.copy(ev)
        data["guidestate"] = state;
        isSettling = (1 if not self.guider.Settle.Done else 0) if self.guider.Settle is not None else 0
        if e == "GuideStep":
            data["IsSettling"] = isSettling
            data["RADistanceRaw"] = data["RADistanceRaw"] * self.pixelScale
            data["DECDistanceRaw"] = data["DECDistanceRaw"] * self.pixelScale
            data["RADistanceGuide"] = data["RADistanceGuide"] * self.pixelScale
            data["DECDistanceGuide"] = data["DECDistanceGuide"] * self.pixelScale
            try:
                data["RADuration"] = data["RADuration"] * (-1 if data["RADirection"] == "East" else 1)
            except:
                None
            try:
                data["DECDuration"] = data["DECDuration"] * (-1 if data["DECDirection"] == "South" else 1)
            except:
                None
            stats = self.guider.GetStats()
            if stats is not None:
                data["rmsTot"] = stats.rms_tot * self.pixelScale;
                data["rmsRa"] = stats.rms_ra * self.pixelScale;
                data["rmsDec"] = stats.rms_dec * self.pixelScale;
        elif e == "GuidingDithered":
            data["DitherXDistanceRaw"] = data["dx"]
            data["DitherYDistanceRaw"] = data["dy"]
            data["DitherDistanceRaw"] = math.sqrt(data["DitherXDistanceRaw"]**2 + data["DitherYDistanceRaw"]**2)
        elif e == "GuidingStopped" or e == "StartGuiding":
            data["SNR"] = 0
            data["HFD"] = 0
            data["dx"] = 0
            data["dy"] = 0
            data["RADistanceRaw"] = 0
            data["DECDistanceRaw"] = 0
            data["RADistanceGuide"] = 0
            data["DECDistanceGuide"] = 0
        elif e == "LoopingExposuresStopped" or e == "StarSelected":
            data["SNR"] = 0
            data["HFD"] = 0
        push_mqtt_message(self.host, data)

def on_log(client, data, level, buf):
    log.info(f"log: {buf}")

def connectGuider():
    try:
        log.info("Connecting to PHD2")
        guider.Connect()
        eventHanlder.pixelScale = guider.PixelScale()
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        log.error("Issue when connecting to PHD2", exc_info=True)

def push_mqtt_message(host, data):
    try:
        client = mqtt.Client("guider", clean_session=False)
        client.connect(host)
        client.on_log = on_log
        log.info("Pushing data")
        log.info(json.dumps(data))
        client.publish("sensors/guider", json.dumps(data))
        client.disconnect()
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        log.info(f"Error pushing data: {data}", exc_info=True)

def getArgv(index, default = None):
    if len(sys.argv) > index:
        return sys.argv[index]
    return default

# ==== main ====

logging.basicConfig(level=logging.INFO)
logFormatter = logging.Formatter(
    "%(asctime)s %(module)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)
log = logging.getLogger()

phdhost = getArgv(1, "localhost")
mqtthost = getArgv(2, "localhost")

eventHanlder = MQTTEventHandler(mqtthost)

with Guider(phdhost, 1, eventHanlder.on_event) as guider:
    eventHanlder.guider = guider
    CHECK_CONNECTION_INTERVAL = 10 # seconds
    while True:
        if not guider.IsConnected():
            log.info("PHD2 Connecting")
            connectGuider()
            log.info("PHD2 Connected!")
        time.sleep(CHECK_CONNECTION_INTERVAL)
        log.info("PHD2 Checking Connection")
        try:
            guider.Call("get_connected") # check connection still alive
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            log.error("PHD2 Disconnected!")
            guider.Disconnect() # force disconnect so it can reconnect when possible
            data = {}
            data["guidestate"] = "Disconnected"
            data["SNR"] = 0
            data["HFD"] = 0
            data["dx"] = 0
            data["dy"] = 0
            data["RADistanceRaw"] = 0
            data["DECDistanceRaw"] = 0
            data["RADistanceGuide"] = 0
            data["DECDistanceGuide"] = 0
            push_mqtt_message(mqtthost, data)

    sys.exit(0)