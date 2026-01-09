#!/usr/bin/env python
# -------------------------------------------------------------------------------
#    FILE: genmqtt.py
# PURPOSE: genmqtt.py is a client interface for a MQTT server / broker
#
#  AUTHOR: jgyates
#    DATE: 08-10-2018
#
# MODIFICATIONS:
# -------------------------------------------------------------------------------


import json
import os
import signal
import ssl
import sys
import threading
import time

# The following is need to install the mqtt module: pip install paho-mqtt

try:
    import paho.mqtt.client as mqtt
except Exception as e1:
    print(
        "\n\nThe program requies the paho-mqtt module to be installed. Please use 'sudo pip install paho-mqtt' to install.\n"
    )
    print("Error: " + str(e1))
    sys.exit(2)
try:
    # this will add the parent of the genmonlib folder to the path
    # if we are one level below the genmonlib parent (e.g. in the addon folder)
    file_root = os.path.dirname(os.path.realpath(__file__))
    parent_root = os.path.abspath(os.path.join(file_root, os.pardir))
    if os.path.isdir(os.path.join(parent_root, "genmonlib")):
        sys.path.insert(1, parent_root)

    from genmonlib.myclient import ClientInterface
    from genmonlib.mycommon import MyCommon
    from genmonlib.myconfig import MyConfig
    from genmonlib.mylog import SetupLogger
    from genmonlib.mysupport import MySupport
    from genmonlib.mythread import MyThread
    from genmonlib.program_defaults import ProgramDefaults

except Exception as e1:
    print(
        "\n\nThis program requires the modules located in the genmonlib directory in the github repository.\n"
    )
    print(
        "Please see the project documentation at https://github.com/jgyates/genmon.\n"
    )
    print("Error: " + str(e1))
    sys.exit(2)


ONLINE_PAYLOAD = "Online"
OFFLINE_PAYLOAD = "Offline"

# Mapping from genmon mqtt topics to how the entity should be represented in Home Assistant.
# An entity will only be created when the topic is first published.
# This is likely an incomplete list because it was created by jterrace@ based on a Generac evolution controller.
# If other people use this, additional topics should be added if found for other controllers.
HA_SENSOR_MAP = {
    # --- Status & Outage ---
    "generator/Outage/Status": {
        "name": "Outage Status",
        "ic": "mdi:weather-lightning",
    },
    "generator/Outage/System In Outage": {
        "name": "System In Outage",
        "ic": "mdi:weather-lightning",
    },
    "generator/Outage/Utility Voltage": {
        "name": "Outage Utility Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Outage/Utility Voltage Minimum": {
        "name": "Outage Utility Min Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Outage/Utility Voltage Maximum": {
        "name": "Outage Utility Max Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Outage/Utility Threshold Voltage": {
        "name": "Outage Utility Threshold Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Outage/Utility Pickup Voltage": {
        "name": "Outage Utility Pickup Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Outage/Startup Delay": {
        "name": "Outage Startup Delay",
        "dev_cla": "duration",
        "unit_of_meas": "s",
        "json": True,
    },
    # --- Engine Status ---
    "generator/Status/Engine/Battery Voltage": {
        "name": "Battery Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
        "ic": "mdi:car-battery",
    },
    "generator/Status/Engine/Battery Charger Current": {
        "name": "Battery Current",
        "dev_cla": "current",
        "ic": "mdi:car-battery",
        "unit_of_meas": "mA",
        "json": True,
    },
    "generator/Status/Engine/RPM": {
        "name": "RPM",
        "unit_of_meas": "RPM",
        "json": True,
        "ic": "mdi:rotate-right",
    },
    "generator/Status/Engine/Frequency": {
        "name": "Frequency",
        "dev_cla": "frequency",
        "unit_of_meas": "Hz",
        "json": True,
        "ic": "mdi:sine-wave",
    },
    "generator/Status/Engine/Output Voltage": {
        "name": "Output Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Status/Engine/Output Current": {
        "name": "Output Current",
        "dev_cla": "current",
        "unit_of_meas": "A",
        "json": True,
    },
    "generator/Status/Engine/Current L1": {
        "name": "Output Current L1",
        "dev_cla": "current",
        "unit_of_meas": "A",
        "json": True,
    },
    "generator/Status/Engine/Current L2": {
        "name": "Output Current L2",
        "dev_cla": "current",
        "unit_of_meas": "A",
        "json": True,
    },
    "generator/Status/Engine/Output Power (Single Phase)": {
        "name": "Output Power",
        "dev_cla": "power",
        "unit_of_meas": "kW",
        "json": True,
        "ic": "mdi:flash",
    },
    "generator/Status/Engine/Switch State": {"name": "Switch State"},
    "generator/Status/Engine/Engine State": {
        "name": "Engine State",
        "ic": "mdi:engine",
    },
    # --- Logs ---
    "generator/Status/Last Log Entries/Logs/Alarm Log": {
        "name": "Last Alarm Log",
        "ic": "mdi:alarm-light",
        "ent_cat": "diagnostic",
    },
    "generator/Status/Last Log Entries/Logs/Run Log": {
        "name": "Last Action",
        "ic": "mdi:motion-play-outline",
        "ent_cat": "diagnostic",
    },
    # --- Maintenance / Info ---
    "generator/Maintenance/Model": {"name": "Model"},
    "generator/Maintenance/Generator Serial Number": {
        "name": "Serial Number",
        "ic": "mdi:barcode-scan",
    },
    "generator/Maintenance/Controller Detected": {
        "name": "Controller Detected",
    },
    "generator/Maintenance/Nominal RPM": {"name": "Nominal RPM"},
    "generator/Maintenance/Rated kW": {"name": "Capacity"},
    "generator/Maintenance/Nominal Frequency": {
        "name": "Nominal Frequency",
    },
    "generator/Maintenance/Fuel Type": {"name": "Fuel Type"},
    "generator/Maintenance/Generator Phase": {"name": "Phase"},
    "generator/Maintenance/Engine Displacement": {
        "name": "Engine Displacement",
        "unit_of_meas": "cc",
        "json": True,
    },
    "generator/Maintenance/Ambient Temperature Sensor": {
        "name": "Ambient Temperature",
        "dev_cla": "temperature",
        "unit_of_meas": "°F",
        "json": True,
        "ic": "mdi:thermometer",
    },
    # --- Controller Settings ---
    "generator/Maintenance/Controller Settings/Calibrate Current 1": {
        "name": "Controller Calibrate Current 1",
        "json": True,
    },
    "generator/Maintenance/Controller Settings/Calibrate Current 2": {
        "name": "Controller Calibrate Current 2",
        "json": True,
    },
    "generator/Maintenance/Controller Settings/Calibrate Volts": {
        "name": "Controller Calibrate Volts",
        "json": True,
    },
    "generator/Maintenance/Controller Settings/Nominal Line Voltage": {
        "name": "Controller Nominal Line Voltage",
    },
    "generator/Maintenance/Controller Settings/Rated Max Power": {
        "name": "Controller Rated Max Power",
    },
    "generator/Maintenance/Controller Settings/Hours of Protection": {
        "name": "Hours of Protection",
        "dev_cla": "duration",
        "unit_of_meas": "h",
        "json": True,
    },
    # --- Exercise ---
    "generator/Maintenance/Exercise/Exercise Time": {
        "name": "Exercise Time",
        "ic": "mdi:refresh-auto",
        "ent_cat": "diagnostic",
    },
    # --- Line Stats ---
    "generator/Status/Line/Utility Voltage": {
        "name": "Line Utility Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Status/Line/Utility Max Voltage": {
        "name": "Line Utility Max Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Status/Line/Utility Min Voltage": {
        "name": "Line Utility Min Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    "generator/Status/Line/Utility Threshold Voltage": {
        "name": "Line Utility Threshold Voltage",
        "dev_cla": "voltage",
        "unit_of_meas": "V",
        "json": True,
    },
    # --- Service ---
    "generator/Maintenance/Service/Service A Due": {
        "name": "Service A Due",
        "ic": "mdi:tools",
        "ent_cat": "diagnostic",
    },
    "generator/Maintenance/Service/Service B Due": {
        "name": "Service B Due",
        "ic": "mdi:tools",
        "ent_cat": "diagnostic",
    },
    "generator/Maintenance/Service/Battery Check Due": {
        "name": "Service Battery Check Due",
        "ic": "mdi:car-battery",
        "ent_cat": "diagnostic",
    },
    "generator/Maintenance/Service/Total Run Hours": {
        "name": "Total Run Time",
        "dev_cla": "duration",
        "unit_of_meas": "h",
        "json": True,
        "ic": "mdi:counter",
    },
    "generator/Maintenance/Service/Hardware Version": {
        "name": "Hardware Version",
    },
    "generator/Maintenance/Service/Firmware Version": {
        "name": "Firmware Version",
    },
    # --- Consumption ---
    "generator/Maintenance/kW Hours in last 30 days": {
        "name": "Energy Used Last 30 Days",
        "dev_cla": "energy",
        "unit_of_meas": "kWh",
        "json": True,
        "ic": "mdi:lightning-bolt",
    },
    "generator/Maintenance/Fuel Consumption in last 30 days": {
        "name": "Fuel Consumption Last 30 Days",
        "dev_cla": "volume",
        "unit_of_meas": "gal",
        "json": True,
        "ic": "mdi:fuel",
    },
    "generator/Maintenance/Run Hours in last 30 days": {
        "name": "Run Hours Last 30 Days",
        "dev_cla": "duration",
        "unit_of_meas": "h",
        "json": True,
    },
}


def _CleanString(s):
    s = "".join(c if (c.isalnum() or c == "_") else "_" for c in s)
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_").lower()


# ------------ MyGenPush class --------------------------------------------------
class MyGenPush(MySupport):

    # ------------ MyGenPush::init-----------------------------------------------
    def __init__(
        self,
        host=ProgramDefaults.LocalHost,
        port=ProgramDefaults.ServerPort,
        log=None,
        callback=None,
        discovery_callback=None,
        polltime=None,
        blacklist=None,
        flush_interval=float("inf"),
        use_numeric=False,
        use_numeric_object=False,
        strlist_json = False,
        debug=False,
        loglocation=ProgramDefaults.LogPath,
        console=None,
    ):

        super(MyGenPush, self).__init__()
        self.Callback = callback
        self.DiscoveryCallback = discovery_callback

        self.UseNumeric = use_numeric
        self.UseNumericObject = use_numeric_object
        self.StrListJson = strlist_json
        self.debug = debug
        self.Exiting = False

        if polltime == None:
            self.PollTime = 3
        else:
            self.PollTime = float(polltime)

        if log != None:
            self.log = log
        else:
            # log errors in this module to a file
            self.log = SetupLogger("client", os.path.join(loglocation, "mygenpush.log"))

        self.console = console

        self.AccessLock = threading.Lock()
        self.BlackList = blacklist
        self.LastValues = {}
        self.FlushInterval = flush_interval
        self.LastChange = {}
        self.DiscoveryPublished = set()

        try:
            self.Generator = ClientInterface(host=host, port=port, log=log)

            self.GetGeneratorStartInfo()
            # start thread to accept incoming sockets for nagios heartbeat
            self.Threads["MainPollingThread"] = MyThread(
                self.MainPollingThread, Name="MainPollingThread", start=False
            )
            self.Threads["MainPollingThread"].Start()

        except Exception as e1:
            self.LogErrorLine("Error in mygenpush init: " + str(e1))

    # ----------  MyGenPush::ControllerIsEvolution2 -----------------------------
    def ControllerIsEvolution2(self):
        try:
            if "evolution 2.0" in self.StartInfo["Controller"].lower():
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsEvolution2: " + str(e1))
            return False

    # ----------  MyGenPush::ControllerIsEvolutionNexus -------------------------
    def ControllerIsEvolutionNexus(self):
        try:
            if self.ControllerIsEvolution() or self.ControllerIsNexus:
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsEvolutionNexus: " + str(e1))
            return False

    # ----------  MyGenPush::ControllerIsEvolution ------------------------------
    def ControllerIsEvolution(self):
        try:
            if "evolution" in self.StartInfo["Controller"].lower():
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsEvolution: " + str(e1))
            return False

    # ----------  MyGenPush::ControllerIsNexius ---------------------------------
    def ControllerIsNexius(self):
        try:
            if "nexus" in self.StartInfo["Controller"].lower():
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsNexius: " + str(e1))
            return False

    # ----------  MyGenPush::ControllerIsGeneracH100 ----------------------------
    def ControllerIsGeneracH100(self):
        try:
            if (
                "h-100" in self.StartInfo["Controller"].lower()
                or "g-panel" in self.StartInfo["Controller"].lower()
            ):
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsGeneracH100: " + str(e1))
            return False

    # ----------  MyGenPush::ControllerIsGeneracPowerZone -----------------------
    def ControllerIsGeneracPowerZone(self):
        try:
            if "powerzone" in self.StartInfo["Controller"].lower():
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in ControllerIsGeneracPowerZone: " + str(e1))
            return False

    # ----------  MyGenPush::GetGeneratorStartInfo ------------------------------
    def GetGeneratorStartInfo(self):

        try:
            data = self.SendCommand("generator: start_info_json")
            self.StartInfo = {}
            self.StartInfo = json.loads(data)

            return True
        except Exception as e1:
            self.LogErrorLine("Error in GetGeneratorStartInfo: " + str(e1))
            return False

    # ----------  MyGenPush::SendCommand ----------------------------------------
    def SendCommand(self, Command):

        if len(Command) == 0:
            return "Invalid Command"

        try:
            with self.AccessLock:
                data = self.Generator.ProcessMonitorCommand(Command)
        except Exception as e1:
            self.LogErrorLine("Error calling  ProcessMonitorCommand: " + str(Command))
            data = ""

        return data

    # ---------- MyGenPush::MainPollingThread-----------------------------------
    def MainPollingThread(self):

        while True:
            try:

                if not self.UseNumeric and not self.UseNumericObject:
                    statusdata = self.SendCommand("generator: status_json")
                    maintdata = self.SendCommand("generator: maint_json")
                    outagedata = self.SendCommand("generator: outage_json")
                    monitordata = self.SendCommand("generator: monitor_json")
                else:
                    statusdata = self.SendCommand("generator: status_num_json")
                    maintdata = self.SendCommand("generator: maint_num_json")
                    outagedata = self.SendCommand("generator: outage_num_json")
                    monitordata = self.SendCommand("generator: monitor_num_json")

                try:
                    GenmonDict = {}
                    TempDict = {}
                    TempDict = json.loads(statusdata)
                    GenmonDict["Status"] = TempDict["Status"]
                    TempDict = json.loads(maintdata)
                    GenmonDict["Maintenance"] = TempDict["Maintenance"]
                    TempDict = json.loads(outagedata)
                    GenmonDict["Outage"] = TempDict["Outage"]
                    TempDict = json.loads(monitordata)
                    GenmonDict["Monitor"] = TempDict["Monitor"]
                    self.CheckDictForChanges(GenmonDict, "generator")

                except Exception as e1:
                    self.LogErrorLine("Unable to get status: " + str(e1))

                if self.DiscoveryCallback is not None:
                    try:
                        current_keys = set(self.LastValues.keys())
                        if current_keys != self.DiscoveryPublished:
                            self.DiscoveryCallback(current_keys)
                            self.DiscoveryPublished = current_keys
                    except Exception as e1:
                        self.LogErrorLine("Unable to publish to ha: " + str(e1))

                if self.WaitForExit("MainPollingThread", float(self.PollTime)):
                    return
            except Exception as e1:
                self.LogErrorLine("Error in mynotify:MainPollingThread: " + str(e1))
                if self.WaitForExit("MainPollingThread", float(self.PollTime)):
                    return

    # ------------ MyGenPush::CheckDictForChanges -------------------------------
    # This function is recursive, it will turn a nested dict into a flat dict keys
    # that have a directory structure with corrposonding values and determine if
    # anyting changed. If it has then call our callback function
    def CheckDictForChanges(self, node, PathPrefix):

        CurrentPath = PathPrefix
        if not isinstance(PathPrefix, str):
            return ""

        if isinstance(node, dict):
            for key, item in node.items():
                if isinstance(item, dict):
                    CurrentPath = PathPrefix + "/" + str(key)
                    if self.UseNumericObject and self.DictIsTopicJSON(item):
                        self.CheckForChanges(CurrentPath, json.dumps(item, sort_keys=False))
                    else:
                        self.CheckDictForChanges(item, CurrentPath)
                elif isinstance(item, list):
                    CurrentPath = PathPrefix + "/" + str(key)
                    if self.ListIsStrings(item):
                        if self.StrListJson:
                            self.CheckForChanges(CurrentPath, json.dumps(item, sort_keys=False))
                        else:
                            # if this is a list of strings, the join the list to one comma separated string
                            self.CheckForChanges(CurrentPath, ", ".join(item))
                    else:
                        for listitem in item:
                            if isinstance(listitem, dict):
                                if self.UseNumericObject and self.DictIsTopicJSON(item):
                                    self.CheckForChanges(CurrentPath, json.dumps(item, sort_keys=False))
                                else:
                                    self.CheckDictForChanges(listitem, CurrentPath)
                            else:
                                self.LogError(
                                    "Invalid type in CheckDictForChanges: %s %s (2)"
                                    % (key, str(type(listitem)))
                                )
                else:
                    CurrentPath = PathPrefix + "/" + str(key)
                    self.CheckForChanges(CurrentPath, item)
        else:
            self.LogError("Invalid type in CheckDictForChanges %s " % str(type(node)))

    # ---------- MyGenPush::DictIsTopicJSON-------------------------------------
    def DictIsTopicJSON(self, entry):
        try:
            if not isinstance(entry, dict):
                return False
            if "type" in entry.keys() and "value" in entry.keys() and "unit" in entry.keys():
                return True
            return False
        except Exception as e1:
            self.LogErrorLine("Error in DictIsTopicJSON: " + str(e1))
            return False
    # ---------- MyGenPush::ListIsStrings---------------------------------------
    # return true if every element of list is a string
    def ListIsStrings(self, listinput):

        try:
            if not isinstance(listinput, list):
                return False
            for item in listinput:
                if sys.version_info[0] < 3:
                    if not (isinstance(item, str) or isinstance(item, unicode)):
                        return False
                else:
                    if not (isinstance(item, str) or isinstance(item, bytes)):
                        return False
            return True
        except Exception as e1:
            self.LogErrorLine("Error in ListIsStrings: " + str(e1))
            return False

    # ---------- MyGenPush::CheckForChanges-------------------------------------
    def CheckForChanges(self, Path, Value):

        try:

            if self.BlackList != None:
                for BlackItem in self.BlackList:
                    if BlackItem.lower() in Path.lower():
                        return
            LastValue = self.LastValues.get(str(Path), None)
            LastChange = self.LastChange.get(str(Path), 0)

            if (
                LastValue == None
                or LastValue != Value
                or (time.time() - LastChange) > self.FlushInterval
            ):
                self.LastValues[str(Path)] = Value
                self.LastChange[str(Path)] = time.time()
                if self.Callback != None:
                    self.Callback(str(Path), Value)

        except Exception as e1:
            self.LogErrorLine("Error in mygenpush:CheckForChanges: " + str(e1))

    # ---------- MyGenPush::Close-----------------------------------------------
    def Close(self):
        self.Exiting = True
        self.KillThread("MainPollingThread")
        self.Generator.Close()


# ------------ MyMQTT class -----------------------------------------------------
class MyMQTT(MyCommon):

    # ------------ MyMQTT::init--------------------------------------------------
    def __init__(
        self,
        log=None,
        loglocation=ProgramDefaults.LogPath,
        host=ProgramDefaults.LocalHost,
        port=ProgramDefaults.ServerPort,
        configfilepath=ProgramDefaults.ConfPath,
        console=None,
    ):

        super(MyMQTT, self).__init__()

        self.log = log
        self.console = console

        self.Exiting = False
        self.Username = None
        self.Password = None

        self.MQTTAddress = None
        self.MonitorAddress = host
        self.MQTTPort = 1883
        self.TopicRoot = None
        self.BlackList = None
        self.UseNumeric = False
        self.StringListJson = False
        self.RemoveSpaces = False
        self.Retain = False
        self.HaDiscovery = False
        self.PollTime = 2
        self.FlushInterval = float(
            "inf"
        )  # default to inifite flush interval (e.g., never)
        self.debug = False

        try:
            genmon_config = MyConfig(
                filename=configfilepath + "genmon.conf", section="GenMon"
            )
            self.SiteName = genmon_config.ReadValue("sitename", default=None)
        except Exception as e1:
            self.LogErrorLine(
                "Error reading "
                + os.path.join(configfilepath, "gengenmonmqtt.conf")
                + " : "
                + str(e1)
            )
            self.console.error(
                "Error reading "
                + os.path.join(configfilepath, "genmqtt.conf")
                + " : "
                + str(e1)
            )
            sys.exit(1)

        try:
            config = MyConfig(
                filename=os.path.join(configfilepath, "genmqtt.conf"),
                section="genmqtt",
                log=log,
            )

            self.Username = config.ReadValue("username")

            self.Password = config.ReadValue("password")

            self.ClientID = config.ReadValue("client_id", default="genmon")

            self.MQTTAddress = config.ReadValue("mqtt_address")

            if self.MQTTAddress == None or not len(self.MQTTAddress):
                log.error("Error: invalid MQTT server address")
                console.error("Error: invalid MQTT server address")
                sys.exit(1)

            self.MonitorAddress = config.ReadValue(
                "monitor_address", default=self.MonitorAddress
            )
            if self.MonitorAddress != None:
                self.MonitorAddress = self.MonitorAddress.strip()
            if self.MonitorAddress == None or not len(self.MonitorAddress):
                self.MonitorAddress = ProgramDefaults.LocalHost

            self.MQTTPort = config.ReadValue("mqtt_port", return_type=int, default=1883)
            self.PollTime = config.ReadValue(
                "poll_interval", return_type=float, default=2.0
            )
            self.UseNumeric = config.ReadValue(
                "numeric_json", return_type=bool, default=False
            )
            self.UseNumericObject = config.ReadValue(
                "numeric_json_object", return_type=bool, default=False
            )
            self.StringListJson = config.ReadValue(
                "strlist_json", return_type=bool, default=False
            )
            self.RemoveSpaces = config.ReadValue(
                "remove_spaces", return_type=bool, default=False
            )
            self.TopicRoot = config.ReadValue("root_topic")

            self.Retain = config.ReadValue(
                "retain", return_type=bool, default=False
            )
            self.HaDiscovery = config.ReadValue(
                "ha_discovery", return_type=bool, default=False
            )

            if self.HaDiscovery and not self.UseNumericObject:
                log.error("Error: the JSON for Numerics setting must be enabled if Home Assistant Discovery is enabled")
                console.error("Error: the JSON for Numerics setting must be enabled if Home Assistant Discovery is enabled")
                sys.exit(1)

            if self.HaDiscovery and self.UseNumeric:
                log.error("Error: the Numeric Topics setting must be disabled if Home Assistant Discovery is enabled")
                console.error("Error: the Numeric Topics setting must be disabled if Home Assistant Discovery is enabled")
                sys.exit(1)

            if self.TopicRoot != None:
                self.TopicRoot = self.TopicRoot.strip()
                self.LogDebug("Root Topic : " + self.TopicRoot)

            if self.TopicRoot == None or not len(self.TopicRoot):
                self.TopicRoot = None

            # http://www.steves-internet-guide.com/mosquitto-tls/
            self.CertificateAuthorityPath = config.ReadValue(
                "cert_authority_path", default=""
            )
            self.TLSVersion = config.ReadValue(
                "tls_version", return_type=str, default="1.0"
            )
            self.CertReqs = config.ReadValue(
                "cert_reqs", return_type=str, default="Required"
            )
            self.ClientCertificatePath = config.ReadValue(
                "client_cert_path", default=""
            )
            self.ClientKeyPath = config.ReadValue(
                "client_key_path", default=""
            )
            BlackList = config.ReadValue("blacklist")

            if BlackList != None:
                if len(BlackList):
                    BList = BlackList.strip().split(",")
                    if len(BList):
                        self.BlackList = []
                        for Items in BList:
                            self.BlackList.append(Items.strip())

            self.debug = config.ReadValue("debug", return_type=bool, default=False)

            if config.HasOption("flush_interval"):
                self.FlushInterval = config.ReadValue(
                    "flush_interval", return_type=float, default=float("inf")
                )
                if self.FlushInterval == 0:
                    self.FlushInterval = float("inf")
            else:
                self.FlushInterval = float("inf")
        except Exception as e1:
            self.LogErrorLine(
                "Error reading "
                + os.path.join(configfilepath, "genmqtt.conf")
                + " : "
                + str(e1)
            )
            self.console.error(
                "Error reading "
                + os.path.join(configfilepath, "genmqtt.conf")
                + " : "
                + str(e1)
            )
            sys.exit(1)

        try:
            self.MQTTclient = mqtt.Client(client_id=self.ClientID)
            if self.Username != None and len(self.Username) and self.Password != None:
                self.MQTTclient.username_pw_set(self.Username, password=self.Password)

            self.MQTTclient.on_connect = self.on_connect
            self.MQTTclient.on_message = self.on_message
            self.MQTTclient.on_disconnect = self.on_disconnect

            if len(self.CertificateAuthorityPath):
                if os.path.isfile(self.CertificateAuthorityPath):
                    cert_reqs = ssl.CERT_REQUIRED
                    if self.CertReqs.lower() == "required":
                        cert_reqs = ssl.CERT_REQUIRED
                    elif self.CertReqs.lower() == "optional":
                        cert_reqs = ssl.CERT_REQUIRED
                    elif self.CertReqs.lower() == "none":
                        cert_reqs = ssl.CERT_NONE
                    else:
                        self.LogError(
                            "Error: invalid cert required specified, defaulting to required: "
                            + self.CertReq
                        )

                    use_tls = ssl.PROTOCOL_TLSv1
                    if self.TLSVersion == "1.0" or self.TLSVersion == "1":
                        use_tls = ssl.PROTOCOL_TLSv1
                    elif self.TLSVersion == "1.1":
                        use_tls = ssl.PROTOCOL_TLSv1_1
                    elif self.TLSVersion == "1.2":
                        use_tls = ssl.PROTOCOL_TLSv1_2
                    else:
                        self.LogError(
                            "Error: invalid TLS version specified, defaulting to 1.0: "
                            + self.TLSVersion
                        )
                    certfile = None
                    keyfile = None
                    # strip off any whitespace
                    self.ClientCertificatePath = self.ClientCertificatePath.strip()
                    self.ClientKeyPath = self.ClientKeyPath.strip()
                    # if nothing is there then use None
                    if len(self.ClientCertificatePath):
                        certfile = self.ClientCertificatePath
                    if len(self.ClientKeyPath):
                        keyfile = self.ClientKeyPath

                    self.MQTTclient.tls_set(
                        ca_certs=self.CertificateAuthorityPath,
                        certfile=certfile,
                        keyfile=keyfile,
                        cert_reqs=cert_reqs,
                        tls_version=use_tls,
                    )
                    self.MQTTPort = 8883  # port for SSL
                else:
                    self.LogError(
                        "Error: Unable to  find CA cert file: "
                        + self.CertificateAuthorityPath
                    )

            # setup last will and testament
            self.LastWillTopic = self.AppendRoot("generator/client_status")
            self.MQTTclient.will_set(
                self.LastWillTopic, payload=OFFLINE_PAYLOAD, qos=0, retain=True
            )
            # connect
            self.LogDebug(
                "Connecting to " + self.MQTTAddress + ":" + str(self.MQTTPort)
            )
            self.MQTTclient.connect(self.MQTTAddress, self.MQTTPort, 60)

            self.Push = MyGenPush(
                host=self.MonitorAddress,
                log=self.log,
                callback=self.PublishCallback,
                discovery_callback=self.SendHaDiscovery if self.HaDiscovery else None,
                polltime=self.PollTime,
                blacklist=self.BlackList,
                flush_interval=self.FlushInterval,
                use_numeric=self.UseNumeric,
                use_numeric_object=self.UseNumericObject,
                strlist_json=self.StringListJson,
                debug=self.debug,
                port=port,
                loglocation=loglocation,
            )

            signal.signal(signal.SIGTERM, self.SignalClose)
            signal.signal(signal.SIGINT, self.SignalClose)

            self.MQTTclient.loop_start()
        except Exception as e1:
            self.LogErrorLine("Error in MyMQTT init: " + str(e1))
            self.console.error("Error in MyMQTT init: " + str(e1))
            sys.exit(1)

    # ------------ MyMQTT::AppendRoot---------------------------------------
    def AppendRoot(self, name):
        if self.TopicRoot != None and len(self.TopicRoot):
            ReturnPath = self.TopicRoot + "/" + str(name)
        else:
            ReturnPath = str(name)
        return ReturnPath

    # ------------ MyMQTT::PublishCallback---------------------------------------
    # Callback to publish data via MQTT
    def PublishCallback(self, name, value):

        try:
            FullPath = self.AppendRoot(name)

            if self.RemoveSpaces:
                FullPath = FullPath.replace(" ", "_")

            if self.debug:
                self.LogDebug(
                    "Publish:  "
                    + FullPath
                    + ": "
                    + str(value)
                    + ": "
                    + str(type(value))
                )

            self.MQTTclient.publish(FullPath, value, retain=self.Retain)
        except Exception as e1:
            self.LogErrorLine("Error in MyMQTT:PublishCallback: " + str(e1))

    # ------------ MyMQTT::on_disconnect-----------------------------------------
    def on_disconnect(self, client, userdata, rc=0):

        self.LogInfo(
            "Disconnected from " + self.MQTTAddress + " result code: " + str(rc)
        )
        self.MQTTclient.publish(self.LastWillTopic, payload=OFFLINE_PAYLOAD, retain=True)

    # ------------ MyMQTT::on_connect--------------------------------------------
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):

        try:
            if rc != 0:
                self.LogError(
                    "Error connecting to MQTT server: return code: " + str(rc)
                )
            self.LogInfo(
                "Connected to " + self.MQTTAddress + " result code: " + str(rc)
            )

            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            FullPath = self.AppendRoot("generator")
            self.MQTTclient.subscribe(FullPath + "/#")

            # Setup Last Will value
            self.MQTTclient.publish(self.LastWillTopic, payload=ONLINE_PAYLOAD, retain=True)

        except Exception as e1:
            self.LogErrorLine("Error in MyMQTT:on_connect: " + str(e1))

    def SendHaDiscovery(self, current_keys):
        clean_sitename = _CleanString(self.SiteName)
        device_id = f"genmon_{clean_sitename}"

        components = {}
        for sub_topic, meta in HA_SENSOR_MAP.items():
            if sub_topic not in current_keys:
                continue
            component_key = _CleanString(sub_topic)
            component_config = {
                "p": "sensor",
                "uniq_id": f"{device_id}_{component_key}",
                "stat_t": self.AppendRoot(sub_topic),
            }
            if meta.pop("json", False):
                component_config["val_tpl"] = "{{ value_json.value }}"
            component_config.update(meta)
            components[component_key] = component_config

        payload = {
            "dev": {
                "ids": [device_id],
                "name": f"Genmon {self.SiteName}",
                "mf": "Genmon",
                "mdl": "Genmon",
                "sw": ProgramDefaults.GENMON_VERSION
            },
            "o": {
                "name": "Genmon MQTT Addon",
                "sw": ProgramDefaults.GENMON_VERSION,
                "url": "https://github.com/jgyates/genmon"
            },
            "avty_t": self.LastWillTopic,
            "pl_avail": ONLINE_PAYLOAD,
            "pl_not_avail": OFFLINE_PAYLOAD,
            "cmps": components
        }

        discovery_topic = f"homeassistant/device/{device_id}/config"

        try:
            json_payload = json.dumps(payload)
            self.MQTTclient.publish(discovery_topic, json_payload, retain=True)
            self.LogInfo(
                f"MQTT: Sent unified discovery payload ({len(json_payload)} bytes) "
                f"for {len(components)} entities to {discovery_topic}."
            )
        except Exception as e1:
            self.LogErrorLine("Error in MyMQTT:SendHaDiscovery: " + str(e1))

    # ------------ MyMQTT::on_message--------------------------------------------
    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, message):

        try:
            if self.debug:
                self.LogDebug(
                    "Confirmed: " + message.topic + ": " + str(message.payload)
                )
            # parse topic
            command = str(message.payload.decode("utf-8"))
            FullPath = self.AppendRoot("generator/command")

            if message.topic.lower() != (FullPath.lower()):
                return

            # write command
            if command != None and len(command):
                self.Push.SendCommand("generator: " + command)
                self.LogDebug("Command Sent: " + command)
        except Exception as e1:
            self.LogErrorLine("Error in MyMQTT:on_message: " + str(e1))

    # ----------MyMQTT::SignalClose---------------------------------------------
    def SignalClose(self, signum, frame):

        self.Close()
        sys.exit(1)

    # ---------- MyMQTT::Close--------------------------------------------------
    def Close(self):
        self.LogDebug("Exiting MyMQTT")
        self.Exiting = True
        self.Push.Close()
        self.MQTTclient.loop_stop(force=True)


# -------------------------------------------------------------------------------
if __name__ == "__main__":
    (
        console,
        ConfigFilePath,
        address,
        port,
        loglocation,
        log,
    ) = MySupport.SetupAddOnProgram("genmqtt")

    InstanceMQTT = MyMQTT(
        host=address,
        port=port,
        log=log,
        loglocation=loglocation,
        configfilepath=ConfigFilePath,
        console=console,
    )

    while not InstanceMQTT.Exiting:
        time.sleep(0.5)

    sys.exit(1)
