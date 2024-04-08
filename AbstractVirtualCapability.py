import asyncio
import json
import socket
import struct
import sys
import traceback
from abc import abstractmethod
from threading import Thread, Timer
from time import sleep, time


def formatPrint(c, string: str) -> None:
    """Prints the given string in a formatted way: [Classname] given string

    Parameters:
        c       (class)  : The class which should be written inside the braces
        string  (str)    : The String to be printed
    """
    # sys.stderr.write(f"DOCKER\t\t[{type(c).__name__}] {string}\n")
    try:
        import rospy
        rospy.logwarn(f"DOCKER\t\t[{type(c).__name__}] {string}\n")
    except:
        print(f"DOCKER\t\t[{type(c).__name__}] {string}\n")


class NumpyArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SubDeviceRepresentation):
            return obj.__str__()
        try:
            import numpy as np
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return super(NumpyArrayEncoder, self).default(obj)
        except Exception as e:
            formatPrint(self, f"Not possible to import numpy... {repr(e)}")
            return obj


class VirtualCapabilityServer(Thread):
    '''Server meant to be run inside a docker container as a Thread.

    '''

    def __init__(self, port: int = None, ip: str = "0.0.0.0"):
        super().__init__()

        if port is not None:
            self.connectionPort = port
        else:
            self.connectionPort = 9999
        self.server = None
        self.ip = ip
        self.virtualDevice = None
        self.running = False
        self.connected = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.connectionPort))
        self.sock: socket = None
        self.start()

    def run(self) -> None:
        formatPrint(self, "[Server]Server running on Address: {}".format((self.ip, self.connectionPort)))
        self.socket.listen(1)
        self.running = True
        while self.running:
            try:
                formatPrint(self, "[Server] Connecting...")
                self.sock, _ = self.socket.accept()
            except Exception as e:
                formatPrint(self, f"[Server] Error connecting to {self.ip}@{self.connectionPort}. {repr(e)}")
                self.socket.listen(1)
                continue

            self.connected = True
            while self.connected:
                try:
                    msg = self.recv_msg()
                    if msg is not None:
                        self.virtualDevice.command_list += [dict(json.loads(msg))]
                except Exception as e:
                    formatPrint(self, e)
                    self.connected = False
                sleep(0.0001)
        self.kill()

    def recv_msg(self):
        def recvall(n):
            # Helper function to recv n bytes or return None if EOF is hit
            data = bytearray()
            while len(data) < n:
                packet = self.sock.recv(n - len(data))
                if not packet:
                    return None
                data.extend(packet)
            return data

        # Read message length and unpack it into an integer
        raw_msglen = recvall(4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        return recvall(msglen)

    def send_msg(self, msg):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        self.sock.sendall(msg)

    def addVirtualCapability(self, vc):
        self.virtualDevice = vc

    def kill(self):
        self.connected = False
        self.running = False
        if self.server:
            self.server.close()
            asyncio.run(self.server.wait_closed())
        self.virtualDevice.kill()


class RepeatedTimer(object):
    def __init__(self, interval, function, args, kwargs):
        self._timer = None
        self.interval = interval
        self.callback = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        # formatPrint(self, *self.args)
        self.callback(self.args)  # , **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


class AbstractVirtualCapability(Thread):
    def __init__(self, server: VirtualCapabilityServer):
        Thread.__init__(self)
        self.uri = None
        self.dynamix = {}
        self.dev_name = None
        self.server = server
        self.timer_list = {}
        self.command_list = []
        # a list with sub_capabilities invoked by @invoke_sync and @invoke_async
        self.sub_cap_server_callback = {}
        self.running = True
        self.sub_caps_running = True
        self.sub_devices = dict()

    def run(self) -> None:
        self.server.addVirtualCapability(self)
        current_thread: Thread = Thread()
        while self.running:
            if len(self.command_list) > 0:
                # formatPrint(self, f"Go my Command: {self.command_list[-1]}")
                Thread(target=self.__handle_command, args=(dict(self.command_list.pop()),), daemon=True).start()

            if not current_thread.is_alive():
                current_thread = Thread(target=self.loop, daemon=True)
                current_thread.start()
            sleep(.0001)

    def __handle_command(self, command: dict):
        # formatPrint(self, f"Got Command {command}")
        cap = command["capability"] if "capability" in command else None
        par = command["parameters"] if "parameters" in command else None

        if cap is not None:
            formatPrint(self, f"Handling Command {cap} with params:\t {par}")
        else:
            formatPrint(self, f"Handling Command: {command}")

        if command["type"] == "trigger":
            ret = {"type": "response", "capability": command["capability"],
                   "device": command["device"] if "device" in command else self.uri}

            if "src" in command:
                ret["src"] = command["src"]

            if "streaming" in command.keys() and command["streaming"] != 0.:
                ret["streaming"] = command["streaming"]
                if command["capability"] in self.timer_list.keys():
                    self.timer_list[command["capability"]].stop()
                # pop because of recursive function handling
                command.pop("streaming")
                if ret["streaming"] > 0:
                    self.timer_list[command["capability"]] = RepeatedTimer(1. / ret["streaming"], self.__handle_command,
                                                                           command, None)
                    formatPrint(self, f"Streaming now {command}")
                elif ret["streaming"] < 0:
                    try:
                        self.timer_list[command["capability"]].stop()
                        formatPrint(self, f"Streaming ended {command}")
                    except:
                        pass
            try:
                ret["parameters"] = self.__getattribute__(command["capability"])(command["parameters"])
            except AttributeError as ae:
                error_string = "Capability {} not implemented!".format(command["capability"])
                error_string = error_string.replace("\"", "").replace("\\", "")
                ret["error"] = error_string
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                formatPrint(self, error_string)
            except KeyError as e:
                error_string = "Parameter not provided for Capability {} -> {}".format(command["capability"], repr(e))
                error_string = error_string.replace("\"", "").replace("\\", "")
                ret["error"] = error_string
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                formatPrint(self, error_string)
            except Exception as e:
                error_string = "Some Error occured in function {}: {}".format(
                    command["capability"], repr(e))
                error_string = error_string.replace("\"", "").replace("\\", "")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                formatPrint(self, error_string)
                ret["error"] = error_string

            self.send_message(ret)
        elif command["type"] == "response":
            self.sub_cap_server_callback[command["src"]] = command["parameters"]
        elif command["type"] == "device":
            self.sub_cap_server_callback[command["src"]] = command
        else:
            raise KeyError(f"Command {command} not found !!!")
        # This could be triggered outside by Timer
        if self.command_list.count(command) > 0:
            self.command_list.remove(command)

    @abstractmethod
    def loop(self):
        raise NotImplementedError

    def send_message(self, command: dict):
        if "capability" in command:
            cap = command["capability"]
        else:
            cap = "NO_CAPABILITY"
        if command["type"] == "trigger":
            formatPrint(self, f"Triggering Sub-Capability: {cap}")
        elif command["type"] == "response":
            formatPrint(self, f"Capability {cap} successful, sending {command}")
        elif command["type"] == "blueprint":
            formatPrint(self, f"Requesting Device {command}")
        try:
            self.server.send_msg(json.dumps(command, cls=NumpyArrayEncoder).encode("UTF-8"))
            # formatPrint(self, f"Sent Command {command}")
        except Exception as e:
            error_string = "Some Error occured while sending. Cause: function {}: {}".format(
                command["capability"], repr(e))
            error_string = error_string.replace("\"", "").replace("\\", "")
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            formatPrint(self, error_string)
            command["error"] = error_string
            command["parameters"] = {}
            self.send_message(command)

    def kill(self):
        self.running = False
        for timer in self.timer_list:
            self.timer_list[timer].stop()

    def invoke_sync(self, capability: str, params: dict) -> dict:
        """Invokes a subcap synchrony

        :param capability: the uri of the subcapability
        :param params: the parameter of the subcapability
        :return: the result of this query
        """
        execute_sub_cap_command = dict()
        src = f"{self.uri}-{capability}-{time()}"
        execute_sub_cap_command["type"] = "trigger"
        execute_sub_cap_command["src"] = src
        execute_sub_cap_command["capability"] = capability
        execute_sub_cap_command["parameters"] = params
        self.sub_cap_server_callback[src] = None
        self.send_message(execute_sub_cap_command)
        while self.sub_cap_server_callback[src] == None and self.sub_caps_running:
            sleep(.005)
            # print(f"having some Trouble... {self.sub_cap_server_callback}")
        ret = {}
        if self.sub_caps_running:
            ret = self.sub_cap_server_callback[src]
        self.sub_cap_server_callback.pop(src)
        return ret

    def invoke_async(self, capability: str, params: dict, callback) -> Thread:
        """Invokes a subcap async
        :param capability: the uri of the subcapability
        :param params: the parameter of the subcapability
        :param callback: function to be called when subcap arrives, takes a dict as parameter
        :return: None
        """
        src = f"{self.uri}-{capability}-{time()}"

        def __wait_for_callback(callback):
            while self.sub_cap_server_callback[src] == None and self.sub_caps_running:
                sleep(.005)
                # print(f"having some Trouble... 2 {self.sub_cap_server_callback}")
                pass
            if self.sub_caps_running:
                callback(dict(self.sub_cap_server_callback[src]))
            self.sub_cap_server_callback.pop(src)

        src = f"{self.uri}-{capability}-{time()}"
        execute_sub_cap_command = dict()
        execute_sub_cap_command["type"] = "trigger"
        execute_sub_cap_command["src"] = src
        execute_sub_cap_command["capability"] = capability
        execute_sub_cap_command["parameters"] = params
        self.sub_cap_server_callback[src] = None
        self.send_message(execute_sub_cap_command)
        t = Thread(target=__wait_for_callback, args=(callback,))
        t.start()
        return t

    def query_sync(self, device: str, ood_id: int = None):
        """Invokes a subcap synchrony

        :param device: the uri of the device to query
        :param ood_id: for positive numbers the device will be queried, for -1 a new device will be instantiated
        :return: the result of this query
        """
        execute_query_device = dict()
        src = f"{self.uri}-{device}-{time()}"
        execute_query_device["type"] = "blueprint"
        execute_query_device["src"] = src
        if device in self.sub_devices:
            self.sub_devices[device] += 1
        else:
            self.sub_devices[device] = 0
        if ood_id is not None:
            execute_query_device["id"] = ood_id
            self.sub_devices[device] = ood_id
        else:
            execute_query_device["id"] = self.sub_devices[device]
        execute_query_device["dev_props"] = {"device": [device], "capabilities": []}
        self.sub_cap_server_callback[src] = None
        self.send_message(execute_query_device)
        while self.sub_cap_server_callback[src] == None and self.sub_caps_running:
            sleep(.005)
            # print(f"having some Trouble... {self.sub_cap_server_callback}")
        ret = {}
        if self.sub_caps_running:
            ret = self.sub_cap_server_callback[src]
        self.sub_cap_server_callback.pop(src)
        sub_dev = SubDeviceRepresentation(ret["json"], self, ret["id"])
        return sub_dev

    def cancel_sub_caps(self):
        """
        Cancels the waiting on current running subcaps.
        Waits 1 second until
        """
        self.sub_caps_running = False
        formatPrint(self, "Canceling Subcaps....")
        sleep(0.01)
        self.sub_caps_running = True


class SubDeviceRepresentation(object):
    def __init__(self, json: dict, master: AbstractVirtualCapability, ood_id: int = None):
        self.json = json
        self.master = master
        self.ood_id = ood_id
        if "id" in json:
            self.ood_id = self.json["id"]
        if ood_id is not None:
            self.json["id"] = ood_id

    def __str__(self):
        return self.json

    def get_device_property(self, prop_name: str):
        for p in self.json["properties"]:
            if p["name"] == prop_name:
                return p["value"]["object"]
        raise KeyError("No such property: " + prop_name)

    def invoke_sync(self, cap: str, params: dict):
        src = f"{self.master.uri}-{cap}-{time()}"
        d = {"capability": cap, "device": self.get_device_property("name") + f"@{self.ood_id}",
             "parameters": params if params != None else [], "src": src, "type": "trigger"}
        self.master.sub_cap_server_callback[d["src"]] = None
        self.master.send_message(d)
        while self.master.sub_cap_server_callback[src] == None and self.master.sub_caps_running:
            sleep(.005)
            # print(f"having some Trouble... {self.sub_cap_server_callback}")
        ret = {}
        if self.master.sub_caps_running:
            ret = self.master.sub_cap_server_callback[src]
        self.master.sub_cap_server_callback.pop(src)
        return ret

    def invoke_async(self, cap: str, params: dict, callback):
        src = f"{self.master.uri}-{cap}-{time()}"
        if params is None:
            parameters = {}
        else:
            parameters = params

        d = {"capability": cap, "device": self.get_device_property("name") + f"@{self.ood_id}",
             "parameters": parameters, "src": src, "type": "trigger"}

        self.master.sub_cap_server_callback[src] = None

        def __wait_for_callback(cb):
            while self.master.sub_cap_server_callback[src] == None and self.master.sub_caps_running:
                sleep(.005)
                # print(f"having some Trouble... {self.sub_cap_server_callback}")
            ret = {}
            if self.master.sub_caps_running:
                ret = self.master.sub_cap_server_callback[src]
                cb(dict(ret))
            self.master.sub_cap_server_callback.pop(src)

        self.master.send_message(d)
        t = Thread(target=__wait_for_callback, args=(callback,))
        t.start()
        return t
