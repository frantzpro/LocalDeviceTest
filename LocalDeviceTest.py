import signal
import sys
import pickle
from threading import Thread
from time import sleep

from AbstractVirtualCapability import AbstractVirtualCapability, VirtualCapabilityServer, formatPrint


class LocalDeviceTest(AbstractVirtualCapability):

    def __init__(self, server):
        super().__init__(server)
        self.ergebnis = 0

    def Addition(self, params: dict) -> dict:
        #formatPrint(self, f"Sending TestFieldBountaries: {self.TestFieldBoundaries}")
        if params['int'] and params['SimpleIntParameter']:
            self.ergebnis = params['SimpleIntParameter'] + params['int']

        return {"int": self.ergebnis}

    def loop(self):
        pass


if __name__ == "__main__":
    try:
        port = None
        if len(sys.argv[1:]) > 0:
            port = int(sys.argv[1])
        server = VirtualCapabilityServer(port)
        tf = LocalDeviceTest(server)
        tf.start()
        while server.running:
            pass
        # Needed for properly closing, when program is being stopped wit a Keyboard Interrupt
    except KeyboardInterrupt:
        print("[Main] Received KeyboardInterrupt")
