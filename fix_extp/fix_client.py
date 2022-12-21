# Ex Trading Pro Samples
# FIX Client sample.
#

import quickfix as fix
import quickfix44 as fix44

import time
import sys
import queue



__SOH__ = "\x01"


class FixClient(fix.Application):
    """ Defines a FIX application that initiates a connection to a FIX server.
    For further details, see the QuickFix Documentation.
    """
    md_inqueue = queue.Queue()
    md_outqueue = queue.Queue()
    md_session = None
    ma_session = None
    ma_logged_in = False
    md_logged_in = False
    
    

    def onCreate(self, sessionID):
        """Event when a session has been created."""
        print("onCreate : Session (%s)" % sessionID.toString())
        if sessionID.toString().endswith("md"):
            self.md_session = sessionID
        else:
            self.ma_session = sessionID

    def onLogon(self, sessionID):
        """Event called when a session has logged on successfuly."""
        print("Successful Logon to session '%s'." % sessionID.toString())
        if sessionID.toString().endswith("md"):
            self.md_logged_in = True
        else:
            self.ma_logged_in = True

    def onLogout(self, sessionID):
        """Event called when a session has logged out."""
        print("Session (%s) logout !" % sessionID.toString())
        if sessionID.toString().endswith("md"):
            self.md_logged_in = False
        else:
            self.ma_logged_in = False

    def toAdmin(self, message, sessionID):
        """A message is about to be sent to the server (Admin level)."""
        print("Session (%s) toAdmin !" % sessionID.toString())
        print('message =', message)
        
    def toApp(self, message, sessionID):
        """A message is about to be sent to the server (App Level)"""
        print("Session (%s) toApp !" % sessionID.toString())
        print('message =', message)

    def fromAdmin(self, message, sessionID):
        """Message received from the server (admin level)."""
        print("Session (%s) fromAdmin !" % sessionID.toString())
        print('message =', message)

    def fromApp(self, message, sessionID):
        """Message received from the server (app level)."""
        print("R << fromApp: {}".format(message.replace(__SOH__, "^")))
        
              
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        msgTypeValue = msgType.getValue()     
        if msgTypeValue == "W":
            self.onQuote(message)
        else:
            print("** Unhandled message type: {}".format(msgTypeValue))

    def onQuote(self, sessionID, message):
        """A quote has been received, lets publish it to the queue."""
        symTag = fix.Symbol()
        md = fix44.MarketDataRequest.NoMDEntries()
        message.getField(md)
        message.getValue(symTag)
        symbol = szmTag.getValue()
        mdCount = md.getValue()
        data = {"symbol": symbol, "quotes": []}
        idx = 1
        while idx != mdCount:
            grp = fix.NoMDEntries()
            message.getGroup(idx, grp)
            pxField = fix.MDEntryPx()
            grp.getValue(pxField)
            sizeField = fix.MDEntrySize()
            grp.getValue(sizeField)
            typeField = fix.MDEntryType()
            grp.getValue(typeField)
            data["quotes"].append({"price": pxField.getValue(),
                                   "size": sizeField.getValue(),
                                   "type": typeField.getValue()})
            
        self.md_outqueue.put(data)

def subscribe(product):
    """Subscribe to the given product"""
    global fix_client
    global initiator

    msg = fix44.MarketDataRequest()
    symTag = fix.Symbol()
    symTag.setValue(product)
    upTypeTag = fix.MDUpdateType()
    upTypeTag.setValue(fix.MDUpdateType_FULL_REFRESH)
    msg.setField(upTypeTag)
    
    mdReqIdTag = fix.MDReqID()
    mdReqIdTag.setValue(str(time.time()))
    msg.setField(mdReqIdTag)
                            
    nrSyms = fix44.MarketDataRequest.NoRelatedSym()
    nrSyms.setField(symTag)
    msg.addGroup(nrSyms)
    initiator.getSession(fix_client.ma_session).send(msg)

initiator = None
fix_client = None
def main():
    global initiator
    global fix_client
    
    if len(sys.argv) == 1:
        print("Usage: fix_client <path_to_fix_config.cfg>")
        return 1
    try:
        settings = fix.SessionSettings(sys.argv[1])
        fix_client = FixClient()
        storeFactory = fix.FileStoreFactory(settings)
        logFactory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(
            fix_client, storeFactory, settings, logFactory)
        initiator.start()
        i = 0
        while fix_client.md_logged_in is False and i < 10:
            time.sleep(1)
            i += 1
        if i == 10:
            print("Failed to log on")
            initiator.stop()
            return 1
        subscribe("BTC-USD")
        while True:
            time.sleep(0.1)
            data = None
            try:
                data = fix_client.md_outqueue.get_nowait()
            except queue.Empty:
                pass
            if data is not None:
                print("<< quote: %s" %(data))

        initiator.stop()
        return 0
    except fix.ConfigError as ex:
        print("Error initializing FiX client: {}".format(ex))
        return 1

if __name__ == '__main__':
    sys.exit(main())

        
