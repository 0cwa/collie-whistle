#!/usr/bin/python3
"""Stand-in for UnifiedPush connector apps on the D-Bus session bus (runs in the KUnifiedPush container).

KUnifiedPush's distributor drops clients whose D-Bus service has left the bus, so the
registered apps must stay alive. This owns the service names given as arguments, answers
org.unifiedpush.Connector1, and logs what the distributor delivers: one line per call,
"Message <token> <sha256 of payload> <length>" or "NewEndpoint <token> <url>".
"""
import hashlib, sys
from gi.repository import Gio, GLib

XML = """<node><interface name='org.unifiedpush.Connector1'>
<method name='Message'><arg type='s' direction='in'/><arg type='ay' direction='in'/><arg type='s' direction='in'/></method>
<method name='NewEndpoint'><arg type='s' direction='in'/><arg type='s' direction='in'/></method>
<method name='Unregistered'><arg type='s' direction='in'/></method>
</interface></node>"""
LOG = open("/run/kup/connector.log", "a", buffering=1)


def call(conn, sender, path, iface, method, params, inv):
    if method == "Message":
        token, data, mid = params.unpack()
        LOG.write("Message %s %s %d\n" % (token, hashlib.sha256(bytes(data)).hexdigest(), len(data)))
    else:
        LOG.write("%s %s\n" % (method, " ".join(map(str, params.unpack()))))
    inv.return_value(None)


def on_bus(conn, name, node=Gio.DBusNodeInfo.new_for_xml(XML)):
    conn.register_object("/org/unifiedpush/Connector", node.interfaces[0], call, None, None)


loop = GLib.MainLoop()
for n in sys.argv[1:]:
    Gio.bus_own_name(Gio.BusType.SESSION, n, Gio.BusNameOwnerFlags.NONE, on_bus, None, None)
loop.run()
