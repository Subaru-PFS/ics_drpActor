"""Keep an actor's hub connections alive, and drop them when the link dies without either end noticing.

Two independent mechanisms:

- TCP keepalive, run by the kernel: probes an idle connection, which keeps firewall/NAT state on the path
  alive, and fails the socket when the peer stops answering, even while the reactor is busy.
- HubWatchdog, run by the reactor: pings the hub periodically and aborts the hub connection when a ping
  goes unanswered, which also covers a hub that still answers at the TCP level but has stopped serving us.

Either way the connection is reported lost, and actorcore reconnects and re-subscribes on its own.
"""

import socket
import time

import opscore.actor.keyvar as keyvar
from twisted.internet import task


def enableTcpKeepAlive(sock, idle=60, interval=10, count=6, userTimeout=120):
    """
    Enable kernel keepalive on a TCP socket.

    Set on a listening socket, the options are inherited by every connection it accepts.

    Parameters
    ----------
    sock : socket.socket
        Connected or listening TCP socket.
    idle : int
        Seconds without traffic before the first probe.
    interval : int
        Seconds between probes.
    count : int
        Unanswered probes before the connection is failed.
    userTimeout : int
        Seconds data may stay unacknowledged before the connection is failed.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, userTimeout * 1000)


class HubWatchdog:
    """
    Ping the hub every `period` seconds, and abort the hub connection when the previous ping is unanswered.

    A ping is judged at the next tick, and only when that tick fires on time: a late tick means the reactor
    was busy, in which case the reply may still be waiting unread on the socket.

    Parameters
    ----------
    actor : actorcore.Actor
        Actor whose hub connection (actor.cmdr) is watched.
    period : float
        Seconds between pings.
    """

    def __init__(self, actor, period=120):
        self.actor = actor
        self.period = period
        self.pending = None
        self.lastTick = None
        self.loop = task.LoopingCall(self.tick)

    def start(self):
        """Start pinging, if not already started."""
        if not self.loop.running:
            self.loop.start(self.period, now=False)

    def tick(self):
        """Judge the previous ping, then send the next one."""
        now = time.monotonic()
        onTime = self.lastTick is not None and now - self.lastTick < 1.5 * self.period
        self.lastTick = now

        if self.pending is not None and not self.pending.isDone and onTime:
            self.actor.logger.warning(f'hub ping unanswered after {self.period}s, aborting the hub connection')
            self.abort()
            return

        self.ping()

    def ping(self):
        """Send a ping to the hub, when connected."""
        if not self.actor.cmdr.connector.isConnected():
            self.pending = None
            return

        self.pending = keyvar.CmdVar(actor='hub', cmdStr='ping', timeLim=0)
        self.actor.cmdr.dispatcher.executeCmd(self.pending)

    def abort(self):
        """Abort the hub connection, so that actorcore reconnects."""
        self.pending = None
        connection = self.actor.cmdr.connector.activeConnection

        if connection is not None:
            connection.transport.abortConnection()


class HubLink:
    """
    Keepalive on both hub connections, and a hub watchdog, for one actor.

    Parameters
    ----------
    actor : actorcore.Actor
        Actor whose hub connections are kept alive.
    keepAlive : dict, optional
        Keyword arguments of enableTcpKeepAlive.
    pingPeriod : float
        Seconds between hub pings, see HubWatchdog.
    """

    def __init__(self, actor, keepAlive=None, pingPeriod=120):
        self.actor = actor
        self.keepAlive = keepAlive if keepAlive is not None else {}
        self.watchdog = HubWatchdog(actor, period=pingPeriod)

    def connectionMade(self):
        """Enable keepalive on the new hub connection and on the command port, and start the watchdog."""
        enableTcpKeepAlive(self.actor.cmdr.connector.activeConnection.transport.getHandle(), **self.keepAlive)
        enableTcpKeepAlive(self.actor.commandSources.listeningPort.socket, **self.keepAlive)
        self.watchdog.start()
