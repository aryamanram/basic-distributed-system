import argparse
import json
import random
import socket
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional

CONTROL_PORT = 9090 #p port to listen for user commands
MEMBERSHIP_PORT = 8080

INTRODUCER_HOST = "fa25-cs425-a701.cs.illinois.edu" # fixed introducer

GOSSIP_INTERVAL = 0.5 # time between each gossip heartbeat
PINGACK_INTERVAL = 0.5 # time between each ping
FAILURE_TIMEOUT = 1.5 # time before makring node as suspicious/failed
SUSPICION_TIMEOUT = 2.0 # time before marking node as failed
CLEANUP_TIMEOUT = 2.0 # time before cleaning up failed node

class MemberStatus(Enum):
    JOINING = "JOINING"
    ACTIVE = "ACTIVE"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    LEFT = "LEFT"

class ProtocolType(Enum):
    GOSSIP = "GOSSIP"
    PINGACK = "PINGACK"

# logger for each node's log file
class Logger:
    def __init__(self, vm_id: int):
        self.vm_id = vm_id
        self.lock = threading.Lock()
        self.log_file = f"machine.{vm_id}.log"

    def log(self, msg: str):
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}"
        with self.lock:
            print(line)
            try:
                with open(self.log_file, "a") as f:
                    f.write(line + "\n")
            except Exception:
                pass

# data structure to keep track of current membership list at a node
class MembershipList:
    def __init__(self, hostname: str, member_id: str, logger: Logger, service):
        self.lock = threading.RLock()
        self.members = {}
        self.hostname = hostname
        self.member_id = member_id
        self.logger = logger
        self.service = service

        self.members[hostname] = {
            "id": member_id,
            "hostname": hostname,
            "heartbeat": 0,
            "timestamp": time.time(),
            "status": MemberStatus.JOINING.value,
            "incarnation": 0,     # start at 0
        }

    def get_members(self):
        with self.lock:
            return self.members.copy()

    def get_self(self):
        return self.member_id

    def increment_self_heartbeat(self):
        with self.lock:
            m = self.members[self.hostname]
            m["heartbeat"] += 1
            m["timestamp"] = time.time()

    def increment_self_incarnation(self):
        with self.lock:
            m = self.members[self.hostname]
            m["incarnation"] += 1
            m["timestamp"] = time.time()
            m["status"] = MemberStatus.ACTIVE.value
            self.logger.log(f"INCARNATION: Bumped to {m['incarnation']} for {self.hostname}")

    def update_member_from_remote(self, hostname: str, data: dict):
        with self.lock:
            now = time.time()
            remote_inc = data.get("incarnation", 0)
            remote_hb = data.get("heartbeat", 0)
            remote_status = data.get("status", MemberStatus.ACTIVE.value)

            if hostname not in self.members:
                self.members[hostname] = {
                    "id": data.get("id", f"unknown_{hostname}"),
                    "hostname": hostname,
                    "heartbeat": remote_hb,
                    "timestamp": now,
                    "status": remote_status,
                    "incarnation": remote_inc,
                }
                return True

            local = self.members[hostname]
            updated = False

            if remote_inc > local["incarnation"]:
                local.update({
                    "heartbeat": remote_hb,
                    "status": remote_status,
                    "timestamp": now,
                    "incarnation": remote_inc,
                })
                updated = True

            elif remote_inc == local["incarnation"] and remote_hb > local["heartbeat"]:
                local["heartbeat"] = remote_hb
                local["status"] = remote_status
                local["timestamp"] = now
                updated = True

            return updated

    def mark_active(self, hostname: str):
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                m["timestamp"] = time.time()
                if m["status"] != MemberStatus.LEFT.value:
                    m["status"] = MemberStatus.ACTIVE.value

    def mark_suspected(self, hostname: str, source: str):
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                if m["status"] not in (MemberStatus.FAILED.value, MemberStatus.LEFT.value):
                    m["status"] = MemberStatus.SUSPECTED.value
                    m["timestamp"] = time.time()
                    self.logger.log(f"SUSPECTED: {hostname} (by {source})")
                    # Possible false positive if node later proves active
                    self.service.false_positives += 1

    def mark_failed(self, hostname: str):
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                elapsed = time.time() - m["timestamp"]
                m["status"] = MemberStatus.FAILED.value
                m["timestamp"] = time.time()
                self.logger.log(f"DETECTED FAILURE {hostname} after {elapsed:.3f}s since last heartbeat")

    def mark_left(self, hostname: str):
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                m["status"] = MemberStatus.LEFT.value
                m["timestamp"] = time.time()

    def remove_member(self, hostname: str):
        with self.lock:
            if hostname in self.members:
                del self.members[hostname]

# for implementing pingack
class PingAckProtocol:
    def __init__(self, service):
        self.service = service
        self.running = False
        self.thread = None
        self.waiting_acks = {}

    def start(self):
        if self.running: return
        self.running = True
        self.thread = threading.Thread(target=self.ping_loop, daemon=True)
        self.thread.start()
        self.service.logger.log("PROTOCOL: Started PingAck")

    def stop(self):
        if not self.running: return
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.service.logger.log("PROTOCOL: Stopped PingAck")

    def ping_loop(self):
        while self.running:
            try:
                self.send_ping()
                self.check_timeouts()
                time.sleep(PINGACK_INTERVAL)
            except Exception as e:
                self.service.logger.log(f"ERROR: PingAck loop: {e}")

    def send_ping(self):
        members = self.service.membership.get_members()
        actives = [h for h, v in members.items()
                   if h != self.service.hostname and v["status"] == MemberStatus.ACTIVE.value]
        if not actives:
            return
        target = random.choice(actives)
        msg = {"type": "PING", "sender": self.service.hostname}
        try:
            self.service.sendto(json.dumps(msg).encode("utf-8"), (target, MEMBERSHIP_PORT))
            self.waiting_acks[target] = time.time()
            self.service.logger.log(f"PINGACK: Sent PING to {target}")
        except Exception as e:
            self.service.logger.log(f"ERROR: send PING to {target}: {e}")

    def handle_ping(self, msg, addr):
        reply = {"type": "ACK", "sender": self.service.hostname}
        try:
            self.service.sendto(json.dumps(reply).encode("utf-8"), addr)
            self.service.logger.log(f"PINGACK: Received PING from {msg.get('sender')}, sent ACK")
        except Exception as e:
            self.service.logger.log(f"ERROR: send ACK: {e}")

    def handle_ack(self, msg):
        sender = msg.get("sender")
        if sender in self.waiting_acks:
            del self.waiting_acks[sender]
        self.service.membership.mark_active(sender)
        self.service.logger.log(f"PINGACK: Received ACK from {sender}")

    def check_timeouts(self):
        now = time.time()
        expired = [h for h, t0 in self.waiting_acks.items() if now - t0 > FAILURE_TIMEOUT]
        for host in expired:
            del self.waiting_acks[host]
            if self.service.suspicion_enabled:
                self.service.membership.mark_suspected(host, source=f"{self.service.hostname}/ping")
                threading.Timer(SUSPICION_TIMEOUT, self.suspect_to_fail, args=[host]).start()
            else:
                self.service.membership.mark_failed(host)
                self.service.membership.remove_member(host)
                self.service.logger.log(f"PINGACK: FAILED {host} (removed, suspicion OFF)")

    def suspect_to_fail(self, host):
        mem = self.service.membership.get_members()
        if host in mem and mem[host]["status"] == MemberStatus.SUSPECTED.value:
            self.service.membership.mark_failed(host)
            self.service.membership.remove_member(host)
            self.service.logger.log(f"PINGACK: Escalated {host} to FAILED (removed)")

# for implementing gossip
class GossipProtocol:
    def __init__(self, service):
        self.service = service
        self.running = False
        self.thread = None

    def start(self):
        if self.running: return
        self.running = True
        self.thread = threading.Thread(target=self.gossip_loop, daemon=True)
        self.thread.start()
        self.service.logger.log("PROTOCOL: Started Gossip")

    def stop(self):
        if not self.running: return
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.service.logger.log("PROTOCOL: Stopped Gossip")

    def gossip_loop(self):
        while self.running:
            try:
                self.service.membership.increment_self_heartbeat()
                self.send_gossip()
                self.check_suspected()
                time.sleep(GOSSIP_INTERVAL)
            except Exception as e:
                self.service.logger.log(f"ERROR: Gossip loop: {e}")

    def send_gossip(self):
        members = {
            h: m for h, m in self.service.membership.get_members().items()
            if not (m["status"] in (MemberStatus.FAILED.value, MemberStatus.LEFT.value)
                    and (time.time() - m["timestamp"]) > CLEANUP_TIMEOUT)
        }
        peers = [h for h, v in members.items()
                 if h != self.service.hostname and v["status"] != MemberStatus.LEFT.value]
        if not peers:
            return
        k = min(len(peers), random.randint(1, 3))
        targets = random.sample(peers, k)

        msg = {
            "type": "GOSSIP",
            "sender": self.service.hostname,
            "members": members,
            "protocol": self.service.current_protocol.value,
            "suspicion": self.service.suspicion_enabled,
        }
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                self.service.sendto(data, (t, MEMBERSHIP_PORT))
            except Exception as e:
                self.service.logger.log(f"ERROR: Gossip send to {t}: {e}")

    def handle_gossip(self, msg):
        remote_members = msg.get("members", {})
        for host, m in remote_members.items():
            if host == self.service.hostname:
                continue
            updated = self.service.membership.update_member_from_remote(host, m)
            if updated and m.get("status") == MemberStatus.ACTIVE.value:
                self.service.membership.mark_active(host)

        try:
            r_proto = ProtocolType(msg.get("protocol", self.service.current_protocol.value))
            r_susp = bool(msg.get("suspicion", self.service.suspicion_enabled))
            if (r_proto != self.service.current_protocol) or (r_susp != self.service.suspicion_enabled):
                self.service.switch(r_proto, r_susp, rebroadcast=False, originator=None)
                self.service.logger.log(
                    f"GOSSIP: Adopted protocol ({r_proto.value.lower()}, {'suspect' if r_susp else 'nosuspect'})"
                )
        except Exception:
            pass

    def check_suspected(self):
        now = time.time()
        members = self.service.membership.get_members()
        for host, m in members.items():
            if host == self.service.hostname:
                continue
            if m["status"] == MemberStatus.LEFT.value:
                continue
            age = now - m["timestamp"]
            if m["status"] == MemberStatus.ACTIVE.value and age > FAILURE_TIMEOUT:
                if self.service.suspicion_enabled:
                    self.service.membership.mark_suspected(host, source=f"{self.service.hostname}/gossip")
                else:
                    self.service.membership.mark_failed(host)
                    self.service.logger.log(f"GOSSIP: FAILED {host} (timeout, suspicion OFF)")
            elif m["status"] == MemberStatus.SUSPECTED.value and age > CLEANUP_TIMEOUT:
                self.service.membership.mark_failed(host)
                self.service.logger.log(f"GOSSIP: Suspected {host} marked as FAILED")

# the main service to keep track of members and protocol
class MembershipService:
    def __init__(self, vm_id: int):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.member_id = f"{int(time.time())}_{self.hostname}_{MEMBERSHIP_PORT}"

        self.logger = Logger(vm_id)
        self.membership = MembershipList(self.hostname, self.member_id, self.logger, self)

        self.suspicion_enabled = False  # default
        self.current_protocol = ProtocolType.PINGACK

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", MEMBERSHIP_PORT))
        self.sock.settimeout(0.5)

        self.gossip = GossipProtocol(self)
        self.pingack = PingAckProtocol(self)

        self.running = False
        self.recv_thread = None
        self.monitor_thread = None
        self.control_thread = None

        self.drop_rate = 0.0
        self.bytes_sent = 0
        self.bytes_recv = 0
        self.false_positives = 0


        self.logger.log(f"INIT: Service on {self.hostname} (ID {self.member_id})")

    def sendto(self, data: bytes, addr): # socket wrapper to count bytes being sent and received
        try:
            sent = self.sock.sendto(data, addr)
            self.bytes_sent += sent
            return sent
        except Exception as e:
            self.logger.log(f"ERROR: sendto failed: {e}")
            return 0

    def start(self):
        self.running = True
        self.recv_thread = threading.Thread(target=self.receive_loop, daemon=True)
        self.recv_thread.start()
        self.monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.control_thread = threading.Thread(target=self.control_loop, daemon=True)
        self.control_thread.start()
        self.pingack.start()
        self.logger.log("SERVICE: Started")

    def stop(self):
        self.running = False
        try:
            self.gossip.stop()
            self.pingack.stop()
        except Exception:
            pass
        self.logger.log("SERVICE: Stopped")

    def switch(self, new_proto: ProtocolType, new_suspect_enabled: bool,
               rebroadcast: bool = True, originator: Optional[str] = None):
        if (self.current_protocol == new_proto and
            self.suspicion_enabled == new_suspect_enabled):
            return
        if self.current_protocol != new_proto:
            if self.current_protocol == ProtocolType.GOSSIP:
                self.gossip.stop()
            else:
                self.pingack.stop()
            self.current_protocol = new_proto
            if self.current_protocol == ProtocolType.GOSSIP:
                self.gossip.start()
            else:
                self.pingack.start()

        self.suspicion_enabled = new_suspect_enabled
        self.logger.log(f"PROTOCOL: Now ({self.current_protocol.value.lower()}, "
                        f"{'suspect' if self.suspicion_enabled else 'nosuspect'})")

        if rebroadcast:
            self.broadcast_switch(self.current_protocol, self.suspicion_enabled, originator or self.hostname)

    # huge broadcast, keep letting every node know about the protocol switch
    def broadcast_switch(self, proto: ProtocolType, susp: bool, originator: str):
        msg = {
            "type": "SWITCH",
            "sender": self.hostname,
            "protocol": proto.value,
            "suspicion": susp,
            "originator": originator,
        }
        members = self.membership.get_members()
        targets = [h for h, v in members.items()
                   if h != self.hostname and v["status"] != MemberStatus.LEFT.value]
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                self.sendto(data, (t, MEMBERSHIP_PORT))
            except Exception:
                pass

    def join_group(self):
        self.membership.mark_active(self.hostname)
        if self.hostname == INTRODUCER_HOST:
            self.logger.log("JOIN: This node is the introducer")
            return "Joined as introducer"

        msg = {
            "type": "JOIN",
            "sender": self.hostname,
            "member_data": self.membership.get_members()[self.hostname]
        }
        try:
            self.sendto(json.dumps(msg).encode("utf-8"), (INTRODUCER_HOST, MEMBERSHIP_PORT))
            self.logger.log(f"JOIN: Sent join to introducer {INTRODUCER_HOST}")
            return "Join sent"
        except Exception as e:
            self.logger.log(f"ERROR: join send: {e}")
            return f"Join error: {e}"

    def handle_join(self, message):
        sender = message.get("sender")
        data = message.get("member_data", {})
        self.membership.update_member_from_remote(sender, data)
        self.membership.mark_active(sender)
        self.logger.log(f"JOIN: {sender} joined")

        if self.hostname == INTRODUCER_HOST:
            members = self.membership.get_members()
            for h, v in members.items():
                if h in (self.hostname, sender):
                    continue
                if v["status"] == MemberStatus.LEFT.value:
                    continue
                fwd = {"type": "JOIN", "sender": sender, "member_data": data}
                try:
                    self.sendto(json.dumps(fwd).encode("utf-8"), (h, MEMBERSHIP_PORT))
                except Exception:
                    pass
            reply = {"type": "JOIN_REPLY", "members": members}
            try:
                self.sendto(json.dumps(reply).encode("utf-8"), (sender, MEMBERSHIP_PORT))
                self.logger.log(f"JOIN: Sent JOIN_REPLY to {sender}")
            except Exception as e:
                self.logger.log(f"ERROR: JOIN_REPLY: {e}")

    def handle_join_reply(self, message):
        remote = message.get("members", {})
        for host, m in remote.items():
            if host == self.hostname:
                continue
            self.membership.update_member_from_remote(host, m)
        self.logger.log("JOIN: Applied JOIN_REPLY membership")

    def leave_group(self):
        if self.current_protocol == ProtocolType.GOSSIP:
            self.gossip.stop()
        elif self.current_protocol == ProtocolType.PINGACK:
            self.pingack.stop()

        self.membership.mark_left(self.hostname)

        members = self.membership.get_members()
        targets = [h for h, v in members.items()
                if h != self.hostname and v["status"] != MemberStatus.LEFT.value]
        msg = {"type": "LEAVE", "sender": self.hostname}
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                self.sendto(data, (t, MEMBERSHIP_PORT))
            except Exception:
                pass

        self.logger.log("LEAVE: Voluntarily left the group")
        return "Left group"

    def handle_leave(self, message):
        sender = message.get("sender")
        self.membership.mark_failed(sender)
        self.logger.log(f"LEAVE: {sender} left")

    def receive_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                self.bytes_recv += len(data)

                message = json.loads(data.decode("utf-8"))

                t = message.get("type")
                if t == "PING":
                    if random.random() < self.drop_rate:
                        self.logger.log(f"DROP: Simulated drop of {t} from {message.get('sender')}")
                        continue
                    self.pingack.handle_ping(message, addr)
                elif t == "ACK":
                    if random.random() < self.drop_rate:
                        self.logger.log(f"DROP: Simulated drop of {t} from {message.get('sender')}")
                        continue
                    self.pingack.handle_ack(message)
                elif t == "GOSSIP":
                    if random.random() < self.drop_rate:
                        self.logger.log(f"DROP: Simulated drop of {t} from {message.get('sender')}")
                        continue
                    self.gossip.handle_gossip(message)
                elif t == "JOIN":
                    self.handle_join(message)
                elif t == "JOIN_REPLY":
                    self.handle_join_reply(message)
                elif t == "LEAVE":
                    self.handle_leave(message)
                elif t == "SWITCH":
                    proto = ProtocolType(message["protocol"])
                    susp = bool(message["suspicion"])
                    origin = message.get("originator")
                    self.switch(proto, susp, rebroadcast=True, originator=origin)
            except socket.timeout:
                continue
            except Exception as e:
                self.logger.log(f"ERROR: recv: {e}")

    def monitor_loop(self):
        while self.running:
            try:
                now = time.time()
                members = self.membership.get_members()
                for host, m in list(members.items()):
                    if host == self.hostname:
                        continue
                    if m["status"] == MemberStatus.FAILED.value and (now - m["timestamp"]) > CLEANUP_TIMEOUT:
                        self.membership.remove_member(host)
                        self.logger.log(f"MONITOR: Removed {host}")
                time.sleep(1.0)
            except Exception as e:
                self.logger.log(f"ERROR: monitor: {e}")

    def control_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("", CONTROL_PORT))
        srv.listen(8)
        self.logger.log(f"CONTROL: listening on {CONTROL_PORT}")
        while self.running:
            try:
                conn, _ = srv.accept()
                cmd = conn.recv(8192).decode().strip()
                resp = self.handle_command(cmd)
                conn.sendall(resp.encode())
                conn.close()
            except Exception as e:
                self.logger.log(f"ERROR: control: {e}")

    def handle_command(self, cmd: str) -> str:
        try:
            if cmd == "list_mem":
                return json.dumps(self.membership.get_members(), indent=2)

            if cmd == "list_self":
                return self.membership.get_self()

            if cmd == "display_suspects":
                mem = self.membership.get_members()
                suspects = [h for h, v in mem.items() if v["status"] == MemberStatus.SUSPECTED.value]
                return json.dumps(suspects)

            if cmd == "display_protocol":
                return f"({self.current_protocol.value.lower()}, {'suspect' if self.suspicion_enabled else 'nosuspect'})"

            if cmd == "join":
                return self.join_group()

            if cmd == "leave":
                return self.leave_group()

            if cmd.startswith("switch"):
                parts = cmd.split()
                if len(parts) != 3:
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"
                proto_str = parts[1].strip().lower()
                susp_str = parts[2].strip().lower()
                if proto_str not in ("gossip", "ping"):
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"
                if susp_str not in ("suspect", "nosuspect"):
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"

                new_proto = ProtocolType.GOSSIP if proto_str == "gossip" else ProtocolType.PINGACK
                new_susp = (susp_str == "suspect")
                self.switch(new_proto, new_susp, rebroadcast=True, originator=self.hostname)
                return f"Switched -> ({new_proto.value.lower()}, {'suspect' if new_susp else 'nosuspect'}) (broadcasted)"

            if cmd.startswith("drop_rate"):
                parts = cmd.split()
                if len(parts) != 2:
                    return "Usage: drop_rate <0.0-1.0>"
                try:
                    rate = float(parts[1])
                    if not (0.0 <= rate <= 1.0):
                        return "Rate must be between 0.0 and 1.0"
                    self.drop_rate = rate
                    return f"Drop rate set to {rate*100:.1f}%"
                except ValueError:
                    return "Invalid number"
                
            if cmd == "metrics":
                return json.dumps({
                    "bytes_sent": self.bytes_sent,
                    "bytes_recv": self.bytes_recv,
                    "false_positives": self.false_positives
                }, indent=2)

            return f"Unknown command: {cmd}"
        except Exception as e:
            return f"Error: {e}"

def main():
    ap = argparse.ArgumentParser(description="Membership Service")
    ap.add_argument("--vm-id", type=int, required=True, help="VM ID (1-10)")
    args = ap.parse_args()

    service = MembershipService(args.vm_id)
    service.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        service.stop()

if __name__ == "__main__":
    main()
