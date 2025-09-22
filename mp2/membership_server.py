import socket
import json
import threading
import time
import random
import sys
import argparse
from datetime import datetime
from collections import OrderedDict
from enum import Enum

# Configuration
MEMBERSHIP_PORT = 8080
INTRODUCER_HOST = "fa25-cs425-a701.cs.illinois.edu"  # VM1 as introducer
GOSSIP_INTERVAL = 0.5  # 500ms
SWIM_INTERVAL = 1.0    # 1 second
FAILURE_TIMEOUT = 4.0  # Mark as failed after 4 seconds
SUSPICION_TIMEOUT = 2.0  # Suspect after 2 seconds
CLEANUP_TIMEOUT = 8.0  # Remove from list after 8 seconds
SWIM_K = 3  # Number of indirect ping targets

class MemberStatus(Enum):
    """Member status in the group"""
    JOINING = "JOINING"
    ACTIVE = "ACTIVE"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    LEFT = "LEFT"

class ProtocolType(Enum):
    """Failure detection protocol type"""
    GOSSIP = "GOSSIP"
    SWIM = "SWIM"

class MembershipList:
    """Thread-safe membership list shared by both protocols"""
    
    def __init__(self, hostname, member_id):
        self.lock = threading.RLock()
        self.members = OrderedDict()
        self.hostname = hostname
        self.member_id = member_id
        
        # Add self to list
        self.members[hostname] = {
            'id': member_id,
            'hostname': hostname,
            'heartbeat': 0,
            'timestamp': time.time(),
            'status': MemberStatus.JOINING.value,
            'incarnation': 0
        }
    
    def update_member(self, hostname, data):
        """Update or add a member to the list"""
        with self.lock:
            current_time = time.time()
            
            if hostname not in self.members:
                # New member
                self.members[hostname] = data
                self.members[hostname]['timestamp'] = current_time
                return True
            
            # Update existing member
            member = self.members[hostname]
            
            # Handle incarnation numbers for suspicion
            if data.get('incarnation', 0) > member.get('incarnation', 0):
                self.members[hostname] = data
                self.members[hostname]['timestamp'] = current_time
                return True
            elif data.get('incarnation', 0) == member.get('incarnation', 0):
                # Same incarnation, check heartbeat
                if data.get('heartbeat', 0) > member.get('heartbeat', 0):
                    member['heartbeat'] = data['heartbeat']
                    member['timestamp'] = current_time
                    member['status'] = data.get('status', MemberStatus.ACTIVE.value)
                    return True
            
            return False
    
    def get_members(self, status=None):
        """Get list of members, optionally filtered by status"""
        with self.lock:
            if status:
                return {k: v for k, v in self.members.items() 
                       if v['status'] == status}
            return self.members.copy()
    
    def mark_suspected(self, hostname):
        """Mark a member as suspected"""
        with self.lock:
            if hostname in self.members:
                self.members[hostname]['status'] = MemberStatus.SUSPECTED.value
                self.members[hostname]['timestamp'] = time.time()
                return True
        return False
    
    def mark_failed(self, hostname):
        """Mark a member as failed"""
        with self.lock:
            if hostname in self.members:
                self.members[hostname]['status'] = MemberStatus.FAILED.value
                self.members[hostname]['timestamp'] = time.time()
                return True
        return False
    
    def remove_member(self, hostname):
        """Remove a member from the list"""
        with self.lock:
            if hostname in self.members:
                del self.members[hostname]
                return True
        return False
    
    def increment_heartbeat(self):
        """Increment self's heartbeat"""
        with self.lock:
            if self.hostname in self.members:
                self.members[self.hostname]['heartbeat'] += 1
                self.members[self.hostname]['timestamp'] = time.time()

class SuspicionManager:
    """Manages suspicion mechanism for both protocols"""
    
    def __init__(self, membership_list, logger):
        self.membership_list = membership_list
        self.logger = logger
        self.suspicion_timers = {}
        self.lock = threading.Lock()
    
    def suspect_member(self, hostname):
        """Start suspicion for a member"""
        with self.lock:
            if hostname not in self.suspicion_timers:
                self.membership_list.mark_suspected(hostname)
                self.logger.log(f"SUSPICION: Suspected {hostname}")
                
                # Start timer to mark as failed
                timer = threading.Timer(SUSPICION_TIMEOUT, 
                                      self._confirm_failure, args=[hostname])
                timer.start()
                self.suspicion_timers[hostname] = timer
    
    def clear_suspicion(self, hostname):
        """Clear suspicion for a member"""
        with self.lock:
            if hostname in self.suspicion_timers:
                self.suspicion_timers[hostname].cancel()
                del self.suspicion_timers[hostname]
                
                # Mark as active again
                members = self.membership_list.get_members()
                if hostname in members:
                    members[hostname]['status'] = MemberStatus.ACTIVE.value
                    self.membership_list.update_member(hostname, members[hostname])
                    self.logger.log(f"SUSPICION: Cleared suspicion for {hostname}")
    
    def _confirm_failure(self, hostname):
        """Confirm failure after suspicion timeout"""
        with self.lock:
            if hostname in self.suspicion_timers:
                del self.suspicion_timers[hostname]
                self.membership_list.mark_failed(hostname)
                self.logger.log(f"FAILURE: Confirmed failure of {hostname}")

class Logger:
    """Centralized logging for the membership service"""
    
    def __init__(self, vm_id):
        self.vm_id = vm_id
        self.log_file = f"machine.{vm_id}.log"
        self.lock = threading.Lock()
    
    def log(self, message):
        """Log a message with timestamp"""
        with self.lock:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            log_entry = f"[{timestamp}] {message}\n"
            
            # Print to console
            print(log_entry.strip())
            
            # Write to file
            with open(self.log_file, 'a') as f:
                f.write(log_entry)

class GossipProtocol:
    """Gossip-style failure detection protocol"""
    
    def __init__(self, membership_list, suspicion_mgr, logger, sock):
        self.membership_list = membership_list
        self.suspicion_mgr = suspicion_mgr
        self.logger = logger
        self.sock = sock
        self.running = False
        self.thread = None
        self.hostname = membership_list.hostname
    
    def start(self):
        """Start the gossip protocol"""
        self.running = True
        self.thread = threading.Thread(target=self._gossip_loop)
        self.thread.daemon = True
        self.thread.start()
        self.logger.log("PROTOCOL: Started Gossip protocol")
    
    def stop(self):
        """Stop the gossip protocol"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.logger.log("PROTOCOL: Stopped Gossip protocol")
    
    def _gossip_loop(self):
        """Main gossip loop"""
        while self.running:
            try:
                # Increment own heartbeat
                self.membership_list.increment_heartbeat()
                
                # Get active members
                members = self.membership_list.get_members()
                active_members = [h for h, m in members.items() 
                                if h != self.hostname and 
                                m['status'] == MemberStatus.ACTIVE.value]
                
                if active_members:
                    # Select random members to gossip to (min 2, max 3)
                    num_targets = min(len(active_members), random.randint(2, 3))
                    targets = random.sample(active_members, num_targets)
                    
                    # Send gossip message
                    message = {
                        'type': 'GOSSIP',
                        'sender': self.hostname,
                        'members': members
                    }
                    
                    for target in targets:
                        self._send_message(target, message)
                
                time.sleep(GOSSIP_INTERVAL)
                
            except Exception as e:
                self.logger.log(f"ERROR: Gossip loop error: {e}")
    
    def handle_gossip(self, data, addr):
        """Handle incoming gossip message"""
        sender = data.get('sender')
        remote_members = data.get('members', {})
        
        # Update membership list
        for hostname, member_data in remote_members.items():
            if hostname != self.hostname:
                updated = self.membership_list.update_member(hostname, member_data)
                if updated:
                    self.logger.log(f"GOSSIP: Updated {hostname} from {sender}")
                
                # Clear suspicion if we see higher heartbeat
                if member_data['status'] == MemberStatus.ACTIVE.value:
                    self.suspicion_mgr.clear_suspicion(hostname)
    
    def _send_message(self, target, message):
        """Send UDP message to target"""
        try:
            data = json.dumps(message).encode('utf-8')
            self.sock.sendto(data, (target, MEMBERSHIP_PORT))
        except Exception as e:
            self.logger.log(f"ERROR: Failed to send to {target}: {e}")

class SwimProtocol:
    """SWIM (PingAck) failure detection protocol"""
    
    def __init__(self, membership_list, suspicion_mgr, logger, sock):
        self.membership_list = membership_list
        self.suspicion_mgr = suspicion_mgr
        self.logger = logger
        self.sock = sock
        self.running = False
        self.thread = None
        self.hostname = membership_list.hostname
        self.ping_targets = []
        self.ping_index = 0
        self.waiting_acks = {}  # hostname -> timestamp
        self.indirect_ping_requests = {}  # target -> (original_target, timestamp)
    
    def start(self):
        """Start the SWIM protocol"""
        self.running = True
        self.thread = threading.Thread(target=self._swim_loop)
        self.thread.daemon = True
        self.thread.start()
        self.logger.log("PROTOCOL: Started SWIM protocol")
    
    def stop(self):
        """Stop the SWIM protocol"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.logger.log("PROTOCOL: Stopped SWIM protocol")
    
    def _swim_loop(self):
        """Main SWIM loop"""
        while self.running:
            try:
                # Get active members
                members = self.membership_list.get_members()
                active_members = [h for h, m in members.items() 
                                if h != self.hostname and 
                                m['status'] == MemberStatus.ACTIVE.value]
                
                if active_members:
                    # Update ping targets if membership changed
                    if set(active_members) != set(self.ping_targets):
                        self.ping_targets = active_members.copy()
                        random.shuffle(self.ping_targets)
                        self.ping_index = 0
                    
                    # Select next target
                    if self.ping_targets:
                        target = self.ping_targets[self.ping_index]
                        self.ping_index = (self.ping_index + 1) % len(self.ping_targets)
                        
                        # Send ping
                        self._send_ping(target)
                        
                        # Check for timeout after 1 second
                        threading.Timer(1.0, self._check_ping_timeout, args=[target]).start()
                
                time.sleep(SWIM_INTERVAL)
                
            except Exception as e:
                self.logger.log(f"ERROR: SWIM loop error: {e}")
    
    def _send_ping(self, target):
        """Send ping to target"""
        message = {
            'type': 'PING',
            'sender': self.hostname
        }
        self._send_message(target, message)
        self.waiting_acks[target] = time.time()
        self.logger.log(f"SWIM: Sent PING to {target}")
    
    def _check_ping_timeout(self, target):
        """Check if ping timed out and send indirect pings"""
        if target in self.waiting_acks:
            # Direct ping failed, try indirect
            del self.waiting_acks[target]
            
            # Get K random members for indirect ping
            members = self.membership_list.get_members()
            candidates = [h for h, m in members.items() 
                        if h != self.hostname and h != target and
                        m['status'] == MemberStatus.ACTIVE.value]
            
            if candidates:
                k_members = random.sample(candidates, min(SWIM_K, len(candidates)))
                
                for member in k_members:
                    message = {
                        'type': 'PING_REQ',
                        'sender': self.hostname,
                        'target': target
                    }
                    self._send_message(member, message)
                    self.indirect_ping_requests[member] = (target, time.time())
                
                self.logger.log(f"SWIM: Sent indirect PINGs for {target}")
                
                # Final timeout check after another second
                threading.Timer(1.0, self._final_timeout_check, args=[target]).start()
    
    def _final_timeout_check(self, target):
        """Final check before marking as suspected"""
        # Check if we received any response
        if target not in self.waiting_acks:
            # No response received, suspect the member
            self.suspicion_mgr.suspect_member(target)
            self.logger.log(f"SWIM: No response from {target}, suspecting")
    
    def handle_ping(self, data, addr):
        """Handle incoming ping"""
        sender = data.get('sender')
        
        # Send ACK
        message = {
            'type': 'ACK',
            'sender': self.hostname
        }
        self._send_message(sender, message)
        self.logger.log(f"SWIM: Received PING from {sender}, sent ACK")
    
    def handle_ack(self, data, addr):
        """Handle incoming ack"""
        sender = data.get('sender')
        
        # Remove from waiting list
        if sender in self.waiting_acks:
            del self.waiting_acks[sender]
            self.logger.log(f"SWIM: Received ACK from {sender}")
            
            # Clear any suspicion
            self.suspicion_mgr.clear_suspicion(sender)
    
    def handle_ping_req(self, data, addr):
        """Handle indirect ping request"""
        sender = data.get('sender')
        target = data.get('target')
        
        # Send ping to target on behalf of sender
        message = {
            'type': 'PING',
            'sender': self.hostname,
            'on_behalf_of': sender
        }
        self._send_message(target, message)
        self.logger.log(f"SWIM: Received PING_REQ from {sender} for {target}")
    
    def _send_message(self, target, message):
        """Send UDP message to target"""
        try:
            data = json.dumps(message).encode('utf-8')
            self.sock.sendto(data, (target, MEMBERSHIP_PORT))
        except Exception as e:
            self.logger.log(f"ERROR: Failed to send to {target}: {e}")

class MembershipService:
    """Main membership service coordinating both protocols"""
    
    def __init__(self, vm_id):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.member_id = f"{int(time.time())}_{self.hostname}_{MEMBERSHIP_PORT}"
        
        # Initialize components
        self.logger = Logger(vm_id)
        self.membership_list = MembershipList(self.hostname, self.member_id)
        self.suspicion_mgr = SuspicionManager(self.membership_list, self.logger)
        
        # Network setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', MEMBERSHIP_PORT))
        self.sock.settimeout(0.5)  # Non-blocking with timeout
        
        # Protocol instances
        self.gossip = GossipProtocol(self.membership_list, self.suspicion_mgr, 
                                    self.logger, self.sock)
        self.swim = SwimProtocol(self.membership_list, self.suspicion_mgr,
                               self.logger, self.sock)
        
        # Current protocol
        self.current_protocol = ProtocolType.GOSSIP
        self.running = False
        
        # Threads
        self.receiver_thread = None
        self.monitor_thread = None
        
        self.logger.log(f"INIT: Membership service started - ID: {self.member_id}")
    
    def start(self):
        """Start the membership service"""
        self.running = True
        
        # Start receiver thread
        self.receiver_thread = threading.Thread(target=self._receive_loop)
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        
        # Start monitor thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        # Start default protocol
        if self.current_protocol == ProtocolType.GOSSIP:
            self.gossip.start()
        else:
            self.swim.start()
        
        self.logger.log("SERVICE: Membership service started")
    
    def stop(self):
        """Stop the membership service"""
        self.running = False
        
        # Stop protocols
        self.gossip.stop()
        self.swim.stop()
        
        # Wait for threads
        if self.receiver_thread:
            self.receiver_thread.join(timeout=1)
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
        
        self.sock.close()
        self.logger.log("SERVICE: Membership service stopped")
    
    def switch_protocol(self, protocol_type):
        """Switch between Gossip and SWIM protocols"""
        if self.current_protocol == protocol_type:
            self.logger.log(f"PROTOCOL: Already using {protocol_type.value}")
            return
        
        self.logger.log(f"PROTOCOL: Switching from {self.current_protocol.value} to {protocol_type.value}")
        
        # Stop current protocol
        if self.current_protocol == ProtocolType.GOSSIP:
            self.gossip.stop()
        else:
            self.swim.stop()
        
        # Start new protocol
        self.current_protocol = protocol_type
        if protocol_type == ProtocolType.GOSSIP:
            self.gossip.start()
        else:
            self.swim.start()
        
        self.logger.log(f"PROTOCOL: Switched to {protocol_type.value}")
    
    def join_group(self):
        """Join the membership group"""
        # Mark self as active
        self.membership_list.members[self.hostname]['status'] = MemberStatus.ACTIVE.value
        
        if self.hostname == INTRODUCER_HOST:
            # This is the introducer
            self.logger.log("JOIN: Started as introducer")
        else:
            # Contact introducer
            message = {
                'type': 'JOIN',
                'sender': self.hostname,
                'member_data': self.membership_list.members[self.hostname]
            }
            
            try:
                data = json.dumps(message).encode('utf-8')
                self.sock.sendto(data, (INTRODUCER_HOST, MEMBERSHIP_PORT))
                self.logger.log(f"JOIN: Sent join request to introducer {INTRODUCER_HOST}")
            except Exception as e:
                self.logger.log(f"ERROR: Failed to join: {e}")
    
    def leave_group(self):
        """Leave the membership group"""
        # Mark self as left
        self.membership_list.members[self.hostname]['status'] = MemberStatus.LEFT.value
        
        # Notify all active members
        members = self.membership_list.get_members()
        active_members = [h for h, m in members.items() 
                        if h != self.hostname and 
                        m['status'] == MemberStatus.ACTIVE.value]
        
        message = {
            'type': 'LEAVE',
            'sender': self.hostname
        }
        
        for member in active_members:
            try:
                data = json.dumps(message).encode('utf-8')
                self.sock.sendto(data, (member, MEMBERSHIP_PORT))
            except:
                pass
        
        self.logger.log("LEAVE: Left the group")
    
    def _receive_loop(self):
        """Receive and handle incoming messages"""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                message = json.loads(data.decode('utf-8'))
                msg_type = message.get('type')
                
                # Route message to appropriate handler
                if msg_type == 'GOSSIP':
                    self.gossip.handle_gossip(message, addr)
                elif msg_type == 'PING':
                    self.swim.handle_ping(message, addr)
                elif msg_type == 'ACK':
                    self.swim.handle_ack(message, addr)
                elif msg_type == 'PING_REQ':
                    self.swim.handle_ping_req(message, addr)
                elif msg_type == 'JOIN':
                    self._handle_join(message, addr)
                elif msg_type == 'LEAVE':
                    self._handle_leave(message, addr)
                
            except socket.timeout:
                continue
            except Exception as e:
                self.logger.log(f"ERROR: Receive error: {e}")
    
    def _handle_join(self, message, addr):
        """Handle join request"""
        sender = message.get('sender')
        member_data = message.get('member_data')
        
        # Add to membership list
        self.membership_list.update_member(sender, member_data)
        self.logger.log(f"JOIN: {sender} joined the group")
        
        # If we're introducer, broadcast to others
        if self.hostname == INTRODUCER_HOST:
            members = self.membership_list.get_members()
            active_members = [h for h, m in members.items() 
                            if h != self.hostname and h != sender and
                            m['status'] == MemberStatus.ACTIVE.value]
            
            for member in active_members:
                forward_msg = {
                    'type': 'JOIN',
                    'sender': sender,
                    'member_data': member_data
                }
                try:
                    data = json.dumps(forward_msg).encode('utf-8')
                    self.sock.sendto(data, (member, MEMBERSHIP_PORT))
                except:
                    pass
    
    def _handle_leave(self, message, addr):
        """Handle leave notification"""
        sender = message.get('sender')
        self.membership_list.mark_failed(sender)
        self.logger.log(f"LEAVE: {sender} left the group")
    
    def _monitor_loop(self):
        """Monitor members for failures"""
        while self.running:
            try:
                current_time = time.time()
                members = self.membership_list.get_members()
                
                for hostname, member in members.items():
                    if hostname == self.hostname:
                        continue
                    
                    time_diff = current_time - member['timestamp']
                    status = member['status']
                    
                    # Check for timeout based on status
                    if status == MemberStatus.ACTIVE.value:
                        if time_diff > FAILURE_TIMEOUT:
                            self.suspicion_mgr.suspect_member(hostname)
                            self.logger.log(f"MONITOR: {hostname} timed out, suspecting")
                    
                    elif status == MemberStatus.FAILED.value:
                        if time_diff > CLEANUP_TIMEOUT:
                            self.membership_list.remove_member(hostname)
                            self.logger.log(f"MONITOR: Removed {hostname} from list")
                
                time.sleep(1)
                
            except Exception as e:
                self.logger.log(f"ERROR: Monitor error: {e}")
    
    def print_membership(self):
        members = self.membership_list.get_members()
        print("\n=== Membership List ===")
        for hostname, info in members.items():
            print(f"{hostname}: {info['status']} (HB: {info['heartbeat']}, Inc: {info['incarnation']})")
        print(f"Total members: {len(members)}\n")
    
    def print_status(self):
        print("\n=== Service Status ===")
        print(f"VM ID: {self.vm_id}")
        print(f"Hostname: {self.hostname}")
        print(f"Member ID: {self.member_id}")
        print(f"Current Protocol: {self.current_protocol.value}")
        print(f"Service Running: {self.running}")
        
        members = self.membership_list.get_members()
        active = sum(1 for m in members.values() if m['status'] == MemberStatus.ACTIVE.value)
        suspected = sum(1 for m in members.values() if m['status'] == MemberStatus.SUSPECTED.value)
        failed = sum(1 for m in members.values() if m['status'] == MemberStatus.FAILED.value)
        
        print(f"Members: Active={active}, Suspected={suspected}, Failed={failed}")
        print()

def main():
    parser = argparse.ArgumentParser(description='MP2 Membership Service')
    parser.add_argument('--vm-id', type=int, required=True, help='VM ID (1-10)')
    args = parser.parse_args()
    
    # Create and start service
    service = MembershipService(args.vm_id)
    service.start()
    
    # Command loop
    print("\n=== MP2 Membership Service ===")
    print("Commands: join, leave, list, status, switch, grep, quit")
    print()
    
    try:
        while True:
            cmd = input("mp2> ").strip().lower()
            
            if cmd == 'join':
                service.join_group()
            elif cmd == 'leave':
                service.leave_group()
            elif cmd == 'list':
                service.print_membership()
            elif cmd == 'status':
                service.print_status()
            elif cmd == 'switch':
                proto = input("Enter protocol (gossip/swim): ").strip().upper()
                if proto == 'GOSSIP':
                    service.switch_protocol(ProtocolType.GOSSIP)
                elif proto == 'SWIM':
                    service.switch_protocol(ProtocolType.SWIM)
                else:
                    print("Invalid protocol")
            elif cmd == 'grep':
                pattern = input("Enter grep pattern: ").strip()
                # Call MP1 grep here
                import subprocess
                result = subprocess.run(
                    ['python3', '../mp1/client.py', pattern],
                    capture_output=True, text=True
                )
                print(result.stdout)
            elif cmd == 'quit':
                break
            else:
                print(f"Unknown command: {cmd}")
    
    except KeyboardInterrupt:
        print("\nShutting down...")
    
    finally:
        service.stop()

if __name__ == '__main__':
    main()