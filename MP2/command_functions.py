import socket

CONTROL_PORT = 9090

VM_HOSTS = {
    1: "fa25-cs425-a701.cs.illinois.edu",
    2: "fa25-cs425-a702.cs.illinois.edu",
    3: "fa25-cs425-a703.cs.illinois.edu",
    4: "fa25-cs425-a704.cs.illinois.edu",
    5: "fa25-cs425-a705.cs.illinois.edu",
    6: "fa25-cs425-a706.cs.illinois.edu",
    7: "fa25-cs425-a707.cs.illinois.edu",
    8: "fa25-cs425-a708.cs.illinois.edu",
    9: "fa25-cs425-a709.cs.illinois.edu",
    10: "fa25-cs425-a710.cs.illinois.edu"
}

def send_command(vm_id, command):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((VM_HOSTS[vm_id], CONTROL_PORT))
        sock.sendall(command.encode())
        response = sock.recv(8192).decode()
        sock.close()
        print(f"[VM{vm_id}] {response}")
    except Exception as e:
        print(f"[VM{vm_id}] Error sending command: {e}")

def main():
    print("\nCommands:")
    print("  vm <id> - select VM (1–10 or 'all')")
    print("  join - join group")
    print("  leave - leave group")
    print("  list_mem - show membership list")
    print("  drop_rate <value> - specify UDP drop rate for a node 0.0-1.0")
    print("  display_protocol - show current protocol")
    print("  display_suspects - show suspected machines")
    print("  switch gossip/ping suspect/nosuspect - switch protocol")
    print("  quit - exit")
    print()

    target = None
    while True:
        try:
            cmd = input(">>> ").strip()
            if not cmd:
                continue

            if cmd.startswith("vm "):
                arg = cmd.split()[1]
                if arg == "all":
                    target = "all"
                else:
                    try:
                        target = int(arg)
                    except ValueError:
                        print("Invalid VM ID")
                        continue
                print(f"Target set to {target}")
                continue

            if cmd in ("quit", "exit"):
                break

            if target is None:
                print("Select a VM first with: vm <id> or vm all")
                continue

            if target == "all":
                for vm_id in range(1, 11):
                    send_command(vm_id, cmd)
            else:
                send_command(target, cmd)

        except KeyboardInterrupt:
            print("\nExiting controller.")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
