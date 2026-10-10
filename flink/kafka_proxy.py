import socket
import threading
import sys

def forward(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind(('127.0.0.1', 9092))
    except Exception as e:
        print(f"Proxy already running or port bound: {e}", flush=True)
        return

    srv.listen(128)
    print("Kafka localhost proxy running on 127.0.0.1:9092 -> icestream-kafka:9092", flush=True)
    while True:
        try:
            client, _ = srv.accept()
            remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            remote.connect(('icestream-kafka', 9092))
            threading.Thread(target=forward, args=(client, remote), daemon=True).start()
            threading.Thread(target=forward, args=(remote, client), daemon=True).start()
        except Exception:
            pass

if __name__ == '__main__':
    main()
