from multiprocessing import Process, Pipe

from apache.crypto.datafeeds.ws import WebsocketMonitor



if __name__ == '__main__':
    parent_conn, child_conn = Pipe()
    ws = WebsocketMonitor('BINANCE-DATA')
    ws.resolver()
    ws.register_stream()

    p = Process(target=f, args=(child_conn,))
    p.start()
    print(parent_conn.recv())
    p.join()