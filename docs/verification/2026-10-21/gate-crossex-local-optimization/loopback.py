"""Build-server-only standard-library websocket fixture; public L2 schema, no account API."""
import asyncio, base64, hashlib, json, struct, time

def frame(opcode, data):
    size = len(data)
    header = bytes([0x80 | opcode, size]) if size < 126 else bytes([0x80 | opcode, 126]) + struct.pack('!H', size)
    return header + data

async def client(reader, writer):
    headers = (await reader.readuntil(b'\r\n\r\n')).decode()
    key = next(line.split(':', 1)[1].strip() for line in headers.splitlines() if line.lower().startswith('sec-websocket-key:'))
    accept = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
    writer.write(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
    await writer.drain()
    ready = asyncio.Event()
    async def receive():
        while True:
            first, second = await reader.readexactly(2)
            size = second & 127
            if size == 126: size = struct.unpack('!H', await reader.readexactly(2))[0]
            elif size == 127: size = struct.unpack('!Q', await reader.readexactly(8))[0]
            mask = await reader.readexactly(4) if second & 128 else b''
            payload = await reader.readexactly(size)
            if mask: payload = bytes(v ^ mask[i % 4] for i, v in enumerate(payload))
            opcode = first & 15
            if opcode == 9:
                writer.write(frame(10, payload)); await writer.drain()
            elif opcode == 8: return
            elif opcode == 1:
                value = json.loads(payload)
                assert value['event'] == 'subscribe' and value['channel'] == 'order_book_5'
                ready.set()
    task = asyncio.create_task(receive())
    try:
        await ready.wait()
        while not task.done():
            value = dict(event='update', channel='order_book_5', time_ms=time.time_ns()//1_000_000,
                result=dict(s='GATE_FUTURE_BTC_USDT', ts=time.time_ns()//1_000_000,
                    b=[[f'{80000-i/10:.1f}', str(100+i)] for i in range(5)],
                    a=[[f'{80001+i/10:.1f}', str(200+i)] for i in range(5)]))
            writer.write(frame(1, json.dumps(value).encode())); await writer.drain()
            await asyncio.sleep(.1)
    except (ConnectionError, asyncio.IncompleteReadError): pass
    finally:
        task.cancel(); writer.close(); await writer.wait_closed()

async def main():
    server = await asyncio.start_server(client, '127.0.0.1', 38875)
    print('ready', flush=True)
    async with server: await server.serve_forever()
asyncio.run(main())
