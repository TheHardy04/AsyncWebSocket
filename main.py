import asyncio
import websockets
import json
import sys
import aiofiles

file_lock = asyncio.Lock()
queue = asyncio.Queue()

async def get_binance_stream(reset: bool = False, multiple_streams: bool = False):
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth5/btcusdt@trade"

    async with websockets.connect(url) as websocket:
        print("Connection established to Binance")

        while True:
            try:
                message = await websocket.recv()
                # print("Message received from Binance")
                data = json.loads(message)
                if "e" in data and data["e"] == "trade":
                    trade_data = {
                        "price": data.get("p"),
                        "quantity": data.get("q")
                    }
                    await queue.put(json.dumps({"trade": trade_data}))
                elif "lastUpdateId" in data and "bids" in data and "asks" in data:
                    depth_data = {
                        "lastUpdateId": data.get("lastUpdateId"),
                        "bids": data.get("bids", []),
                        "asks": data.get("asks", [])
                    }
                    await queue.put(json.dumps({"depth": depth_data}))
                await asyncio.sleep(0.4)
            except asyncio.CancelledError:
                print("Binance stream cancelled.")
                break
            except Exception as e:
                print("Error in Binance stream:", e)
                break

async def get_upbit_stream(reset: bool = False, multiple_streams: bool = False):
    url = "wss://api.upbit.com/websocket/v1"

    async with websockets.connect(url) as websocket:
        print("Connection established to Upbit")

        await websocket.send('[{"ticket":"UNIQUE_TICKET"},{"type":"orderbook","codes":["KRW-BTC"]}]')

        while True:
            try:
                message = await websocket.recv()
                # print("Message received from Upbit")
                data = json.loads(message)
                if "type" in data and data["type"] == "orderbook":
                    upbit_data = {"orderbook_units": data.get("orderbook_units")}
                    await queue.put(json.dumps({"orderbook": upbit_data}))
                await asyncio.sleep(0.4)
            except asyncio.CancelledError:
                print("Upbit stream cancelled.")
                break
            except Exception as e:
                print("Error in Upbit stream:", e)
                break

async def write_to_file(file_name):
    async with aiofiles.open(file_name, mode='w') as f:
        while True:
            message = await queue.get()
            if message is None:  # Exit signal
                break
            async with file_lock:
                await f.write(message + "\n")
            queue.task_done()

def print_help():
    print("Usage:")
    print("  -r --reset     : reset the file")
    print("  -b --binance   : get Binance stream")
    print("  -u --upbit     : get Upbit stream")

class ArgumentException(Exception):
    pass

if __name__ == "__main__":
    reset = False
    to_launch = []
    agr_list = [str(i) for i in sys.argv]
    try:
        for arg in agr_list:
            if arg[0] == "-":
                if arg[1] == "-":
                    if "reset" in arg:
                        reset = True
                    elif "binance" in arg:
                        to_launch.append(get_binance_stream)
                    elif "upbit" in arg:
                        to_launch.append(get_upbit_stream)
                    elif "help" in arg:
                        print_help()
                        sys.exit(0)
                    else:
                        raise ArgumentException("Unknown argument:", arg)
                else:
                    for char in arg[1:]:
                        if char == "r":
                            reset = True
                        elif char == "b":
                            to_launch.append(get_binance_stream)
                        elif char == "u":
                            to_launch.append(get_upbit_stream)
                        elif char == "h":
                            print_help()
                            sys.exit(0)
                        else:
                            raise ArgumentException("Unknown argument:", char)
    except ArgumentException as e:
        print("Error:", e)
        sys.exit(1)

    if not to_launch:
        to_launch = [get_binance_stream]
    multiple_streams = len(to_launch) > 1
    if multiple_streams:
        print("Multiple streams activated")


    async def main():
        tasks = []
        try:
            # Create tasks for streams and the file writer
            tasks = [stream(reset, multiple_streams) for stream in to_launch]
            tasks.append(write_to_file("stream-data-multi.txt"))

            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("Tasks have been cancelled.")
        except KeyboardInterrupt:
            print("Keyboard interrupt detected. Shutting down...")
            for task in asyncio.all_tasks():
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await queue.put(None)
            print("Shutdown complete.")


    asyncio.run(main())
