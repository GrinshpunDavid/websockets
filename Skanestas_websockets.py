import asyncio
import json
import websockets

# Configuration
source_ws_url = "wss://test-ws.skns.dev/raw-messages"
target_ws_url_template = "wss://test-ws.skns.dev/ordered-messages/{}"
candidate_surname = "david_grinshpun"
received_messages = {}
N = 1000

async def receive_messages(source_ws_url, N):
    async with websockets.connect(source_ws_url) as source_ws:
        received_messages = {}
        while len(received_messages) < N:
            message = await source_ws.recv()
            message_data = json.loads(message)
            received_messages[message_data.get("id")] = message_data

        print("All messages received.")

        # Sort the keys in ascending order
        sorted_keys = sorted(received_messages.keys())
        for key in sorted_keys: # Send the ordered message to the target WebSocket
            await send_ordered_message(received_messages[key])
        print("All messages processed and sent.")

async def send_ordered_message(message_data):
    async with websockets.connect(target_ws_url_template.format(candidate_surname)) as target_ws:
        await target_ws.send(json.dumps(message_data))

async def main():
    # Start the message receiving task
    start_time = asyncio.get_event_loop().time()
    receive_task = asyncio.create_task(receive_messages(source_ws_url, N))

    # Wait for all messages to be received and sent
    await receive_task
    end_time = asyncio.get_event_loop().time()
    total_time = round(end_time - start_time, 2)
    print(f"All {N} ordered messages sent in {total_time} seconds.")

if __name__ == "__main__":
    asyncio.run(main())