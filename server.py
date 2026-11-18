import asyncio
import hmac
import json
import os
import ssl
import uuid
from pathlib import Path

from aiohttp import web


BASE_DIR = Path(__file__).resolve().parent
AUDIO_PACKET = 1
SCREEN_PACKET = 2
MAX_WS_MESSAGE_SIZE = 2 * 1024 * 1024


class PTTServer:
    def __init__(self):
        self.clients = {}
        self.room_code = None
        self.teacher_code = None

    def configure_credentials(self, room_code, teacher_code):
        self.room_code = room_code
        self.teacher_code = teacher_code

    async def index(self, _request):
        return web.FileResponse(BASE_DIR / "index.html")

    async def audio_worklet(self, _request):
        return web.FileResponse(
            BASE_DIR / "audio-worklet.js",
            headers={"Cache-Control": "no-cache"},
        )

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse(heartbeat=25, max_msg_size=MAX_WS_MESSAGE_SIZE)
        await ws.prepare(request)

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        await ws.send_json({"type": "error", "message": "Некоректний формат повідомлення."})
                        continue

                    if not isinstance(data, dict):
                        await ws.send_json({"type": "error", "message": "Очікувався JSON-об'єкт."})
                        continue

                    client_info = self.clients.get(ws)
                    if client_info is None:
                        if data.get("type") != "join":
                            await ws.send_json({
                                "type": "auth_error",
                                "message": "Спочатку потрібно увійти в систему."
                            })
                            break
                        if not await self._join(ws, data):
                            break
                        continue

                    if await self._handle_text(ws, client_info, data):
                        break

                elif msg.type == web.WSMsgType.BINARY:
                    client_info = self.clients.get(ws)
                    if client_info is None:
                        await ws.close(code=1008, message=b"Authentication required")
                        break
                    if not await self._handle_binary(ws, client_info, msg.data):
                        break

                elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                    break
        finally:
            client_info = self.clients.pop(ws, None)
            if client_info:
                if client_info["role"] == "teacher":
                    await self._broadcast_students(json.dumps({"type": "stop_screen"}))
                elif client_info["role"] == "student":
                    await self.broadcast_students_list()
        return ws

    async def _join(self, ws, data):
        role = data.get("role")
        name = data.get("name")
        credential = data.get("credential")
        if role not in ("teacher", "student") or not isinstance(credential, str):
            await ws.send_json({"type": "auth_error", "message": "Невірні дані для входу."})
            return False

        expected_code = self.teacher_code if role == "teacher" else self.room_code
        if expected_code is None or not hmac.compare_digest(credential, expected_code):
            await ws.send_json({"type": "auth_error", "message": "Невірний код доступу."})
            return False

        if role == "teacher":
            name = "Вчитель"
        elif not isinstance(name, str) or not name.strip() or len(name.strip()) > 60:
            await ws.send_json({"type": "auth_error", "message": "Ім'я має містити від 1 до 60 символів."})
            return False
        else:
            name = name.strip()

        client_id = uuid.uuid4()
        info = {
            "id": client_id,
            "id_bytes": client_id.bytes,
            "role": role,
            "name": name,
            "muted": False,
        }
        self.clients[ws] = info
        await ws.send_json({
            "type": "joined",
            "id": str(client_id),
            "muted": info["muted"],
        })
        await self.broadcast_students_list()
        return True

    async def _handle_text(self, ws, sender_info, data):
        message_type = data.get("type")
        if message_type == "stop_screen":
            if sender_info["role"] == "teacher":
                await self._broadcast_students(json.dumps({"type": "stop_screen"}))
            return False

        if message_type in ("mute_all", "unmute_all"):
            if sender_info["role"] != "teacher":
                await ws.send_json({"type": "error", "message": "Цю дію може виконати лише вчитель."})
                return False
            muted = message_type == "mute_all"
            for client, info in list(self.clients.items()):
                if info["role"] == "student":
                    info["muted"] = muted
                    if not client.closed:
                        await client.send_json({"type": "you_are_muted", "muted": muted})
            await self.broadcast_students_list()
            return False

        if message_type == "set_student_mute":
            if sender_info["role"] != "teacher":
                await ws.send_json({"type": "error", "message": "Цю дію може виконати лише вчитель."})
                return False
            target_id = data.get("id")
            muted = data.get("muted")
            if not isinstance(target_id, str) or not isinstance(muted, bool):
                await ws.send_json({"type": "error", "message": "Некоректні дані для зміни муту."})
                return False
            for client, info in list(self.clients.items()):
                if info["role"] == "student" and str(info["id"]) == target_id:
                    info["muted"] = muted
                    if not client.closed:
                        await client.send_json({"type": "you_are_muted", "muted": muted})
                    break
            await self.broadcast_students_list()
            return False

        await ws.send_json({"type": "error", "message": "Невідома команда."})
        return False

    async def _handle_binary(self, ws, sender_info, payload):
        if not payload:
            await ws.close(code=1003, message=b"Empty binary message")
            return False

        packet_type = payload[0]
        if packet_type == AUDIO_PACKET:
            if len(payload) < 4 or (len(payload) - 2) % 2:
                await ws.close(code=1003, message=b"Invalid audio packet")
                return False
            stream_kind = payload[1]
            if stream_kind not in (0, 1):
                await ws.close(code=1003, message=b"Invalid audio stream")
                return False
            if sender_info["role"] == "student":
                if stream_kind != 0:
                    await ws.close(code=1008, message=b"Students may only send microphone audio")
                    return False
                if sender_info["muted"]:
                    return True

            routed_packet = (
                bytes((AUDIO_PACKET,))
                + sender_info["id_bytes"]
                + bytes((stream_kind,))
                + payload[2:]
            )
            target_role = "student" if sender_info["role"] == "teacher" else "teacher"
            await self._send_binary_to_role(target_role, ws, routed_packet)
            return True

        if packet_type == SCREEN_PACKET:
            if sender_info["role"] != "teacher":
                await ws.close(code=1008, message=b"Only teachers may share a screen")
                return False
            if len(payload) < 4:
                await ws.close(code=1003, message=b"Invalid screen frame")
                return False
            await self._broadcast_students_binary(payload)
            return True

        await ws.close(code=1003, message=b"Unsupported binary packet")
        return False

    async def _send_binary_to_role(self, role, sender, payload):
        tasks = [
            client.send_bytes(payload)
            for client, info in list(self.clients.items())
            if client != sender and info["role"] == role and not client.closed
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_students(self, message):
        tasks = [
            client.send_str(message)
            for client, info in list(self.clients.items())
            if info["role"] == "student" and not client.closed
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_students_binary(self, payload):
        tasks = [
            client.send_bytes(payload)
            for client, info in list(self.clients.items())
            if info["role"] == "student" and not client.closed
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast_students_list(self):
        students = [
            {"id": str(info["id"]), "name": info["name"], "muted": info["muted"]}
            for info in self.clients.values()
            if info["role"] == "student"
        ]
        message = json.dumps({"type": "students_list", "students": students}, ensure_ascii=False)
        tasks = [
            client.send_str(message)
            for client, info in list(self.clients.items())
            if info["role"] == "teacher" and not client.closed
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


app = web.Application()
server = PTTServer()
app.router.add_get("/", server.index)
app.router.add_get("/audio-worklet.js", server.audio_worklet)
app.router.add_get("/ws", server.websocket_handler)


def load_credentials():
    room_code = os.environ.get("ROOM_CODE", "")
    teacher_code = os.environ.get("TEACHER_CODE", "")
    if len(room_code) < 6:
        raise RuntimeError("Set ROOM_CODE to a room access code of at least 6 characters.")
    if len(teacher_code) < 6:
        raise RuntimeError("Set TEACHER_CODE to a private teacher code of at least 6 characters.")
    return room_code, teacher_code


if __name__ == "__main__":
    room_code, teacher_code = load_credentials()
    server.configure_credentials(room_code, teacher_code)

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.load_cert_chain(
        certfile=BASE_DIR / "cert.pem",
        keyfile=BASE_DIR / "key.pem",
    )

    print("Сервер запущено на https://0.0.0.0:8000")
    web.run_app(app, host="0.0.0.0", port=8000, ssl_context=ssl_context)
