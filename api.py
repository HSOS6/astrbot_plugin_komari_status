import asyncio
from multiprocessing import Queue

from quart import Quart, jsonify, request, abort
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig

from astrbot.api import logger


class KomariWebhookServer:
    def __init__(self, webhook_path: str, token: str, in_queue: Queue):
        self.app = Quart(__name__)
        self.webhook_path = webhook_path
        self.token = token
        self.in_queue = in_queue
        self._server_task: asyncio.Task | None = None
        self._setup_routes()

    def _setup_routes(self):
        @self.app.errorhandler(400)
        async def bad_request(e):
            return jsonify({"error": "Bad Request"}), 400

        @self.app.errorhandler(403)
        async def forbidden(e):
            return jsonify({"error": "Forbidden"}), 403

        @self.app.errorhandler(415)
        async def unsupported_media_type(e):
            return jsonify({"error": "Unsupported Media Type"}), 415

        @self.app.errorhandler(500)
        async def server_error(e):
            return jsonify({"error": "Internal Server Error"}), 500

        @self.app.route(self.webhook_path, methods=["POST"])
        async def handle_webhook():
            if self.token:
                auth_header = request.headers.get("Authorization")
                expected_auth = f"Bearer {self.token}"
                if not auth_header or auth_header != expected_auth:
                    logger.warning(
                        f"[KomariWebhook] 来自 {request.remote_addr} 的无效或缺失令牌"
                    )
                    abort(403)

            # 尝试解析 JSON，兼容 Komari 可能发送的非标准 JSON
            data = None
            if request.is_json:
                try:
                    data = await request.get_json()
                except Exception:
                    pass

            # 如果 JSON 解析失败，尝试从原始 body 解析
            if not data:
                try:
                    raw_body = await request.get_data()
                    text_body = raw_body.decode("utf-8", errors="replace").strip()
                    if text_body:
                        import json
                        data = json.loads(text_body)
                except Exception:
                    pass

            if not data or not isinstance(data, dict):
                logger.warning(
                    f"[KomariWebhook] 来自 {request.remote_addr} 的空或无效 JSON 负载"
                )
                abort(400)

            self.in_queue.put(data)
            logger.info(
                f"[KomariWebhook] 来自 {request.remote_addr} 的通知已入队"
                f" (首段: {str(data)[:120]})"
            )
            return jsonify({"status": "ok"}), 200

        @self.app.route(self.webhook_path, methods=["GET"])
        async def webhook_health():
            return jsonify({"status": "ok", "service": "komari-webhook"}), 200

    async def start(self, host: str, port: int):
        config = HypercornConfig()
        config.bind = [f"{host}:{port}"]
        logger.info(f"[KomariWebhook] 服务已启动于 http://{host}:{port}{self.webhook_path}")

        self._server_task = asyncio.create_task(serve(self.app, config))
        try:
            await self._server_task
        except asyncio.CancelledError:
            logger.info("[KomariWebhook] 请求关闭服务")
        finally:
            await self.close()

    async def close(self):
        if self._server_task and not self._server_task.done():
            self._server_task.cancel()
            try:
                await self._server_task
            except asyncio.CancelledError:
                pass
        logger.info("[KomariWebhook] 服务已关闭")


def run_server(host: str, port: int, webhook_path: str, token: str, in_queue: Queue):
    server = KomariWebhookServer(webhook_path, token, in_queue)
    asyncio.run(server.start(host, port))
