import asyncio
import re
import time
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import astrbot.core.message.components as Comp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.message_event_result import MessageChain

from .api import run_server


def _format_bytes(n: int) -> str:
    """将字节数转换为人类可读格式。"""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


class KomariError(RuntimeError):
    """Komari API 调用错误。"""


class KomariClient:
    """基于 Komari HTTP API 的异步客户端。"""

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        timeout: int = 15,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "AstrBot-Komari-Status/1.0",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _request(self, method: str, path: str) -> Any:
        url = f"{self.base_url}{path}"
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    method, url, headers=self._headers()
                ) as resp:
                    raw = await resp.text()
                    if resp.status >= 400:
                        raise KomariError(f"HTTP {resp.status}: {raw[:200]}")
        except asyncio.TimeoutError:
            raise KomariError(f"请求超时({self.timeout}秒): {url}")
        except KomariError:
            raise
        except Exception as exc:
            raise KomariError(f"接口请求失败: {exc}") from exc

        import json

        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise KomariError("返回数据 JSON 解析失败") from exc

        if isinstance(data, dict) and data.get("status") == "error":
            raise KomariError(str(data.get("message", "接口返回错误")))
        return data

    async def get_nodes(self) -> List[Dict[str, Any]]:
        """获取所有节点信息。"""
        result = await self._request("GET", "/api/nodes")
        if isinstance(result, dict):
            nodes = result.get("data", result)
        else:
            nodes = result
        if not isinstance(nodes, list):
            raise KomariError("获取节点列表失败：返回数据格式异常")
        return nodes

    async def fetch_mjpeg_frame(
        self, lang: str = "zh", tz_offset: int = 8
    ) -> Optional[bytes]:
        """从 MJPEG 实时状态流中获取一帧 JPEG 图片数据。"""
        url = f"{self.base_url}/api/mjpeg_live?lang={lang}&tz_offset={tz_offset}"
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=self._headers()) as resp:
                    if resp.status != 200:
                        raise KomariError(f"MJPEG 请求失败: HTTP {resp.status}")

                    ct = resp.headers.get("Content-Type", "")
                    boundary = ""
                    if "boundary=" in ct:
                        boundary = ct.split("boundary=")[1].strip().strip('"')

                    if not boundary:
                        raise KomariError("无法解析 MJPEG 流边界")

                    boundary_bytes = f"--{boundary}".encode()
                    buf = b""

                    while True:
                        chunk = await resp.content.read(8192)
                        if not chunk:
                            break
                        buf += chunk

                        # 查找边界标记
                        b_idx = buf.find(boundary_bytes)
                        if b_idx < 0:
                            if len(buf) > 200000:
                                break
                            continue

                        after_boundary = buf[b_idx + len(boundary_bytes) :]
                        hdr_end = after_boundary.find(b"\r\n\r\n")
                        if hdr_end < 0:
                            continue

                        header_section = after_boundary[:hdr_end].decode(
                            "utf-8", errors="replace"
                        )
                        m = re.search(
                            r"Content-Length:\s*(\d+)", header_section, re.IGNORECASE
                        )
                        if not m:
                            continue

                        cl = int(m.group(1))
                        data_start = b_idx + len(boundary_bytes) + hdr_end + 4
                        data_end = data_start + cl

                        if len(buf) >= data_end:
                            return buf[data_start:data_end]

                        # 读取剩余字节
                        remaining = data_end - len(buf)
                        remaining_bytes = await resp.content.read(remaining)
                        buf += remaining_bytes
                        if len(buf) >= data_end:
                            return buf[data_start:data_end]
        except asyncio.TimeoutError:
            raise KomariError(f"MJPEG 流读取超时({self.timeout}秒)")
        except KomariError:
            raise
        except Exception as exc:
            raise KomariError(f"读取 MJPEG 流失败: {exc}") from exc

        return None


@register(
    "astrbot_plugin_komari_watch",
    "星见雅",
    "基于 Komari 实时状态检测的 AstrBot 插件，支持 MJPEG 状态图、节点信息查询与 Webhook 告警通知",
    "v1.0.0",
)
class KomariWatchPlugin(Star):
    def __init__(self, context: Context, config: Dict[str, Any]):
        super().__init__(context)
        self.context = context
        self.config = config

        self.base_url = str(config.get("base_url", "")).rstrip("/")
        self.api_key = str(config.get("api_key", ""))
        self.timeout = int(config.get("timeout_seconds", 15))
        self.mjpeg_lang = str(config.get("mjpeg_lang", "zh"))
        self.mjpeg_tz_offset = int(config.get("mjpeg_tz_offset", 8))

        self.client = KomariClient(
            base_url=self.base_url,
            api_key=self.api_key,
            timeout=self.timeout,
        )

        self.temp_dir = Path(StarTools.get_data_dir("astrbot_plugin_komari_watch"))
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.in_queue: Queue | None = None
        self.process: Process | None = None
        self._running = False
        self.target_umos: list[str] = []

        logger.info("[KomariWatch] 插件已加载")

    async def initialize(self):
        """插件初始化：启动 Webhook 服务器。"""
        webhook_conf = self.config.get("webhook", {})
        enabled = bool(webhook_conf.get("enabled", True))
        if not enabled:
            logger.info("[KomariWatch] Webhook 功能已关闭，未启动服务。")
            return

        target_umo_cfg = self.config.get("target_umo")
        if isinstance(target_umo_cfg, list) and target_umo_cfg:
            self.target_umos = target_umo_cfg
        else:
            logger.error(
                "[KomariWatch] 配置中的 'target_umo' 通知ID不是有效的列表或为空，"
                "Webhook 服务未启动。"
            )
            return

        host = str(webhook_conf.get("host", "0.0.0.0"))
        port = int(webhook_conf.get("port", 9968))
        webhook_path = str(webhook_conf.get("webhook_path", "/webhook"))
        token = str(webhook_conf.get("token", ""))

        self.in_queue = Queue()
        self.process = Process(
            target=run_server,
            args=(host, port, webhook_path, token, self.in_queue),
            daemon=True,
        )
        self.process.start()
        self._running = True
        asyncio.create_task(self._process_messages())
        logger.info(
            f"[KomariWatch] Webhook 服务已启动于 http://{host}:{port}{webhook_path}"
        )

    def _format_webhook_message(self, data: dict) -> str:
        """将 Komari Webhook 负载格式化为人类可读文本。

        清理「节点：」行和「消息：」前缀，替换事件名为中文。
        """
        title = data.get("title", "")
        message = data.get("message", "")
        title_lower = str(title).lower()

        # 事件名称映射（可配置）
        event_map = self.config.get("event_map", {})
        if not event_map:
            event_map = {
                "offline": "下线",
                "online": "上线",
                "alert": "警告",
            }

        # 图标映射
        icon_map = {
            "offline": "🔴",
            "online": "🟢",
            "alert": "⚠️",
            "expire": "⏰",
            "renew": "♻️",
            "login": "🔑",
            "traffic": "📊",
            "dreport": "📅",
            "wreport": "📅",
            "mreport": "📅",
        }
        icon = icon_map.get(title_lower, "🔔")

        # 处理消息正文
        cleaned_lines = []
        if message:
            for line in message.split("\n"):
                s = line.strip()
                if not s:
                    continue
                # 去掉「节点：」行
                if s.startswith("节点：") or s.startswith("节点:"):
                    continue
                # 去掉「消息：」前缀
                if s.startswith("消息：") or s.startswith("消息:"):
                    s = s[3:].strip()
                # 替换事件名为中文
                for eng, chn in event_map.items():
                    s = s.replace(eng, chn)
                cleaned_lines.append(s)

        cleaned_msg = "\n".join(cleaned_lines) if cleaned_lines else message

        # 构建最终文本
        result = f"{icon} Komari 监控通知\n──────────────\n"
        if cleaned_msg:
            result += cleaned_msg
        else:
            import json
            result += json.dumps(data, ensure_ascii=False, indent=2)
        return result

    async def _process_messages(self):
        """处理来自子进程的消息。"""
        if not self.in_queue or not self.target_umos:
            return

        while self._running:
            try:
                notification_msg = await asyncio.get_event_loop().run_in_executor(
                    None, self.in_queue.get
                )

                if notification_msg is None and not self._running:
                    break

                if isinstance(notification_msg, str):
                    logger.info(
                        f"[KomariWatch] 处理通知: \"{notification_msg[:100]}...\""
                    )
                    text = f"【Komari 监控通知】\n{notification_msg}"
                elif isinstance(notification_msg, dict):
                    logger.info(
                        f"[KomariWatch] 处理通知字典"
                        f" ({str(notification_msg)[:80]}...)"
                    )
                    text = self._format_webhook_message(notification_msg)
                else:
                    logger.warning(
                        f"[KomariWatch] 收到未知类型消息: {type(notification_msg)}"
                    )
                    continue

                chain = MessageChain(chain=[Comp.Plain(text)])

                for umo in self.target_umos:
                    try:
                        await self.context.send_message(umo, chain)
                        logger.info(f"[KomariWatch] 消息已发送至 {umo}")
                    except Exception as send_error:
                        logger.error(
                            f"[KomariWatch] 发送至 {umo} 失败: {send_error}",
                            exc_info=True,
                        )

            except EOFError:
                logger.info("[KomariWatch] 消息队列已关闭。")
                self._running = False
                break
            except Exception as e:
                logger.error(f"[KomariWatch] 处理消息时发生错误: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def terminate(self):
        """停止插件。"""
        logger.info("[KomariWatch] 正在终止插件...")
        self._running = False

        if self.in_queue:
            try:
                self.in_queue.put_nowait(None)
            except Exception:
                pass

        if self.process and self.process.is_alive():
            logger.info("[KomariWatch] 正在终止 Webhook 服务进程...")
            self.process.terminate()
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self.process.join, 5
                )
                if self.process.is_alive():
                    logger.warning("[KomariWatch] 进程未响应，强制终止...")
                    self.process.kill()
                    await asyncio.get_event_loop().run_in_executor(
                        None, self.process.join, 1
                    )
            except Exception as e:
                logger.error(f"[KomariWatch] 终止进程时发生错误: {e}")

        if self.in_queue:
            while not self.in_queue.empty():
                try:
                    self.in_queue.get_nowait()
                except Exception:
                    break
            self.in_queue.close()
            self.in_queue.join_thread()

        logger.info("[KomariWatch] 插件已终止。")

    def _check_configured(self) -> Optional[str]:
        if not self.base_url:
            return "Komari 站点地址未配置"
        return None

    @filter.command("服务器状态")
    async def server_status(self, event: AstrMessageEvent):
        """获取 Komari 服务器实时状态图片（MJPEG）。"""
        event.stop_event()

        err = self._check_configured()
        if err:
            yield event.plain_result(f"配置错误：{err}")
            return

        try:
            frame = await self.client.fetch_mjpeg_frame(
                lang=self.mjpeg_lang, tz_offset=self.mjpeg_tz_offset
            )
        except Exception as exc:
            yield event.plain_result(f"获取实时状态失败：{exc}")
            return

        if not frame:
            yield event.plain_result("获取实时状态失败：未获取到有效帧数据")
            return

        ts = int(time.time())
        img_path = self.temp_dir / f"status_{ts}.jpg"
        try:
            img_path.write_bytes(frame)
        except Exception as exc:
            yield event.plain_result(f"保存状态图片失败：{exc}")
            return

        yield event.image_result(str(img_path))

    @filter.command("服务器信息")
    async def server_info(self, event: AstrMessageEvent):
        """获取所有 Komari 服务器节点信息。"""
        event.stop_event()

        err = self._check_configured()
        if err:
            yield event.plain_result(f"配置错误：{err}")
            return

        try:
            nodes = await self.client.get_nodes()
        except Exception as exc:
            yield event.plain_result(f"获取节点信息失败：{exc}")
            return

        if not nodes:
            yield event.plain_result("当前没有可用的服务器节点。")
            return

        lines: List[str] = []
        for node in nodes:
            name = str(node.get("name", "未知"))
            cpu_name = str(node.get("cpu_name", ""))
            cpu_cores = int(node.get("cpu_cores", 0) or 0)
            region = str(node.get("region", ""))
            group = str(node.get("group", ""))
            os_info = str(node.get("os", ""))
            arch = str(node.get("arch", ""))

            mem_total = int(node.get("mem_total", 0) or 0)
            disk_total = int(node.get("disk_total", 0) or 0)

            lines.append(f"{region} {name}")
            if cpu_name:
                lines.append(f"  CPU：{cpu_name} ({cpu_cores}核)")
            else:
                lines.append(f"  CPU：{cpu_cores}核")
            lines.append(f"  内存：{_format_bytes(mem_total)}")
            lines.append(f"  磁盘：{_format_bytes(disk_total)}")
            if os_info:
                lines.append(f"  系统：{os_info} ({arch})")
            if group:
                lines.append(f"  分组：{group}")
            lines.append("")

        if not lines:
            yield event.plain_result("节点信息为空。")
            return

        result = "🌐 Komari 服务器节点信息\n"
        result += "━" * 24 + "\n"
        result += "\n".join(lines).rstrip()
        yield event.plain_result(result)