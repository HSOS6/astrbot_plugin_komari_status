# [astrbot_plugin_komari_watch](https://github.com/HSOS6/astrbot_plugin_komari_status)

基于 [Komari](https://github.com/komari-monitor/komari) 服务器监控工具的 AstrBot 插件，提供实时状态查询和 Webhook 告警推送。

## 功能

- **/服务器状态** — 获取 MJPEG 实时状态图（中文，支持时区配置）
- **/服务器信息** — 查询所有节点信息（CPU、内存、磁盘等）
- **Webhook 告警接收** — 接收 Komari 的离线/上线/警告等通知并推送到 QQ 群/用户

## 安装

1. 将本插件目录放入 AstrBot 的 `addons/` 目录
2. 重启 AstrBot
3. 在 WebUI 中配置插件

## 配置说明

| 配置项 | 说明 |
|--------|------|
| `base_url` | Komari 站点地址，例如 `https://zt.xinjianya.top` |
| `api_key` | Komari API 密钥（公开站点可留空） |
| `target_umo` | Webhook 通知接收目标（QQ 群号/用户ID），可用 `/sid` 获取 |
| `event_map` | 事件名称中文映射，默认：offline→下线，online→上线，alert→警告 |
| `webhook.enabled` | 是否启用 Webhook 接收 |
| `webhook.port` | 监听端口（默认 9968） |
| `webhook.token` | 鉴权 Token，可选 |

## Komari 后台 Webhook 配置

| 字段 | 填写 |
|------|------|
| URL | `http://<AstrBotIP>:端口/webhook` |
| Method | `POST` |
| Content-Type | `application/json` |
| Headers | 如果配置了 token，填 `{"Authorization":"Bearer <token>"}` |
| Body | `{"message":"{{message}}","title":"{{title}}"}` |

## 指令

- `/服务器状态` — 返回 Komari 实时状态图片
- `/服务器信息` — 返回所有节点硬件配置信息
