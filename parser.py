import re
import requests
from bridge.reply import Reply, ReplyType
from bridge.context import ContextType
from common.log import logger
from plugins import register, Plugin, Event, EventContext, EventAction

@register(name="media_parser", desc="视频图集解析插件", version="1.0", author="安与", desire_priority=100)
class MediaParserPlugin(Plugin):
    def __init__(self):
        super().__init__()
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context

        # 从配置文件中加载参数
        config = self.load_config()
        self.video_api = config.get("api_endpoints", {}).get("video", "")
        self.image_api = config.get("api_endpoints", {}).get("image", "")
        self.supported_platforms = config.get("supported_platforms", [])
        self.commands = config.get("commands", [])

        logger.info(f"[{__class__.__name__}] 插件已加载")

    def load_config(self):
        import os
        import json

        plugin_dir = os.path.dirname(__file__)
        config_path = os.path.join(plugin_dir, "config.json")
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def get_help_text(self, **kwargs):
        help_text = "使用方法：\n"
        commands = [cmd["trigger"] for cmd in self.commands]
        help_text += f"1. 输入 '{'/'.join(commands)} <链接>' 来解析视频或图集链接，例如：解析视频 https://www.douyin.com/example_video_url\n"
        help_text += f"2. 机器人将返回解析后的详细信息。"
        return help_text

    def on_handle_context(self, e_context: EventContext):
        if e_context['context'].type != ContextType.TEXT:
            return

        content = e_context['context'].content
        for command in self.commands:
            trigger = command.get("trigger")
            if content.startswith(trigger):
                logger.info(f"[{__class__.__name__}] 收到解析请求: {content}")
                args = content.replace(trigger, "").strip()

                if not args:
                    reply = Reply(ReplyType.ERROR, f"请提供要解析的链接，例如：{trigger} <链接>")
                    e_context['reply'] = reply
                    e_context.action = EventAction.BREAK_PASS
                    return

                if trigger == "解析视频":
                    result = self.parse_video_url(args)
                elif trigger == "解析图集":
                    result = self.parse_image_url(args)
                else:
                    continue

                reply = self.format_response(result)
                e_context['reply'] = Reply(ReplyType.TEXT, reply)
                e_context.action = EventAction.BREAK_PASS
                return

    def parse_video_url(self, url):
        if not self.video_api:
            return {"error": "视频API接口未配置。"}

        try:
            response = requests.get(self.video_api, params={"url": url}, timeout=10)
            data = response.json()

            if data.get("code") == 200:
                video_data = data.get("data", {})
                result = {
                    "平台": self.identify_platform(url),
                    "视频标题": video_data.get("title", "未知标题"),
                    "作者": video_data.get("author", "未知作者"),
                    "无水印视频链接": video_data.get("url", "无视频链接"),
                    "视频预览": video_data.get("preview_image", "无预览图"),
                    "背景音乐链接": video_data.get("music_url", "无音乐链接")
                }
                return result
            else:
                return {"error": data.get("msg", "解析失败，请检查链接是否正确。")}
        except requests.Timeout:
            logger.error(f"[{__class__.__name__}] 视频API请求超时")
            return {"error": "解析请求超时，请稍后再试。"}
        except requests.RequestException as e:
            logger.error(f"[{__class__.__name__}] 视频API请求失败: {str(e)}")
            return {"error": "解析过程中发生网络错误，请稍后再试。"}
        except KeyError as e:
            logger.error(f"[{__class__.__name__}] 视频API返回数据缺少关键字段: {str(e)}")
            return {"error": "解析结果格式错误，请稍后再试。"}
        except Exception as e:
            logger.error(f"[{__class__.__name__}] 视频API未知错误: {str(e)}")
            return {"error": "解析过程中发生未知错误，请稍后再试。"}

    def parse_image_url(self, url):
        if not self.image_api:
            return {"error": "图集API接口未配置。"}

        try:
            response = requests.get(self.image_api, params={"url": url}, timeout=10)
            data = response.json()

            if data.get("code") == 200:
                image_data = data.get("data", {})
                images = image_data.get("images", [])
                if not isinstance(images, list):
                    images = [images]
                result = {
                    "作者": image_data.get("author", "未知作者"),
                    "图片预览": images
                }
                return result
            else:
                return {"error": data.get("msg", "解析失败，请检查链接是否正确。")}
        except requests.Timeout:
            logger.error(f"[{__class__.__name__}] 图集API请求超时")
            return {"error": "解析请求超时，请稍后再试。"}
        except requests.RequestException as e:
            logger.error(f"[{__class__.__name__}] 图集API请求失败: {str(e)}")
            return {"error": "解析过程中发生网络错误，请稍后再试。"}
        except KeyError as e:
            logger.error(f"[{__class__.__name__}] 图集API返回数据缺少关键字段: {str(e)}")
            return {"error": "解析结果格式错误，请稍后再试。"}
        except Exception as e:
            logger.error(f"[{__class__.__name__}] 图集API未知错误: {str(e)}")
            return {"error": "解析过程中发生未知错误，请稍后再试。"}

    def identify_platform(self, url):
        platforms = {
            "抖音": r"www\.douyin\.com",
            "快手": r"www\.kuaishou\.com",
            "小红书": r"xiaohongshu\.com",
            "皮皮虾": r"pipix\.mndmedia\.com",
            "西瓜视频": r"xigua\.video",
            "最右": r"zuiyou\.com",
            "火山": r"huoshan\.com",
            "微博": r"weibo\.com",
            "微视": r"weishi\.tv",
            "绿洲": r"zoo\.weibo\.com",
            "bilibili": r"bilibili\.com",
            "陌陌": r"momom\.com",
            "全民视频": r"quanmin\.com",
            "全民K歌": r"kg\.quanmin\.com",
            "逗拍": r"dou\.pai\.com",
            "美拍": r"mei\.pai\.com",
            "六间房": r"liujianfang\.com",
            "梨视频": r"lireader\.com",
            "虎牙": r"huya\.com",
            "新片场": r"xinpianchang\.com",
            "AcFun": r"acfun\.cn"
        }

        for platform, pattern in platforms.items():
            if re.search(pattern, url):
                return platform
        return "未知平台"

    def format_response(self, data):
        if "error" in data:
            return data["error"]
        response = ""
        for key, value in data.items():
            if isinstance(value, list):
                value = '\n'.join(value)
            response += f"{key}: {value}\n"
        return response