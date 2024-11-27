import re
import requests
import time
import os
import json
from bridge.reply import Reply, ReplyType
from bridge.context import ContextType, Context
from common.log import logger
from plugins import register, Plugin, Event, EventContext, EventAction
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import hashlib
import threading
from io import BytesIO
from PIL import Image
import io

@register(name="media_parser", desc="视频图集解析插件", version="1.4", author="安与", desire_priority=100)
class MediaParserPlugin(Plugin):
    def __init__(self):
        super().__init__()
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
        
        # 初始化配置和缓存
        self._load_config()
        self._init_cache()
        
        # 初始化会话和线程池
        self.session = requests.Session()
        self.executor = ThreadPoolExecutor(max_workers=5)  # 增加线程池大小以提高下载效率
        
        # 线程锁用于缓存管理的同步
        self.cache_lock = threading.Lock()  # 确保这行存在
        self.tasks_lock = threading.Lock()
        
        # 支持的文件类型
        self.video_extensions = {
            'video/mp4': '.mp4',
            'video/x-flv': '.flv',
            'video/quicktime': '.mov',
            'video/x-ms-wmv': '.wmv',
            'video/x-msvideo': '.avi',
        }
        
        self.image_extensions = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'image/webp': '.webp',
            'image/bmp': '.bmp',
        }
        
        # 记录正在处理的任务
        self.processing_tasks = {}
        
        # 启动后台线程处理待发送的任务
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._process_pending_tasks, daemon=True)
        self.worker_thread.start()
        
        logger.info("[MediaParser] 插件已加载")

    def _load_config(self):
        """加载配置文件"""
        curdir = os.path.dirname(__file__)
        config_path = os.path.join(curdir, "config.json")
        
        self.default_config = {
            "api_endpoints": {
                "video": "https://www.hhlqilongzhu.cn/api/sp_jx/sp.php",
                "image": "https://www.hhlqilongzhu.cn/api/sp_jx/tuji.php"
            },
            "supported_platforms": [
                "抖音",
                "快手",
                "微博",
                "小红书"
            ],
            "cache": {
                "max_size_mb": 500,
                "max_age_hours": 24,
                "chunk_size": 8192
            },
            "download": {
                "timeout": 30,
                "max_retries": 3,
                "retry_delay": 1,
                "max_workers": 5
            },
            "batch": {
                "image_limit": 10,  # 每批最多发送的图片数
                "delay_seconds": 2   # 批次间延迟时间
            },
            "max_video_size_mb": 20  # 视频最大大小（MB）
        }
        
        try:
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                self.config = self._merge_config(self.default_config, config)
            else:
                self.config = self.default_config
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.warn(f"[MediaParser] 加载配置文件失败: {e}, 使用默认配置")
            self.config = self.default_config
            
        # 设置API endpoints
        self.video_api = self.config["api_endpoints"]["video"]
        self.image_api = self.config["api_endpoints"]["image"]
        self.supported_platforms = self.config.get("supported_platforms", [])

        # 配置验证
        self._validate_config()

    def _validate_config(self):
        """验证配置文件的有效性"""
        try:
            assert isinstance(self.config["cache"]["max_size_mb"], (int, float)) and self.config["cache"]["max_size_mb"] > 0, "缓存大小配置错误"
            assert isinstance(self.config["cache"]["max_age_hours"], (int, float)) and self.config["cache"]["max_age_hours"] > 0, "缓存过期时间配置错误"
            assert isinstance(self.config["download"]["timeout"], (int, float)) and self.config["download"]["timeout"] > 0, "下载超时配置错误"
            assert isinstance(self.config["download"]["max_retries"], int) and self.config["download"]["max_retries"] >= 0, "最大重试次数配置错误"
            assert isinstance(self.config["download"]["retry_delay"], (int, float)) and self.config["download"]["retry_delay"] >= 0, "重试延迟配置错误"
            assert isinstance(self.config["batch"]["image_limit"], int) and self.config["batch"]["image_limit"] > 0, "图集批量发送限制配置错误"
            assert isinstance(self.config["batch"]["delay_seconds"], (int, float)) and self.config["batch"]["delay_seconds"] >= 0, "批次延迟时间配置错误"
            assert isinstance(self.config["max_video_size_mb"], (int, float)) and self.config["max_video_size_mb"] > 0, "视频最大大小配置错误"
            logger.info("[MediaParser] 配置文件验证通过")
        except AssertionError as e:
            logger.error(f"[MediaParser] 配置文件验证失败: {e}")
            self.config = self.default_config  # 回退到默认配置

    def _merge_config(self, default, custom):
        """递归合并配置"""
        result = default.copy()
        for key, value in custom.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result

    def _init_cache(self):
        """初始化缓存目录"""
        curdir = os.path.dirname(__file__)
        self.cache_dir = os.path.join(curdir, "cache")
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # 启动时清理过期缓存，使用后台线程
        threading.Thread(target=self._clear_expired_cache, daemon=True).start()

    def get_help_text(self, **kwargs):
        help_text = "视频/图集解析插件使用说明：\n"
        help_text += "1. 发送 '解析视频 <链接>' 获取无水印视频\n"
        help_text += "2. 发送 '解析图集 <链接>' 获取图集原图\n"
        help_text += "3. 发送 '清理缓存' 清除临时文件\n"
        help_text += "4. 发送 '查看缓存' 查看缓存状态\n"
        help_text += "\n支持批量发送图片，每批最多发送 {} 张\n".format(
            self.config["batch"]["image_limit"])
        if self.supported_platforms:
            help_text += "\n支持的平台：\n"
            help_text += "、".join(self.supported_platforms)
        return help_text

    def on_handle_context(self, e_context: EventContext):
        if e_context['context'].type != ContextType.TEXT:
            return

        content = e_context['context'].content.strip()
        logger.info(f"[MediaParser] 收到消息: {content}")
        
        if content == "清理缓存":
            result = self.clean_cache()
            e_context['reply'] = Reply(ReplyType.TEXT, result)
            e_context.action = EventAction.BREAK_PASS
            return
            
        elif content == "查看缓存":
            result = self.cache_status()
            e_context['reply'] = Reply(ReplyType.TEXT, result)
            e_context.action = EventAction.BREAK_PASS
            return

        # 检查是否是解析命令
        if content.startswith(("解析视频", "解析图集")):
            command = "解析视频" if content.startswith("解析视频") else "解析图集"
            url = content[len(command):].strip()
            
            logger.info(f"[MediaParser] 解析命令: {command}, URL: {url}")
            
            if not url:
                e_context['reply'] = Reply(ReplyType.TEXT, f"请提供要解析的链接\n例如：{command} <链接>")
                e_context.action = EventAction.BREAK_PASS
                return

            # 获取接收者信息
            receiver = e_context['context'].kwargs.get('receiver')
            if not receiver:
                logger.error("[MediaParser] 无法获取接收者信息")
                e_context['reply'] = Reply(ReplyType.TEXT, "无法获取接收者信息")
                e_context.action = EventAction.BREAK_PASS
                return
            
            logger.info(f"[MediaParser] 开始处理，接收者: {receiver}")
            
            try:
                # 提取链接中的URL
                url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
                urls = re.findall(url_pattern, url)
                
                if not urls:
                    # 尝试从文本中提取分享链接
                    share_pattern = r'复制打开抖音|快手|微博|小红书.*?(?:https?://[^\s]+)'
                    share_match = re.search(share_pattern, url)
                    if share_match:
                        share_text = share_match.group(0)
                        urls = re.findall(url_pattern, share_text)
                
                if not urls:
                    logger.error(f"[MediaParser] 未找到有效链接: {url}")
                    e_context['reply'] = Reply(ReplyType.TEXT, "未找到有效的链接，请确保链接格式正确")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                target_url = urls[0]
                logger.info(f"[MediaParser] 提取到链接: {target_url}")
                
                if command == "解析视频":
                    replies = self.parse_video(target_url)
                else:
                    task_id = f"{receiver}_{int(time.time())}"
                    replies = self.parse_images(target_url, task_id)
                
                # 确保回复被正确发送
                if isinstance(replies, list) and replies:
                    # 将第一个回复设置为主回复
                    e_context['reply'] = replies[0]
                    # 如果有更多回复，则将它们添加到reply_list中
                    if len(replies) > 1:
                        for reply in replies[1:]:
                            self.send_to_channel(reply, receiver)
                else:
                    # 如果只有一个回复或没有回复
                    e_context['reply'] = replies if replies else Reply(ReplyType.TEXT, "解析失败")
                
            except Exception as e:
                logger.error(f"[MediaParser] 解析失败: {e}", exc_info=True)
                e_context['reply'] = Reply(ReplyType.TEXT, "解析失败，请检查链接是否有效")

            e_context.action = EventAction.BREAK_PASS
            return

    def parse_video(self, url):
        """解析视频链接"""
        try:
            # 使用新的聚合解析API
            video_api = "https://www.hhlqilongzhu.cn/api/sp_jx/sp.php"
            params = {"url": url}
            
            response = self._make_request("GET", video_api, params=params)
            
            if not response or response.status_code != 200:
                msg = f"API请求失败，状态码：{response.status_code}" if response else "API无响应"
                logger.error(f"[MediaParser] 视频解析API请求失败: {msg}")
                return [Reply(ReplyType.TEXT, msg)]
            
            # 解析响应内容
            data = response.json()
            if data.get("code") != 200:
                error_msg = data.get("msg", "视频解析失败")
                logger.error(f"[MediaParser] 视频解析失败: {error_msg}")
                return [Reply(ReplyType.TEXT, error_msg)]
            
            video_data = data.get("data", {})
            if not video_data:
                logger.error("[MediaParser] 未获取到视频信息")
                return [Reply(ReplyType.TEXT, "未获取到视频信息")]

            video_url = video_data.get("url")
            if not video_url:
                logger.error("[MediaParser] 未找到视频地址")
                return [Reply(ReplyType.TEXT, "未找到视频地址")]
            
            self._check_cache_size()
            file_obj, filename = self.download_media(video_url, "video")
            if not file_obj:
                logger.error("[MediaParser] 视频下载失败")
                return [Reply(ReplyType.TEXT, "视频下载失败")]
            
            # 检查文件大小并返回提示消息
            file_size = os.path.getsize(os.path.join(self.cache_dir, filename))
            max_size = self.config["max_video_size_mb"] * 1024 * 1024
            if file_size > max_size:
                logger.info(f"[MediaParser] 视频大小为 {file_size/(1024*1024):.2f}MB，返回提示消息")
                file_obj.close()
                return [Reply(ReplyType.TEXT, f"抱歉，该视频文件大于{self.config['max_video_size_mb']}MB，暂时无法处理。请尝试分享较小的视频文件。")]
            
            # 构建详细的视频描述
            description_parts = []
            
            # 添加标题
            if video_data.get("title"):
                description_parts.append(f"🎬 标题：{video_data['title']}")
            
            # 添加作者信息（如果有）
            if video_data.get("author"):
                description_parts.append(f"👤 作者：{video_data['author']}")
            
            # 添加文本信息
            text_info = data.get("text", {})
            if text_info:
                description_parts.append(f"📝 信息：{text_info.get('msg', '')}")
                description_parts.append(f"🕒 时间：{text_info.get('time', '')}")
            
            # 组合描述
            description = "\n".join(description_parts)
            
            # 创建回复列表
            replies = []
            
            # 添加描述文本回复
            if description_parts:
                text_reply = Reply(ReplyType.TEXT, description)
                replies.append(text_reply)
            
            # 添加视频回复
            video_reply = Reply(ReplyType.VIDEO, file_obj)
            video_reply.filename = filename
            replies.append(video_reply)
            
            logger.info(f"[MediaParser] 视频解析成功，描述：{description}")
            
            return replies
        
        except Exception as e:
            logger.error(f"[MediaParser] 视频解析出错: {e}", exc_info=True)
            return [Reply(ReplyType.TEXT, "视频解析失败，请检查链接是否有效")]

    def parse_images(self, url, task_id):
        """解析图集链接"""
        try:
            # 使用新的聚合解析图集API
            images_api = "https://www.hhlqilongzhu.cn/api/sp_jx/tuji.php"
            params = {"url": url}
            
            response = self._make_request("GET", images_api, params=params)
            
            if not response or response.status_code != 200:
                msg = f"API请求失败，状态码：{response.status_code}" if response else "API无响应"
                return Reply(ReplyType.TEXT, msg)
            
            # 解析响应内容
            data = response.json()
            if data.get("code") != 200:
                return Reply(ReplyType.TEXT, data.get("msg", "图集解析失败"))
            
            image_data = data.get("data", {})
            images = image_data.get("images", [])
            
            if not images:
                return Reply(ReplyType.TEXT, "未找到图片")
            
            # 记录图片数量
            logger.info(f"[MediaParser] 获取到 {len(images)} 张图片的URL")
            
            # 准备发送的图片列表
            image_replies = []
            
            # 构建详细的图集描述
            description_parts = []
            
            # 添加作者
            if image_data.get("author"):
                description_parts.append(f"👤 作者：{image_data['author']}")
            
            # 添加标题
            if image_data.get("title"):
                description_parts.append(f"🖼️ 标题：{image_data['title']}")
            
            # 添加文本信息
            text_info = image_data.get("text", {})
            if text_info:
                description_parts.append(f"📝 信息：{text_info.get('msg', '')}")
                description_parts.append(f"🕒 时间：{text_info.get('time', '')}")
            
            # 组合描述
            description = "\n".join(description_parts)
            
            # 发送描述文本
            if description_parts:
                text_reply = Reply(ReplyType.TEXT, description)
                text_reply.receiver = task_id
                image_replies.append(text_reply)
            
            # 下载并发送图片
            for index, img_url in enumerate(images, 1):
                self._check_cache_size()
                file_obj, filename = self.download_media(img_url, "image")
                
                if file_obj:
                    # 为每张图片创建单独的图片描述
                    image_description = f"📸 图片 {index}/{len(images)}"
                    
                    # 创建文本回复
                    text_reply = Reply(ReplyType.TEXT, image_description)
                    text_reply.receiver = task_id
                    
                    # 创建图片回复
                    image_reply = Reply(ReplyType.IMAGE, file_obj)
                    image_reply.filename = filename
                    image_reply.receiver = task_id
                    
                    # 分别发送文本和图片
                    image_replies.extend([text_reply, image_reply])
            
            # 发送完成提示
            complete_reply = Reply(ReplyType.TEXT, f"图集发送完成，共 {len(images)} 张图片")
            complete_reply.receiver = task_id
            image_replies.append(complete_reply)
            
            return image_replies
        
        except Exception as e:
            logger.error(f"[MediaParser] 图集解析出错: {e}")
            return Reply(ReplyType.TEXT, "图集解析失败，请检查链接是否有效")

    def _make_request(self, method, url, **kwargs):
        """发送HTTP请求，支持重试机制"""
        timeout = self.config["download"]["timeout"]
        max_retries = self.config["download"]["max_retries"]
        retry_delay = self.config["download"]["retry_delay"]
        
        for i in range(max_retries + 1):
            try:
                logger.debug(f"[MediaParser] 发起请求: method={method}, url={url}, kwargs={kwargs}")
                
                # 使用会话发送请求
                response = self.session.request(
                    method, 
                    url, 
                    timeout=timeout, 
                    **kwargs
                )
                
                # 记录完整的响应信息
                logger.debug(f"[MediaParser] 响应状态码: {response.status_code}")
                logger.debug(f"[MediaParser] 响应头: {dict(response.headers)}")
                
                # 如果是 JSON 请求，记录 JSON 内容
                if not kwargs.get('stream'):  # 只在非流式请求时尝试解析JSON
                    try:
                        json_data = response.json()
                        logger.debug(f"[MediaParser] 响应 JSON: {json_data}")
                    except Exception as json_error:
                        logger.debug(f"[MediaParser] 解析 JSON 失败: {json_error}")
                
                # 检查响应状态码
                if response.status_code == 200:
                    return response
                
                logger.warning(f"[MediaParser] 请求失败，状态码: {response.status_code}")
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"[MediaParser] 请求失败，将在 {retry_delay} 秒后重试（第 {i+1} 次）: {e}")
                
                if i == max_retries:
                    logger.error(f"[MediaParser] 请求最终失败: {e}")
                    return None
                
                time.sleep(retry_delay)
            
            except Exception as e:
                logger.error(f"[MediaParser] 未知错误: {e}")
                return None

    def download_media(self, url, media_type="video"):
        """下载媒体文件，支持视频和图片"""
        try:
            logger.info(f"[MediaParser] 开始下载{media_type}: {url}")
            response = self._make_request("GET", url, stream=True)
            if not response:
                logger.error(f"[MediaParser] 下载失败: 无法获取响应")
                return None, None

            # 获取Content-Type和文件大小
            content_type = response.headers.get('content-type', '').lower()
            content_length = response.headers.get('content-length')
            logger.info(f"[MediaParser] 文件MIME类型: {content_type}, 预期大小: {content_length} bytes")
            
            # 验证Content-Type
            if media_type == "video" and content_type not in self.video_extensions:
                logger.error(f"[MediaParser] 不支持的视频类型: {content_type}")
                return None, None
            elif media_type == "image" and content_type not in self.image_extensions:
                logger.error(f"[MediaParser] 不支持的图片类型: {content_type}")
                return None, None

            # 根据媒体类型选择扩展名
            if media_type == "video":
                extension = self.video_extensions.get(content_type, '.mp4')
            else:
                extension = self.image_extensions.get(content_type, '.jpg')

            # 使用时间戳和随机数生成唯一文件名
            filename = f"{int(time.time())}_{hash(url)}{extension}"
            filepath = os.path.join(self.cache_dir, filename)

            # 使用二进制模式写入文件
            total_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.config["cache"]["chunk_size"]):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)

            logger.info(f"[MediaParser] 文件下载成功: {filename}")
            logger.info(f"[MediaParser] 文件路径: {filepath}")
            logger.info(f"[MediaParser] 文件大小: {self.format_size(total_size)}")

            # 确保文件权限正确
            try:
                os.chmod(filepath, 0o644)
            except Exception as e:
                logger.warning(f"[MediaParser] 设置文件权限失败: {e}")

            # 打开文件用于读取
            file_obj = open(filepath, 'rb')
            return file_obj, filename

        except Exception as e:
            logger.error(f"[MediaParser] 下载媒体文件失败: {e}")
            import traceback
            logger.error(f"[MediaParser] 错误追踪: {traceback.format_exc()}")
            return None, None

    def close_file(self, file_obj):
        """安全地关闭文件对象"""
        try:
            if hasattr(file_obj, '_filepath'):
                filepath = getattr(file_obj, '_filepath')
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        logger.debug(f"[MediaParser] 删除缓存文件: {filepath}")
                    except Exception as e:
                        logger.warning(f"[MediaParser] 删除缓存文件失败: {e}")
            if hasattr(file_obj, 'close'):
                file_obj.close()
        except Exception as e:
            logger.error(f"[MediaParser] 关闭文件失败: {e}")

    def _clear_expired_cache(self):
        """清理过期缓存"""
        try:
            logger.info("[MediaParser] 开始清理过期缓存")
            max_age = self.config["cache"]["max_age_hours"] * 3600  # 转换为秒
            current_time = time.time()
            removed_count = 0
            removed_size = 0
            
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                if os.path.isfile(filepath):
                    # 检查文件年龄
                    file_age = current_time - os.path.getctime(filepath)
                    if file_age > max_age:
                        try:
                            size = os.path.getsize(filepath)
                            os.remove(filepath)
                            removed_count += 1
                            removed_size += size
                            logger.info(f"[MediaParser] 删除过期文件: {filepath}, 年龄: {int(file_age/3600)}小时")
                        except Exception as e:
                            logger.error(f"[MediaParser] 删除过期文件失败: {filepath}, 错误: {e}")
            
            if removed_count > 0:
                logger.info(f"[MediaParser] 清理完成，删除了 {removed_count} 个文件，总大小: {self.format_size(removed_size)}")
            else:
                logger.info("[MediaParser] 没有发现过期文件")
                
        except Exception as e:
            logger.error(f"[MediaParser] 清理过期缓存失败: {e}", exc_info=True)

    def _check_cache_size(self):
        """检查并控制缓存大小"""
        try:
            logger.info("[MediaParser] 开始检查缓存大小")
            total_size = 0
            files = []
            
            # 获取所有缓存文件信息
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                if os.path.isfile(filepath):
                    size = os.path.getsize(filepath)
                    ctime = os.path.getctime(filepath)
                    files.append((filepath, size, ctime))
                    total_size += size
            
            max_size = self.config["cache"]["max_size_mb"] * 1024 * 1024  # 转换为字节
            logger.info(f"[MediaParser] 当前缓存大小: {self.format_size(total_size)}, 最大限制: {self.format_size(max_size)}")
            
            if total_size > max_size:
                # 按创建时间排序，删除最旧的文件
                files.sort(key=lambda x: x[2])  # 按创建时间排序
                
                # 删除文件直到缓存大小小于限制
                while total_size > max_size and files:
                    filepath, size, _ = files.pop(0)
                    try:
                        os.remove(filepath)
                        total_size -= size
                        logger.info(f"[MediaParser] 删除缓存文件: {filepath}, 大小: {self.format_size(size)}")
                    except Exception as e:
                        logger.error(f"[MediaParser] 删除缓存文件失败: {filepath}, 错误: {e}")
                
                logger.info(f"[MediaParser] 清理后的缓存大小: {self.format_size(total_size)}")
            
        except Exception as e:
            logger.error(f"[MediaParser] 检查缓存大小失败: {e}", exc_info=True)
            
    def clean_cache(self):
        """清理所有缓存"""
        try:
            with self.cache_lock:
                files = [f for f in os.listdir(self.cache_dir) 
                        if os.path.isfile(os.path.join(self.cache_dir, f))]
                total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) 
                               for f in files)
                
                for f in files:
                    try:
                        os.remove(os.path.join(self.cache_dir, f))
                    except OSError as e:
                        logger.error(f"[MediaParser] 删除文件失败: {e}")
                        
            return f"缓存已清理\n清理前：{len(files)}个文件，{self.format_size(total_size)}"
                
        except Exception as e:
            logger.error(f"[MediaParser] 清理缓存失败: {e}")
            return "清理缓存失败，请稍后重试"

    def cache_status(self):
        """获取缓存状态"""
        try:
            with self.cache_lock:
                files = [f for f in os.listdir(self.cache_dir) 
                        if os.path.isfile(os.path.join(self.cache_dir, f))]
                total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) 
                               for f in files)
                
                max_size = self.config["cache"]["max_size_mb"]
                max_age = self.config["cache"]["max_age_hours"]
                
                status = f"缓存状态：\n"
                status += f"文件数：{len(files)}\n"
                status += f"占用空间：{self.format_size(total_size)}\n"
                status += f"最大空间：{max_size}MB\n"
                status += f"过期时间：{max_age}小时"
                
                return status
                
        except Exception as e:
            logger.error(f"[MediaParser] 获取缓存状态失败: {e}")
            return "获取缓存状态失败，请稍后重试"

    def format_size(self, size):
        """格式化文件大小"""
        units = ['B', 'KB', 'MB', 'GB']
        unit_index = 0
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        return f"{size:.2f} {units[unit_index]}"

    def _process_pending_tasks(self):
        """后台线程处理待发送的任务"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                tasks_to_remove = []
                
                with self.tasks_lock:
                    for task_id, task in self.processing_tasks.items():
                        if current_time >= task['next_send_time']:
                            # 获取下一个要发送的回复
                            reply = task['replies'][task['index']]
                            receiver = task['receiver']  # 从任务中获取接收者信息
                            
                            try:
                                # 发送回复
                                self.send_to_channel(reply, receiver)
                                logger.debug(f"[MediaParser] 发送成功: {reply}")
                                
                                # 更新任务状态
                                task['index'] += 1
                                if task['index'] >= len(task['replies']):
                                    # 所有回复都已发送完成
                                    tasks_to_remove.append(task_id)
                                    logger.info(f"[MediaParser] 任务完成: {task_id}")
                                else:
                                    # 设置下一次发送时间
                                    task['next_send_time'] = current_time + self.config["batch"]["delay_seconds"]
                                    
                            except Exception as e:
                                logger.error(f"[MediaParser] 发送失败: {e}")
                                # 发送失败时也移除任务
                                tasks_to_remove.append(task_id)
                    
                    # 清理已完成的任务
                    for task_id in tasks_to_remove:
                        task = self.processing_tasks.pop(task_id)
                        # 清理相关的文件对象
                        if 'replies' in task:
                            self.clean_up_files(task['replies'])
                
            except Exception as e:
                logger.error(f"[MediaParser] 处理任务出错: {e}")
            
            # 短暂休眠以避免过度占用CPU
            time.sleep(0.1)

    def send_reply(self, reply):
        """发送Reply对象的辅助方法"""
        try:
            # 假设 context 中包含 receiver 信息
            receiver = reply.receiver  # 确保 Reply 对象包含 receiver 属性
            logger.debug(f"[MediaParser] 发送Reply类型: {reply.type}, 内容: {reply.content}")
            self.send_to_channel(reply, receiver)
            # 发送完成后关闭文件对象
            self.clean_up_files([reply])
        except Exception as e:
            logger.error(f"[MediaParser] 发送Reply失败: {e}")

    def send_to_channel(self, reply, receiver):
        """发送Reply对象到目标频道"""
        try:
            from channel.channel_factory import create_channel
            from bridge.context import Context
            from bridge.reply import ReplyType
            
            logger.info(f"[MediaParser] 准备发送Reply: 类型={reply.type}, 接收者={receiver}")
            
            channel = create_channel("wx")
            if channel:
                context = Context()
                context.kwargs = {'receiver': receiver}
                
                if reply.type in [ReplyType.IMAGE, ReplyType.VIDEO]:
                    # 确保文件存在且可读
                    if hasattr(reply.content, 'name'):
                        if not os.path.exists(reply.content.name):
                            logger.error(f"[MediaParser] 文件不存在: {reply.content.name}")
                            return
                        
                        # 重新打开文件以确保它是可读的
                        try:
                            reply.content.close()
                            reply.content = open(reply.content.name, 'rb')
                        except Exception as e:
                            logger.error(f"[MediaParser] 重新打开文件失败: {e}")
                            return
                
                try:
                    channel.send(reply, context)
                    logger.info(f"[MediaParser] 发送成功: {reply}")
                except Exception as send_error:
                    logger.error(f"[MediaParser] 发送失败: {send_error}")
                    import traceback
                    logger.error(f"[MediaParser] 错误追踪: {traceback.format_exc()}")
                finally:
                    # 确保文件被关闭
                    if reply.type in [ReplyType.IMAGE, ReplyType.VIDEO]:
                        try:
                            reply.content.close()
                        except:
                            pass
            else:
                logger.error("[MediaParser] 未找到微信channel")
                raise RuntimeError("未找到微信channel")
        
        except Exception as e:
            logger.error(f"[MediaParser] 发送失败: {e}")
            import traceback
            logger.error(f"[MediaParser] 错误追踪: {traceback.format_exc()}")

    def clean_up_files(self, reply_list):
        """在所有回复发送完成后关闭文件对象"""
        for reply in reply_list:
            if reply.type in [ReplyType.IMAGE, ReplyType.VIDEO]:
                try:
                    self.close_file(reply.content)
                except Exception as e:
                    logger.error(f"[MediaParser] 关闭文件失败: {e}")

    def __del__(self):
        """清理资源"""
        try:
            self.stop_event.set()
            self.worker_thread.join(timeout=5)
            self.session.close()
            self.executor.shutdown(wait=False)
        except Exception as e:
            logger.error(f"[MediaParser] 关闭资源失败: {e}")
