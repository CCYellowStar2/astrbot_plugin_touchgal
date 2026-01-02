import aiohttp
import aiofiles
import aiofiles.os
import json
import re
import os
import asyncio
import time
from datetime import datetime, timezone
from dateutil import parser, tz
import stat as os_stat
from datetime import datetime, timedelta
import hashlib
from typing import Dict, List, Union, Any, Tuple, Optional
from PIL import Image, UnidentifiedImageError
import astrbot.api.message_components as Comp
from astrbot.api.message_components import Node, Plain, Image as CompImage
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api.all import AstrBotConfig
from astrbot.api import logger

# 自定义异常类
class NoGameFound(Exception): pass
class DownloadNotFound(Exception): pass
class APIError(Exception): pass
class ImageProcessingError(Exception): pass

# 检查是否支持AVIF格式

from pillow_avif import AvifImagePlugin
AVIF_SUPPORT = True
logger.info("AVIF格式支持已启用")


# 创建定时任务管理器
class Scheduler:
    def __init__(self):
        self.tasks = []
    
    async def schedule_daily(self, hour, minute, callback):
        """安排每天特定时间执行的任务"""
        async def task_loop():
            while True:
                now = datetime.now()
                # 计算下一个执行时间
                next_run = datetime(
                    now.year, now.month, now.day,
                    hour, minute
                )
                if next_run < now:
                    next_run += timedelta(days=1)
                
                # 计算等待时间（秒）
                wait_seconds = (next_run - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                # 执行任务
                try:
                    await callback()
                except Exception as e:
                    logger.error(f"定时任务执行失败: {str(e)}")
        
        # 启动任务
        self.tasks.append(asyncio.create_task(task_loop()))
    
    async def cancel_all(self):
        """取消所有定时任务"""
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

"""TouchGal API接口封装"""
class TouchGalAPI:
    def __init__(self):
        self.base_url = "https://www.touchgal.top/api"
        self.search_url = f"{self.base_url}/search"
        self.download_url = f"{self.base_url}/patch/resource"
        self.temp_dir = StarTools.get_data_dir("astrbot_plugin_touchgal") / "tmp"
        self.semaphore = asyncio.Semaphore(10)  # 添加信号量限制并发API请求
        
    async def search_game(self, keyword: str, limit: int, nsfw: bool) -> List[Dict[str, Any]]:
        """搜索游戏信息"""
        async with self.semaphore:
            headers = {"Content-Type": "application/json"}
            
            # 正确构造queryString参数（字符串格式的JSON数组）
            query_string = json.dumps([{"type": "keyword", "name": keyword}])
            
            payload = {
                "queryString": query_string,  # 使用字符串格式的JSON
                "limit": limit,
                "searchOption": {
                    "searchInIntroduction": True,
                    "searchInAlias": True,
                    "searchInTag": True
                },
                "page": 1,
                "selectedType": "all",
                "selectedLanguage": "all",
                "selectedPlatform": "all",
                "sortField": "resource_update_time",
                "sortOrder": "desc",
                "selectedYears": ["all"],  # 添加缺失的必需字段
                "selectedMonths": ["all"]  # 添加缺失的必需字段
            }
            cookies = {
                "kun-patch-setting-store|state|data|kunNsfwEnable": "sfw"
            }
            if nsfw:
                cookies = {
                    "kun-patch-setting-store|state|data|kunNsfwEnable": "all"
                }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.search_url, 
                        json=payload, 
                        headers=headers,
                        cookies=cookies,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as response:
                        # 确保响应状态为200
                        if response.status != 200:
                            error_text = await response.text()
                            raise APIError(f"API请求失败: {response.status} - {error_text}")
                        
                        # 尝试解析JSON
                        try:
                            data = await response.json()
                        except Exception as e:
                            text_response = await response.text()
                            logger.error(f"JSON解析失败: {str(e)} - 响应内容: {text_response[:200]}")
                            raise APIError("API返回了无效的JSON数据")
                        
                        # 验证数据结构
                        if not isinstance(data, dict) or "galgames" not in data:
                            logger.warning(f"API返回了意外的数据结构: {data}")
                            raise APIError("API返回了无效的数据结构")
                        
                        if not data.get("galgames"):
                            raise NoGameFound(f"未找到游戏: {keyword}")
                        
                        return data["galgames"]
            except aiohttp.ClientError as e:
                raise APIError(f"网络请求错误: {str(e)}")

    async def get_downloads(self, patch_id: Union[int, str]) -> List[Dict[str, Any]]:
        """获取游戏下载资源"""
        async with self.semaphore:
            params = {"patchId": patch_id}
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.download_url, 
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            raise APIError(f"API请求失败: {response.status} - {error_text}")
                        
                        # 尝试解析JSON
                        try:
                            data = await response.json()
                        except Exception as e:
                            text_response = await response.text()
                            logger.error(f"JSON解析失败: {str(e)} - 响应内容: {text_response[:200]}")
                            raise APIError("API返回了无效的JSON数据")
                        
                        # 验证数据结构
                        if not isinstance(data, list):
                            logger.warning(f"API返回了意外的数据结构: {data}")
                            raise APIError("API返回了无效的数据结构")
                        
                        if not data:
                            raise DownloadNotFound(f"未找到ID为{patch_id}的下载资源")
                        
                        return data
            except aiohttp.ClientError as e:
                raise APIError(f"网络请求错误: {str(e)}")
    
    async def download_and_convert_image(self, url: str) -> Union[str, None]:
        """
        下载并转换图片为JPG格式
        支持AVIF格式转换（如果安装了pillow-avif-plugin）
        """
        async with self.semaphore:
            if not url:
                return None
                
            # 生成唯一的文件名（使用URL的MD5避免重复下载）
            url_hash = hashlib.md5(url.encode()).hexdigest()
            filepath = str(self.temp_dir / f"main_{url_hash}")
            output_path = str(self.temp_dir /  f"converted_{url_hash}.jpg")
            
            # 如果已经转换过，直接返回
            if await async_exists(output_path):
                return output_path
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            logger.warning(f"获取图片失败: {response.status} - {url}")
                            return None
                        
                        # 检查图片类型
                        content_type = response.headers.get('Content-Type', '').split(';')[0].strip().lower()
                        
                        # 写入原始图片
                        async with aiofiles.open(filepath, "wb") as f:
                            await f.write(await response.read())
                        
                        # 处理图片转换
                        result = await self._convert_image(filepath, output_path)
                        if result is None:
                            # 转换失败，清理可能已创建的文件
                            if await async_exists(output_path):
                                await aiofiles.os.remove(output_path)
                        return result
                        
            except Exception as e:
                logger.warning(f"图片处理失败: {str(e)} - {url}")
                if await async_exists(output_path):
                    await aiofiles.os.remove(output_path)
                return None
            finally:
                # 清理原始文件
                if await async_exists(filepath):
                    try:
                        await aiofiles.os.remove(filepath)
                    except Exception as e:
                        logger.warning(f"删除原始图片失败: {str(e)}")
    
    async def _convert_image(self, input_path: str, output_path: str) -> str:
        """转换图片为JPG格式"""
        try:
            # 在异步环境中处理图片转换
            def convert_image():
                with Image.open(input_path) as img:
                    # 转换为RGB模式（JPG需要）
                    if img.mode != "RGB":
                        img = img.convert("RGB")
                    
                    # 调整图片大小（避免过大）
                    max_size = (800, 800)
                    img.thumbnail(max_size, Image.BILINEAR)
                    
                    # 保存为JPG
                    img.save(output_path, "JPEG", quality=85)
                return output_path
            
            # 在线程池中执行同步的图片处理
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, convert_image)
        except UnidentifiedImageError:
            # 如果是AVIF格式但未安装支持库
            if AVIF_SUPPORT:
                logger.warning(f"无法识别的图片格式: {input_path}")
            else:
                logger.warning("检测到AVIF格式但未安装支持库，无法转换")
            return None
        except Exception as e:
            logger.warning(f"图片转换失败: {str(e)}")
            return None

# 高效缓存管理类
class AsyncGameCache:
    """异步游戏缓存管理器，避免锁操作"""
    def __init__(self, max_size: int = 1000, ttl: int = 86400):
        self._cache: Dict[int, Dict] = {}
        self._expiry_times: Dict[int, float] = {}
        self._access_times: Dict[int, float] = {}
        self._max_size = max_size
        self._ttl = ttl
        self._cache_order = []  # 按访问时间排序的缓存ID列表
        self._lock = asyncio.Lock()  # 添加异步锁
        
    async def add(self, game_id: int, game_info: Dict):
        """添加游戏到缓存"""
        async with self._lock:  # 使用异步锁保护关键操作
            current_time = time.time()
            
            # 如果缓存已满，移除最旧的项目
            if len(self._cache) >= self._max_size and self._cache_order:
                oldest_id = self._cache_order.pop(0)
                if oldest_id in self._cache:
                    del self._cache[oldest_id]
                if oldest_id in self._expiry_times:
                    del self._expiry_times[oldest_id]
                if oldest_id in self._access_times:
                    del self._access_times[oldest_id]
            
            # 添加新项目
            self._cache[game_id] = game_info
            self._expiry_times[game_id] = current_time + self._ttl
            self._access_times[game_id] = current_time
            
            # 确保ID在缓存顺序列表中（如果已存在则先移除）
            if game_id in self._cache_order:
                self._cache_order.remove(game_id)
            self._cache_order.append(game_id)
            
            # 确保缓存顺序列表不会过大
            if len(self._cache_order) > self._max_size * 2:
                self._cache_order = [id for id in self._cache_order if id in self._cache]
    
    async def get(self, game_id: int) -> Optional[Dict]:
        """从缓存获取游戏信息"""
        async with self._lock:  # 使用异步锁保护关键操作
            current_time = time.time()
            
            # 检查缓存是否过期
            if game_id in self._expiry_times and current_time > self._expiry_times[game_id]:
                # 如果过期，移除缓存项
                if game_id in self._cache:
                    del self._cache[game_id]
                if game_id in self._expiry_times:
                    del self._expiry_times[game_id]
                if game_id in self._access_times:
                    del self._access_times[game_id]
                # 同时从缓存顺序列表中移除
                if game_id in self._cache_order:
                    self._cache_order.remove(game_id)
                return None
            
            # 更新访问时间
            if game_id in self._cache:
                self._access_times[game_id] = current_time
                # 更新缓存顺序：移动到列表末尾表示最近访问
                if game_id in self._cache_order:
                    self._cache_order.remove(game_id)
                self._cache_order.append(game_id)
                return self._cache[game_id]
            
            return None
    
    async def cleanup(self):
        """清理过期缓存"""
        async with self._lock:  # 使用异步锁保护关键操作
            current_time = time.time()
            expired_ids = []
            
            # 收集所有过期ID
            for game_id, expiry_time in self._expiry_times.items():
                if current_time > expiry_time:
                    expired_ids.append(game_id)
            
            # 清理每个过期ID
            for game_id in expired_ids:
                if game_id in self._cache:
                    del self._cache[game_id]
                if game_id in self._expiry_times:
                    del self._expiry_times[game_id]
                if game_id in self._access_times:
                    del self._access_times[game_id]
                # 确保从缓存顺序列表中移除
                if game_id in self._cache_order:
                    self._cache_order.remove(game_id)
        
            # 清理缓存顺序列表
            self._cache_order = [id for id in self._cache_order if id in self._cache]

@register(
    "astrbot_plugin_touchgal",
    "CCYellowStar2",
    "基于TouchGal API的Galgame信息查询与下载插件",
    "1.2",
    "https://github.com/CCYellowStar2/astrbot_plugin_touchgal"
)
class TouchGalPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.search_limit = self.config.get("search_limit", 15)
        self.enable_nsfw = self.config.get("enable_nsfw", False)
        # 使用异步缓存管理
        self.game_cache = AsyncGameCache(max_size=1000, ttl=86400)

        self.api = TouchGalAPI()
        self.temp_dir = StarTools.get_data_dir("astrbot_plugin_touchgal") / "tmp"
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # 初始化定时任务
        self.scheduler = Scheduler()
        
        # 启动清理任务
        asyncio.create_task(self.start_daily_cleanup())

        # 启动时清理旧缓存
        asyncio.create_task(self.cleanup_old_cache())

        # 启动定期缓存清理并保存任务引用
        self.periodic_task = asyncio.create_task(self.periodic_cache_cleanup())

    async def start_daily_cleanup(self):
        """启动每日清理任务"""
        # 安排在每天00:00执行清理
        await self.scheduler.schedule_daily(0, 0, self.cleanup_old_cache)
        logger.info("已启动每日00:00自动清理图片缓存任务")

    async def periodic_cache_cleanup(self):
        """定期清理缓存（每60分钟一次）"""
        try:
            while True:
                await self.game_cache.cleanup()
                logger.debug("缓存清理完成")
                await asyncio.sleep(3600)  # 60分钟
        except asyncio.CancelledError:
            logger.info("定期缓存清理任务已被取消")
            raise

    async def cleanup_old_cache(self , max_age_days: int = 1, batch_size: int = 100):
        """异步清理过期缓存文件（流式处理）"""
        cache_dir = str(self.temp_dir)
        logger.info(f"开始异步清理缓存目录: {cache_dir}")
        
        # 计算过期时间阈值
        max_age_seconds = max_age_days * 24 * 60 * 60
        current_time = time.time()
        
        # 使用异步迭代器
        deleted_count = 0
        batch_count = 0
        
        try:
            # 使用异步目录遍历
            async for file_path in self._async_walk(cache_dir):
                try:
                    # 异步获取文件状态
                    stat = await aiofiles.os.stat(file_path)
                    
                    # 检查是否过期
                    if current_time - stat.st_mtime > max_age_seconds:
                        # 异步删除文件
                        await aiofiles.os.remove(file_path)
                        deleted_count += 1
                        batch_count += 1
                        
                        # 批量处理日志
                        if batch_count >= batch_size:
                            logger.debug(f"已删除 {batch_count} 个过期缓存文件")
                            batch_count = 0
                            # 短暂释放事件循环
                            await asyncio.sleep(0)
                
                except FileNotFoundError:
                    # 文件可能已被其他进程删除
                    pass
                except Exception as e:
                    logger.warning(f"处理文件失败: {file_path}, 原因: {e}")
            
            # 记录最后一批删除
            if batch_count > 0:
                logger.debug(f"已删除 {batch_count} 个过期缓存文件")
        
        except Exception as e:
            logger.error(f"异步清理缓存失败: {e}")
        
        
        logger.info(f"缓存清理完成，共删除 {deleted_count} 个过期文件")
        return deleted_count

    async def _async_walk(self, directory: str):
        """异步生成目录中的所有文件路径"""
        # 使用递归异步遍历
        try:
            # 获取目录内容
            entries = await aiofiles.os.listdir(directory)
            for entry in entries:
                full_path = os.path.join(directory, entry)
                
                # 检查文件状态
                stat_info = await aiofiles.os.stat(full_path)
                
                if os_stat.S_ISDIR(stat_info.st_mode):  # 目录
                    # 递归遍历子目录
                    async for sub_path in self._async_walk(full_path):
                        yield sub_path
                else:  # 文件
                    yield full_path
        except Exception as e:
            logger.warning(f"遍历目录失败: {directory}, 原因: {e}")
    
    def _format_game_info(self, game_info: Dict[str, Any]) -> str:
        """格式化游戏信息（未使用）"""
        # 处理标签
        tags = ", ".join(game_info.get("tags", []))
        if len(tags) > 100:  # 防止标签过长
            tags = tags[:97] + "..."
            
        # 处理平台
        platforms = ", ".join(game_info.get("platform", []))
        
        # 处理日期
        created_date = game_info.get("created", "")[:10]
        
        return (
            f"🆔 游戏ID: {game_info['id']}\n"
            f"🎮 名称: {game_info['name']}\n"
            f"🏷️ 标签: {tags}\n"
            f"📱 平台: {platforms}\n"
            f"⬇️ 下载次数: {game_info.get('download', 0)}\n"
            f"📅 添加日期: {created_date}\n"
            f"🔍 使用 '/下载gal {game_info['id']}' 获取下载地址"
        )

    def _relative_time(self, date_str: str) -> str:
        """将时间字符串转换为上海时间相对描述"""

            # 预处理字符串 - 移除时区名称部分
        cleaned_str = re.sub(r'\([^)]*\)', '', date_str).strip()
        
        
        # 解析时间字符串并转换为上海时间
        dt = parser.parse(cleaned_str)
        
        # 获取当前时间的时间戳
        current_ts = time.time()
        # 获取目标时间的时间戳
        target_ts = time.mktime(dt.timetuple()) + dt.microsecond/1e6
        
        # 计算时间差（秒）
        seconds = current_ts - target_ts
        
        # 转换为相对时间描述
        if seconds < 60:
            return "刚刚"
        elif seconds < 3600:  # 1小时内
            minutes = int(seconds // 60)
            return f"{minutes}分钟前"
        elif seconds < 86400:  # 24小时内
            hours = int(seconds // 3600)
            return f"{hours}小时前"
        elif seconds < 2592000:  # 30天内
            days = int(seconds // 86400)
            return f"{days}天前"
        elif seconds < 31536000:  # 365天内
            months = int(seconds // 2592000)
            return f"{months}个月前"
        else:
            years = int(seconds // 31536000)
            return f"{years}年前"



    def _format_downloads(self, downloads: List[Dict[str, Any]]) -> str:
        """格式化下载资源信息"""
        result = []
        for i, resource in enumerate(downloads, 1):
            # 确定平台类型
            if "windows" in resource["platform"]:
                platform = "💻 PC"
            elif "android" in resource["platform"]:
                platform = "📱 手机"
            else:
                platform = "🕹️ 其他"
                
            # 获取发布时间并转换为相对时间
            created_time = resource.get('created', '')
            relative_time_str = self._relative_time(created_time) if created_time else "未知时间"
            
            # 构建资源信息的多行字符串
            resource_info = [
                f"{i}. {platform}版: {resource['name']}",
                f"   📦 大小: {resource['size']}",
                f"   🔗 下载地址: {resource['content']}",
                f"      提取码: {resource['code'] or '无'}",
                f"      解压码: {resource['password'] or '无'}",
                f"      语言: {', '.join(resource['language'])}",
                f"   🕒 发布时间: {relative_time_str}",  # 添加发布时间行
                f"   📝 备注: {resource['note'] or '无'}"
            ]
            # 将资源信息列表中的字符串用换行连接
            result.append("\n".join(resource_info))
        
        # 每个资源信息之间用换行分隔
        return "\n\n".join(result)

    @filter.command("查询gal")
    async def search_galgame(self, event: AstrMessageEvent):
        """查询Gal信息（包含封面图片）"""
        cmd = event.message_str.split(maxsplit=1)
        if len(cmd) < 2:
            yield event.plain_result("⚠️ 参数错误，请输入游戏名称")
            return

        keyword = cmd[1]
        user_id = event.get_sender_id()
              
        try:
            yield event.plain_result(f"🔍 正在搜索: {keyword}")
            results = await self.api.search_game(keyword, self.search_limit,self.enable_nsfw)            
            
            # 并发下载所有封面图片
            cover_tasks = []
            for game in results:
                # 缓存游戏信息
                game_id = game['id']
                # 使用优化后的方法添加到缓存
                await self.game_cache.add(game_id, game)
                
                if game.get("banner"):
                    cover_tasks.append(self.api.download_and_convert_image(game["banner"]))
                else:
                    cover_tasks.append(None)  # 如果没有封面，添加None占位
            
            # 等待所有图片下载完成
            cover_paths = await asyncio.gather(*cover_tasks, return_exceptions=True)
            
            # 构建消息链
            chain = []
            
            # 添加搜索结果标题
            response_lines = [f"🔍 找到 {len(results)} 个相关游戏:\n‎"]
            chain.append(Plain(response_lines[0]))
            # 为每个游戏添加图片和信息
            for i, (game, cover_path) in enumerate(zip(results, cover_paths), 1):
                # 添加游戏信息
                game_info = [
                    f"{i}. 🆔 {game['id']}: {game['name']}",
                    f"(平台: {', '.join(game['platform'])})",
                    f"(语言: {', '.join(game['language'])})"
                ]
                chain.append(Plain("\n".join(game_info)))
                # 添加封面图片（如果有）
                if cover_path and not isinstance(cover_path, Exception) and await async_exists(cover_path):
                    chain.append(CompImage.fromFileSystem(cover_path))
                
            
            # 添加提示文本
            chain.append(Plain("\n📌 使用 '/下载gal <游戏ID>' 获取下载地址"))
            
            if len(results) > 1:
                node = Node(
                    uin=3974507586,
                    name="玖玖瑠",
                    content=chain
                )
                yield event.chain_result([node])
            else:
                # 发送消息
                yield event.chain_result(chain)
                
        except NoGameFound as e:
            yield event.plain_result(f"⚠️ {str(e)}")
        except APIError as e:
            logger.error(f"API请求错误: {str(e)}")
            yield event.plain_result("⚠️ 搜索失败，请稍后再试")
        except Exception as e:
            logger.error(f"未知错误: {type(e).__name__}: {str(e)}")
            yield event.plain_result("⚠️ 发生未知错误，请稍后再试")

    @filter.command("下载gal")
    async def download_galgame(self, event: AstrMessageEvent):
        """获取游戏下载地址（包含封面图片）"""
        cmd = event.message_str.split(maxsplit=1)
        if len(cmd) < 2:
            yield event.plain_result("⚠️ 参数错误，请输入游戏ID")
            return
            
        game_id = cmd[1]
        user_id = event.get_sender_id()
        
        try:
            # 验证ID格式
            if not game_id.isdigit():
                raise ValueError("游戏ID必须是数字")
                
            game_id = int(game_id)
            
            # 尝试从缓存获取游戏信息
            game_info = await self.game_cache.get(game_id)
                        
            # 获取游戏封面图片
            cover_image_path = None
            if game_info and game_info.get("banner"):
                try:
                    cover_image_path = await self.api.download_and_convert_image(game_info["banner"])
                except Exception as e:
                    logger.error(f"封面图处理失败: {str(e)}")
            
            yield event.plain_result(f"🔍 正在获取ID:{game_id}的下载资源...")
            downloads = await self.api.get_downloads(game_id)
            
            # 格式化结果
            game_name = game_info["name"] if game_info else f"ID:{game_id}"
            result = [
                f"🎮 游戏: {game_name} (ID: {game_id})",
                f"⬇️ 找到 {len(downloads)} 个下载资源:",
                self._format_downloads(downloads)
            ]
            
            # 构建消息链
            chain = []
            
            # 添加封面图片（如果有）
            if cover_image_path and await async_exists(cover_image_path):
                chain.append(CompImage.fromFileSystem(cover_image_path))
            
            # 添加文本内容
            chain.append(Plain("\n".join(result)))
            
            # 发送消息
            if len(downloads) > 1:
                node = Node(
                    uin=3974507586,
                    name="玖玖瑠",
                    content=chain
                )
                yield event.chain_result([node])
            else:
                # 发送消息
                yield event.chain_result(chain)
            
        except ValueError as e:
            yield event.plain_result(f"⚠️ {str(e)}")
        except DownloadNotFound as e:
            yield event.plain_result(f"⚠️ {str(e)}")
        except APIError as e:
            logger.error(f"API请求错误: {str(e)}")
            yield event.plain_result("⚠️ 下载查询失败，请稍后再试")
        except Exception as e:
            logger.error(f"未知错误: {type(e).__name__}: {str(e)}")
            yield event.plain_result("⚠️ 发生未知错误，请稍后再试")

    async def terminate(self):
        """插件终止时清理资源"""
        await self.scheduler.cancel_all()
        # 取消定期缓存清理任务
        if hasattr(self, 'periodic_task') and not self.periodic_task.done():
            self.periodic_task.cancel()
            try:
                await self.periodic_task
            except asyncio.CancelledError:
                pass
        await self.cleanup_old_cache()
        logger.info("TouchGal插件已终止，用户缓存已清空")

async def async_exists(path):
    """异步检查文件是否存在"""
    try:
        await aiofiles.os.stat(path)
        return True
    except FileNotFoundError:
        return False
