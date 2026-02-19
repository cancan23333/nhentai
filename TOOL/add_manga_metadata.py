# coding: utf-8
"""
为没有元数据的漫画ZIP包添加元数据

更新日志:
- 新增: --retry 参数，仅重试失败的任务
- 新增: 安全退出机制 (Ctrl+C)，等待当前进行中的任务完成后再退出
"""

import os
import sys
import re
import json
import zipfile
import argparse
import time
import logging
import platform
import hashlib
import threading
import shutil
import tempfile
import signal  # 新增: 用于处理信号
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

try:
    from bs4 import BeautifulSoup
    from curl_cffi import requests
except ImportError:
    print("错误: 缺少必要的依赖库")
    print("安装命令: pip install beautifulsoup4 curl-cffi")
    sys.exit(1)


# ==================== 常量定义 ====================
# (保持不变)
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tiff', '.ico'}
SUPPORTED_EXTENSIONS = {'.zip', '.cbz'}


# ==================== 日志模块 ====================
# (Logger 类保持不变，此处省略以节省空间，请保留原有的 Logger 类代码)
class Logger:
    """简化的日志系统"""
    
    def __init__(self, debug_mode: bool = False, debug_log_file: Optional[str] = None):
        self.debug_mode = debug_mode
        self.debug_log_file = debug_log_file
        self.debug_file = None
        self.setup_logging()
    
    def setup_logging(self):
        """配置日志"""
        logging.basicConfig(
            level=logging.DEBUG if self.debug_mode else logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger('manga_metadata')
        
        if self.debug_mode and self.debug_log_file:
            try:
                self.debug_file = open(self.debug_log_file, 'a', encoding='utf-8')
                self.debug_file.write(f"\n\n{'='*80}\n")
                self.debug_file.write(f"Debug Session Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.debug_file.write(f"{'='*80}\n\n")
                self.debug_file.flush()
            except Exception as e:
                print(f"警告: 无法创建debug日志文件: {e}")
    
    def info(self, msg: str):
        with self._get_lock():
            self.logger.info(msg)
            self._write_debug(f"[INFO] {msg}")
    
    def warning(self, msg: str):
        with self._get_lock():
            self.logger.warning(msg)
            self._write_debug(f"[WARNING] {msg}")
    
    def error(self, msg: str):
        with self._get_lock():
            self.logger.error(msg)
            self._write_debug(f"[ERROR] {msg}")
    
    def debug(self, msg: str):
        with self._get_lock():
            if self.debug_mode:
                self.logger.debug(msg)
            self._write_debug(f"[DEBUG] {msg}")
    
    def success(self, msg: str):
        with self._get_lock():
            print(f"\033[92m[SUCCESS] {msg}\033[0m")
            self._write_debug(f"[SUCCESS] {msg}")
    
    def _write_debug(self, msg: str):
        if self.debug_file:
            try:
                self.debug_file.write(f"{msg}\n")
                self.debug_file.flush()
            except:
                pass
    
    def _get_lock(self):
        if not hasattr(self, '_lock'):
            self._lock = threading.Lock()
        return self._lock
    
    def close(self):
        if self.debug_file:
            try:
                self.debug_file.write(f"\n\n{'='*80}\n")
                self.debug_file.write(f"Debug Session Ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.debug_file.write(f"{'='*80}\n")
                self.debug_file.close()
            except:
                pass

logger = None

def init_logger(debug_mode: bool = False, debug_log_file: Optional[str] = None):
    global logger
    logger = Logger(debug_mode, debug_log_file)


# ==================== 配置管理 ====================
# (Config 类保持不变)
class Config:
    def __init__(self):
        self.config_dir = self._get_config_dir()
        self.config_file = os.path.join(self.config_dir, 'manga_metadata_config.json')
        self.tasks_dir = os.path.join(self.config_dir, 'tasks')
        self.debug_dir = os.path.join(self.config_dir, 'debug')
        os.makedirs(self.tasks_dir, exist_ok=True)
        os.makedirs(self.debug_dir, exist_ok=True)
        self.config = self._load_config()
    
    def _get_config_dir(self) -> str:
        if platform.system() == 'Windows':
            base = os.getenv('APPDATA', os.path.expanduser('~'))
        else:
            base = os.path.expanduser('~/.config')
        
        config_dir = os.path.join(base, 'manga_metadata')
        os.makedirs(config_dir, exist_ok=True)
        return config_dir
    
    def _load_config(self) -> Dict:
        default_config = self._get_default_config()
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    for key, value in default_config.items():
                        if key not in loaded_config:
                            loaded_config[key] = value
                    return loaded_config
            except Exception as e:
                if logger:
                    logger.warning(f'加载配置文件失败: {e}')
        
        return default_config
    
    def _get_default_config(self) -> Dict:
        return {
            'base_url': 'https://nhentai.net',
            'cookie': '',
            'useragent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'proxy': '',
            'retry_times': 3,
            'timeout': 30,
            'threads': 5,
        }
    
    def save_config(self):
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            logger.success(f'配置已保存到: {self.config_file}')
        except Exception as e:
            logger.error(f'保存配置文件失败: {e}')
    
    def setup(self):
        print("\n" + "="*50)
        print("开始配置 - 为了获取漫画元数据，需要以下配置")
        print("="*50 + "\n")
        
        print("1. Cookie (用于登录nhentai)")
        print("   获取方法: 访问 https://nhentai.net，登录后在浏览器开发者工具中查看Cookie")
        cookie_input = input("请输入Cookie (如果不需要可按Enter跳过): ").strip()
        if cookie_input:
            self.config['cookie'] = cookie_input
        
        print("\n2. User-Agent (浏览器标识)")
        print(f"   当前默认值: {self.config['useragent']}")
        ua_input = input("请输入User-Agent (按Enter使用默认值): ").strip()
        if ua_input:
            self.config['useragent'] = ua_input
        
        print("\n3. Proxy (代理服务器)")
        print("   格式: http://proxy.example.com:8080 或 socks5://proxy.example.com:1080")
        proxy_input = input("请输入Proxy地址 (如果不需要可按Enter跳过): ").strip()
        if proxy_input:
            self.config['proxy'] = proxy_input
        
        print("\n4. Base URL (nhentai网址)")
        print(f"   当前默认值: {self.config['base_url']}")
        url_input = input("请输入Base URL (按Enter使用默认值): ").strip()
        if url_input:
            self.config['base_url'] = url_input
        
        print("\n5. 线程数 (用于加速处理)")
        print(f"   当前默认值: {self.config.get('threads', 5)}")
        threads_input = input("请输入线程数 (按Enter使用默认值): ").strip()
        if threads_input:
            try:
                self.config['threads'] = int(threads_input)
            except ValueError:
                logger.warning('线程数必须是整数，使用默认值')
        
        self.save_config()
        print("\n✓ 配置完成！\n")
    
    def get(self, key: str, default=None):
        return self.config.get(key, default)


# ==================== ZIP原地修改器 ====================

class ZipInPlaceModifier:
    """直接修改ZIP文件中特定文件内容的工具"""
    
    @staticmethod
    def update_file_content(zip_path: str, target_file: str, new_content: bytes) -> bool:
        """
        直接更新ZIP文件中的特定文件内容
        """
        try:
            # 先检查文件是否存在
            with zipfile.ZipFile(zip_path, 'r') as zf:
                if target_file not in zf.namelist():
                    logger.debug(f'文件 {target_file} 不存在于 {zip_path} 中')
                    return False
            
            # 创建临时ZIP文件
            temp_zip = zip_path + '.temp'
            
            with zipfile.ZipFile(zip_path, 'r') as zip_read:
                with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                    # 复制所有文件，除了要修改的目标文件
                    for item in zip_read.namelist():
                        if item == target_file:
                            # 写入新的内容
                            zip_write.writestr(item, new_content)
                            logger.debug(f'  更新文件: {item}')
                        else:
                            # 复制原有内容
                            zip_write.writestr(item, zip_read.read(item))
            
            # 原子性替换
            os.replace(temp_zip, zip_path)
            return True
            
        except Exception as e:
            logger.error(f'原地修改ZIP文件失败 {zip_path}: {e}')
            # 清理临时文件
            if os.path.exists(temp_zip):
                try:
                    os.remove(temp_zip)
                except:
                    pass
            return False
    
    @staticmethod
    def append_file(zip_path: str, new_file: str, content: bytes) -> bool:
        """
        向ZIP文件中添加新文件
        """
        try:
            with zipfile.ZipFile(zip_path, 'a') as zf:
                zf.writestr(new_file, content)
            return True
        except Exception as e:
            logger.error(f'向ZIP文件添加文件失败 {zip_path}: {e}')
            return False

# ==================== 内存缓存管理器 ====================

class MemoryCacheManager:
    """内存缓存管理器，减少重复磁盘读取"""
    
    def __init__(self, max_cache_size: int = 100):
        self.cache = {}
        self.access_count = {}
        self.max_size = max_cache_size
        self._lock = threading.Lock()
    
    def get_cached_content(self, file_path: str, inner_path: str) -> Optional[bytes]:
        """从缓存获取文件内容"""
        key = f"{file_path}:{inner_path}"
        with self._lock:
            if key in self.cache:
                self.access_count[key] = self.access_count.get(key, 0) + 1
                return self.cache[key]
            return None
    
    def cache_content(self, file_path: str, inner_path: str, content: bytes):
        """缓存文件内容"""
        key = f"{file_path}:{inner_path}"
        with self._lock:
            # 如果缓存已满，移除最少使用的项目
            if len(self.cache) >= self.max_size:
                self._evict_least_used()
            
            self.cache[key] = content
            self.access_count[key] = 1
    
    def _evict_least_used(self):
        """移除最少使用的缓存项"""
        if not self.access_count:
            return
            
        # 找到访问次数最少的项
        min_access_key = min(self.access_count.keys(), key=lambda k: self.access_count[k])
        if min_access_key in self.cache:
            del self.cache[min_access_key]
        if min_access_key in self.access_count:
            del self.access_count[min_access_key]

# ==================== 磁盘I/O控制器 ====================

class DiskIOController:
    """磁盘I/O流量控制器"""
    
    def __init__(self, max_speed_mb_per_sec: float = 0):
        self.max_speed = max_speed_mb_per_sec * 1024 * 1024  # 转换为字节/秒
        self.last_check_time = time.time()
        self.bytes_processed = 0
        self._lock = threading.Lock()
    
    def throttle_io(self, bytes_count: int):
        """根据设定的速度限制I/O操作"""
        if self.max_speed <= 0:
            return  # 无限制模式
            
        with self._lock:
            self.bytes_processed += bytes_count
            current_time = time.time()
            elapsed_time = current_time - self.last_check_time
            
            if elapsed_time >= 1.0:  # 每秒检查一次
                actual_speed = self.bytes_processed / elapsed_time
                
                if actual_speed > self.max_speed:
                    # 计算需要等待的时间
                    excess_bytes = self.bytes_processed - (self.max_speed * elapsed_time)
                    wait_time = excess_bytes / self.max_speed if self.max_speed > 0 else 0
                    if wait_time > 0:
                        time.sleep(min(wait_time, 0.1))  # 最多等待100ms
                
                # 重置计数器
                self.bytes_processed = 0
                self.last_check_time = current_time

# ==================== 翻译器 ====================

class Translator:
    """标签翻译器，用于将英文标签翻译为中文"""
    
    def __init__(self, database_path: str = 'database.json'):
        self.database_path = database_path
        self.translation_dict = {}
        self.reverse_dict = {}  # 新增：反向词典（译文->原文）
        self.untranslated_tags = set()
        self._lock = threading.Lock()
        self.load_database()
    
    def load_database(self):
        """加载翻译词典"""
        try:
            if os.path.exists(self.database_path):
                with open(self.database_path, 'r', encoding='utf-8') as f:
                    self.translation_dict = json.load(f)
                # 构建反向词典
                self.reverse_dict = {v: k for k, v in self.translation_dict.items()}
                if logger:  # 检查logger是否存在
                    logger.info(f'已加载翻译词典，共 {len(self.translation_dict)} 个词条')
                else:
                    print(f'已加载翻译词典，共 {len(self.translation_dict)} 个词条')
            else:
                if logger:
                    logger.warning(f'翻译词典文件不存在: {self.database_path}')
                else:
                    print(f'翻译词典文件不存在: {self.database_path}')
        except Exception as e:
            if logger:
                logger.error(f'加载翻译词典失败: {e}')
            else:
                print(f'加载翻译词典失败: {e}')
    
    def is_already_translated(self, tag: str) -> bool:
        """检查标签是否已经是译文"""
        clean_tag = tag.strip()
        return clean_tag in self.reverse_dict
    
    def get_original_from_translated(self, translated_tag: str) -> str:
        """从译文获取原文"""
        clean_tag = translated_tag.strip()
        return self.reverse_dict.get(clean_tag, translated_tag)
    
    def translate_tag(self, tag: str) -> Tuple[str, bool]:
        """翻译单个标签，返回(翻译结果, 是否找到翻译)"""
        if not tag:
            return tag, True
            
        # 清理标签（去除首尾空格）
        clean_tag = tag.strip()
        
        # 检查是否已经是译文
        if self.is_already_translated(clean_tag):
            return clean_tag, True  # 已经是译文，直接返回
        
        # 查找翻译
        if clean_tag in self.translation_dict:
            return self.translation_dict[clean_tag], True
        else:
            # 记录未翻译标签
            with self._lock:
                self.untranslated_tags.add(clean_tag)
            if logger:
                logger.debug(f'未找到翻译: {clean_tag}')
            return clean_tag, False  # 返回原文和False表示未找到翻译
    
    def translate_tags_list(self, tags: List[str]) -> Tuple[List[str], bool]:
        """翻译标签列表，返回(翻译结果列表, 是否全部找到翻译)"""
        if not tags:
            return tags, True
        
        translated = []
        all_found = True
        
        for tag in tags:
            translated_tag, found = self.translate_tag(tag)
            translated.append(translated_tag)
            if not found:
                all_found = False
                
        return translated, all_found
    
    def save_untranslated_tags(self, output_path: str = 'untranslated_tags.json'):
        """保存未翻译标签到文件"""
        try:
            with self._lock:
                if not self.untranslated_tags:
                    return
                    
                untranslated_data = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'tags': sorted(list(self.untranslated_tags))
                }
                
                # 如果文件已存在，合并标签
                if os.path.exists(output_path):
                    try:
                        with open(output_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            if 'tags' in existing_data:
                                untranslated_data['tags'] = sorted(list(
                                    set(untranslated_data['tags'] + existing_data['tags'])
                                ))
                    except:
                        pass  # 如果读取失败，使用新的数据
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(untranslated_data, f, ensure_ascii=False, indent=2)
                
                if logger:
                    logger.info(f'已保存 {len(untranslated_data["tags"])} 个未翻译标签到: {output_path}')
                else:
                    print(f'已保存 {len(untranslated_data["tags"])} 个未翻译标签到: {output_path}')
                
        except Exception as e:
            if logger:
                logger.error(f'保存未翻译标签失败: {e}')
            else:
                print(f'保存未翻译标签失败: {e}')

# ==================== ComicInfo生成器 ====================
# (ComicInfoGenerator 类保持不变)
class ComicInfoGenerator:
    @staticmethod
    def generate_comic_info(metadata: Dict) -> str:
        try:
            date_str = metadata.get('date', '')
            year = month = day = ''
            if date_str:
                try:
                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    year = str(date_obj.year)
                    month = str(date_obj.month)
                    day = str(date_obj.day)
                except:
                    pass
            
            # 处理列表和字符串
            def format_field(value):
                if isinstance(value, list):
                    return ', '.join(str(v) for v in value)
                return str(value) if value else ''
            
            tags = format_field(metadata.get('tags', ''))
            characters = format_field(metadata.get('characters', ''))
            series = format_field(metadata.get('parodies', ''))
            artists = format_field(metadata.get('artists', ''))
            groups = format_field(metadata.get('groups', ''))
            categories = format_field(metadata.get('categories', ''))
            languages = format_field(metadata.get('languages', ''))
            
            is_translated = 'translated' in (languages.lower() if languages else '')
            is_black_white = 'greyscale' in (tags.lower() if tags else '')
            
            language_iso = 'en'
            if 'chinese' in (languages.lower() if languages else ''):
                language_iso = 'zh'
            elif 'japanese' in (languages.lower() if languages else ''):
                language_iso = 'ja'
            
            xml_content = f"""<?xml version="1.0" encoding="utf-8"?>
<ComicInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <Manga>Yes</Manga>
 <Title>{ComicInfoGenerator._escape_xml(metadata.get('title', ''))}</Title>
 <Summary>{ComicInfoGenerator._escape_xml(metadata.get('subtitle', ''))}</Summary>
 <PageCount>{metadata.get('pages', 0)}</PageCount>
 <URL>https://nhentai.net/g/{metadata.get('id', '')}</URL>
 <NhentaiId>{metadata.get('id', '')}</NhentaiId>
 <Favorites>{metadata.get('favorite_counts', 0)}</Favorites>
 <Genre>{ComicInfoGenerator._escape_xml(categories)}</Genre>
 <BlackAndWhite>{'Yes' if is_black_white else 'No'}</BlackAndWhite>
 <Year>{year}</Year>
 <Month>{month}</Month>
 <Day>{day}</Day>
 <Series>{ComicInfoGenerator._escape_xml(series)}</Series>
 <Characters>{ComicInfoGenerator._escape_xml(characters)}</Characters>
 <Tags>{ComicInfoGenerator._escape_xml(tags)}</Tags>
 <Writer>{ComicInfoGenerator._escape_xml(artists)}</Writer>
 <Translated>{'Yes' if is_translated else 'No'}</Translated>
 <LanguageISO>{language_iso}</LanguageISO>
</ComicInfo>"""
            
            return xml_content
        
        except Exception as e:
            logger.error(f'生成ComicInfo.xml失败: {e}')
            return None
    
    @staticmethod
    def _escape_xml(text: str) -> str:
        if not text:
            return ''
        text = str(text)
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        text = text.replace('"', '&quot;')
        text = text.replace("'", '&apos;')
        return text


# ==================== ZIP调试工具 ====================
# (ZipDebugger 类保持不变)
class ZipDebugger:
    @staticmethod
    def dump_zip_structure(zip_path: str, output_file: str):
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("ZIP File Analysis Report\n")
                f.write(f"{'='*80}\n")
                f.write(f"File: {zip_path}\n")
                f.write(f"File Size: {os.path.getsize(zip_path)} bytes\n")
                f.write(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'='*80}\n\n")
                
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    f.write(f"Total Files: {len(zf.namelist())}\n")
                    f.write(f"Compression Type: {zf.compression}\n\n")
                    
                    f.write("Directory Structure:\n")
                    f.write(f"{'-'*80}\n")
                    
                    root_items = set()
                    nested_info = {}
                    
                    for item in sorted(zf.namelist()):
                        item_info = zf.getinfo(item)
                        item_clean = item.rstrip('/')
                        
                        if '/' in item_clean:
                            root_folder = item_clean.split('/')[0]
                            if root_folder not in nested_info:
                                nested_info[root_folder] = []
                            nested_info[root_folder].append(item)
                            root_items.add(root_folder)
                        else:
                            root_items.add(item_clean)
                        
                        size = item_info.file_size
                        date = item_info.date_time
                        f.write(f"  {item:<60} | Size: {size:>10} | Date: {date}\n")
                    
                    f.write(f"\n\nStructure Analysis:\n")
                    f.write(f"{'-'*80}\n")
                    f.write(f"Root Level Items: {len(root_items)}\n")
                    for item in sorted(root_items):
                        f.write(f"  - {item}\n")
                    
                    f.write(f"\nNested Folders:\n")
                    for folder, items in sorted(nested_info.items()):
                        f.write(f"  {folder}/\n")
                        f.write(f"    Items: {len(items)}\n")
                        for item in sorted(items)[:10]:
                            f.write(f"      - {item}\n")
                        if len(items) > 10:
                            f.write(f"      ... and {len(items) - 10} more\n")
                    
                    f.write(f"\nMetadata Check:\n")
                    f.write(f"{'-'*80}\n")
                    if 'metadata.json' in zf.namelist():
                        f.write(f"✓ metadata.json found\n")
                        try:
                            metadata = json.loads(zf.read('metadata.json').decode('utf-8'))
                            f.write(f"Content: {json.dumps(metadata, ensure_ascii=False, indent=2)}\n")
                        except Exception as e:
                            f.write(f"Error reading metadata: {e}\n")
                    else:
                        f.write(f"✗ metadata.json not found\n")
                    
                    f.write(f"\nFile Extensions:\n")
                    f.write(f"{'-'*80}\n")
                    extensions = {}
                    for item in zf.namelist():
                        if item.endswith('/'):
                            continue
                        ext = os.path.splitext(item)[1].lower() or '[no extension]'
                        extensions[ext] = extensions.get(ext, 0) + 1
                    
                    for ext in sorted(extensions.keys()):
                        f.write(f"  {ext:<15}: {extensions[ext]}\n")
            
            logger.success(f'ZIP文件分析已保存到: {output_file}')
        
        except Exception as e:
            logger.error(f'导出ZIP结构失败: {e}')


# ==================== ZIP工具 ====================
# (ZipStructureConverter 类保持不变)
class ZipStructureConverter:
    @staticmethod
    def analyze_zip_structure(zip_path: str) -> Tuple[str, Optional[str]]:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                file_list = zf.namelist()
                
                if not file_list:
                    return 'unknown', None
                
                logger.debug(f'ZIP文件总条目数: {len(file_list)}')
                
                root_level_items = set()
                
                for item in file_list:
                    item_clean = item.rstrip('/')
                    
                    if not item_clean:
                        continue
                    
                    if '/' in item_clean:
                        root_item = item_clean.split('/')[0]
                        root_level_items.add(root_item)
                    else:
                        root_level_items.add(item_clean)
                
                logger.debug(f'根目录第一级项目: {root_level_items}')
                logger.debug(f'根目录第一级项目数: {len(root_level_items)}')
                
                if len(root_level_items) == 1:
                    single_item = list(root_level_items)[0]
                    
                    is_folder = any('/' in item.rstrip('/') and item.rstrip('/').split('/')[0] == single_item 
                                   for item in file_list if '/' in item)
                    
                    if is_folder:
                        logger.debug(f'检测到嵌套结构: 根目录只有一个文件夹 "{single_item}"')
                        return 'nested', single_item
                    else:
                        logger.debug(f'检测到扁平结构: 根目录只有一个文件 "{single_item}"')
                        return 'flat', None
                else:
                    logger.debug(f'检测到扁平结构: 根目录有 {len(root_level_items)} 个第一级项目')
                    return 'flat', None
        
        except Exception as e:
            logger.error(f'分析ZIP结构失败: {e}')
            import traceback
            logger.debug(f'错误堆栈: {traceback.format_exc()}')
            return 'unknown', None
    
    @staticmethod
    def convert_nested_to_flat(zip_path: str) -> bool:
        try:
            structure_type, folder_name = ZipStructureConverter.analyze_zip_structure(zip_path)
            
            if structure_type != 'nested' or not folder_name:
                logger.debug(f'{os.path.basename(zip_path)} 已是扁平结构或无法识别')
                return True
            
            logger.info(f'正在转换ZIP结构: {os.path.basename(zip_path)}')
            logger.debug(f'文件夹名称: {folder_name}')
            
            temp_zip = zip_path + '.tmp'
            folder_prefix = folder_name + '/'
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_read:
                    with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                        for item in zip_read.namelist():
                            item_clean = item.rstrip('/')
                            
                            if item_clean == folder_name:
                                logger.debug(f'  跳过文件夹: {item}')
                                continue
                            
                            if item.startswith(folder_prefix):
                                new_name = item[len(folder_prefix):]
                                
                                if new_name and not item.endswith('/'):
                                    content = zip_read.read(item)
                                    zip_write.writestr(new_name, content)
                                    logger.debug(f'  移动: {item} -> {new_name}')
                
                backup_zip = zip_path + '.backup'
                os.rename(zip_path, backup_zip)
                os.rename(temp_zip, zip_path)
                
                try:
                    os.remove(backup_zip)
                except:
                    pass
                
                logger.success(f'ZIP结构已转换为扁平结构')
                return True
            
            except Exception as e:
                logger.error(f'ZIP转换过程中出错: {e}')
                if os.path.exists(temp_zip):
                    try:
                        os.remove(temp_zip)
                    except:
                        pass
                return False
        
        except Exception as e:
            logger.error(f'转换ZIP结构失败: {e}')
            return False


# ==================== 任务管理 ====================

class TaskManager:
    STATUS_PENDING = 'pending'
    STATUS_SUCCESS = 'success'
    STATUS_FAILED = 'failed'
    STATUS_SKIPPED = 'skipped'
    
    def __init__(self, config: Config):
        self.config = config
        self.tasks_dir = config.tasks_dir
        self.tasks = self._load_tasks()
    
    def _load_tasks(self) -> Dict:
        tasks = {}
        if os.path.exists(self.tasks_dir):
            for task_file in os.listdir(self.tasks_dir):
                if task_file.endswith('.json'):
                    try:
                        task_path = os.path.join(self.tasks_dir, task_file)
                        with open(task_path, 'r', encoding='utf-8') as f:
                            task_data = json.load(f)
                            task_id = task_file.replace('.json', '')
                            tasks[task_id] = task_data
                    except Exception as e:
                        logger.error(f'加载任务失败 {task_file}: {e}')
        return tasks
    
    def create_task(self, folder_path: str) -> str:
        folder_path = os.path.abspath(folder_path)
        
        for task_id, task_data in self.tasks.items():
            if task_data['folder_path'] == folder_path:
                logger.warning(f'该文件夹已存在任务: {task_id}')
                return task_id
        
        task_id = hashlib.md5(folder_path.encode()).hexdigest()[:8]
        counter = 1
        original_task_id = task_id
        while task_id in self.tasks:
            task_id = f"{original_task_id}_{counter}"
            counter += 1
        
        task_data = {
            'task_id': task_id,
            'folder_path': folder_path,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'files': {},
            'statistics': {
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
                'pending': 0
            }
        }
        
        folder_path_obj = Path(folder_path)
        if folder_path_obj.exists() and folder_path_obj.is_dir():
            for archive_file in sorted(list(folder_path_obj.glob('*.zip')) + list(folder_path_obj.glob('*.cbz'))):
                task_data['files'][archive_file.name] = {
                    'status': self.STATUS_PENDING,
                    'error': '',
                    'updated_at': ''
                }
            task_data['statistics']['total'] = len(task_data['files'])
            task_data['statistics']['pending'] = len(task_data['files'])
        
        self.tasks[task_id] = task_data
        self._save_task(task_id, task_data)
        
        logger.success(f'创建新任务: {task_id}')
        return task_id
    
    def delete_task(self, task_id: str) -> bool:
        if task_id not in self.tasks:
            logger.error(f'任务不存在: {task_id}')
            return False
        
        try:
            task_file = os.path.join(self.tasks_dir, f'{task_id}.json')
            if os.path.exists(task_file):
                os.remove(task_file)
            
            del self.tasks[task_id]
            logger.success(f'任务已删除: {task_id}')
            return True
        except Exception as e:
            logger.error(f'删除任务失败: {e}')
            return False
    
    def _save_task(self, task_id: str, task_data: Dict):
        try:
            task_file = os.path.join(self.tasks_dir, f'{task_id}.json')
            with open(task_file, 'w', encoding='utf-8') as f:
                json.dump(task_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f'保存任务失败: {e}')
    
    def update_file_status(self, task_id: str, filename: str, status: str, error_msg: str = ''):
        """更新文件处理状态"""
        if task_id not in self.tasks:
            return
        
        task_data = self.tasks[task_id]
        
        if filename not in task_data['files']:
            task_data['files'][filename] = {
                'status': self.STATUS_PENDING,
                'translation_status': 'pending',  # 新增翻译状态
                'error': '',
                'translation_error': '',  # 新增翻译错误信息
                'updated_at': ''
            }
        
        old_status = task_data['files'][filename]['status']
        task_data['files'][filename]['status'] = status
        task_data['files'][filename]['error'] = error_msg
        task_data['files'][filename]['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 更新统计信息
        self._update_statistics(task_data, old_status, status)
        
        task_data['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.tasks[task_id] = task_data
        self._save_task(task_id, task_data)
    
    def update_file_translation_status(self, task_id: str, filename: str, translation_status: str, error_msg: str = ''):
        """更新文件翻译状态"""
        if task_id not in self.tasks:
            return
        
        task_data = self.tasks[task_id]
        
        if filename not in task_data['files']:
            task_data['files'][filename] = {
                'status': self.STATUS_PENDING,
                'translation_status': 'pending',
                'error': '',
                'translation_error': '',
                'updated_at': ''
            }
        
        old_translation_status = task_data['files'][filename].get('translation_status', 'pending')
        task_data['files'][filename]['translation_status'] = translation_status
        task_data['files'][filename]['translation_error'] = error_msg
        task_data['files'][filename]['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 更新翻译统计信息
        self._update_translation_statistics(task_data, old_translation_status, translation_status)
        
        task_data['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.tasks[task_id] = task_data
        self._save_task(task_id, task_data)
        self._save_task(task_id, task_data)
    
    def _update_statistics(self, task_data: Dict, old_status: str, new_status: str):
        """更新处理状态统计"""
        stats = task_data['statistics']
        if old_status == self.STATUS_PENDING:
            stats['pending'] -= 1
        elif old_status == self.STATUS_SUCCESS:
            stats['success'] -= 1
        elif old_status == self.STATUS_FAILED:
            stats['failed'] -= 1
        elif old_status == self.STATUS_SKIPPED:
            stats['skipped'] -= 1
        
        if new_status == self.STATUS_SUCCESS:
            stats['success'] += 1
        elif new_status == self.STATUS_FAILED:
            stats['failed'] += 1
        elif new_status == self.STATUS_SKIPPED:
            stats['skipped'] += 1
        elif new_status == self.STATUS_PENDING:
            stats['pending'] += 1
    
    def _update_translation_statistics(self, task_data: Dict, old_status: str, new_status: str):
        """更新翻译状态统计"""
        stats = task_data['statistics']
        # 初始化翻译统计字段
        if 'translation_success' not in stats:
            stats['translation_success'] = 0
            stats['translation_failed'] = 0
            stats['translation_pending'] = len(task_data.get('files', {}))
        
        if old_status == 'pending':
            stats['translation_pending'] -= 1
        elif old_status == 'success':
            stats['translation_success'] -= 1
        elif old_status == 'failed':
            stats['translation_failed'] -= 1
        
        if new_status == 'success':
            stats['translation_success'] += 1
        elif new_status == 'failed':
            stats['translation_failed'] += 1
        elif new_status == 'pending':
            stats['translation_pending'] += 1
    
    def get_task(self, task_id: str) -> Optional[Dict]:
        return self.tasks.get(task_id)
    
    def list_tasks(self) -> Dict:
        return self.tasks
    
    def get_pending_files(self, task_id: str) -> List[str]:
        task_data = self.tasks.get(task_id)
        if not task_data:
            return []
        
        pending = []
        for filename, file_info in task_data['files'].items():
            if file_info['status'] == self.STATUS_PENDING:
                pending.append(filename)
        return pending

    # 新增: 获取失败的文件列表
    def get_failed_files(self, task_id: str) -> List[str]:
        task_data = self.tasks.get(task_id)
        if not task_data:
            return []
        
        failed = []
        for filename, file_info in task_data['files'].items():
            if file_info['status'] == self.STATUS_FAILED:
                failed.append(filename)
        return failed
    
    # 新增: 获取翻译失败的文件列表
    def get_translation_failed_files(self, task_id: str) -> List[str]:
        task_data = self.tasks.get(task_id)
        if not task_data:
            return []
        
        failed = []
        for filename, file_info in task_data['files'].items():
            translation_status = file_info.get('translation_status', 'pending')
            if translation_status == 'failed':
                failed.append(filename)
        return failed
    
    # 新增: 获取未翻译的文件列表
    def get_untranslated_files(self, task_id: str) -> List[str]:
        task_data = self.tasks.get(task_id)
        if not task_data:
            return []
        
        untranslated = []
        for filename, file_info in task_data['files'].items():
            translation_status = file_info.get('translation_status', 'pending')
            if translation_status == 'pending':
                untranslated.append(filename)
        return untranslated
    
    def export_translation_stats(self, task_id: str, output_file: str):
        """导出翻译统计信息"""
        task_data = self.get_task(task_id)
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        stats = task_data['statistics']
        translation_success = stats.get('translation_success', 0)
        translation_failed = stats.get('translation_failed', 0)
        translation_pending = stats.get('translation_pending', 0)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("翻译统计报告\n")
                f.write("="*50 + "\n")
                f.write(f"任务ID: {task_id}\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*50 + "\n\n")
                
                f.write("翻译统计:\n")
                f.write(f"  翻译成功: {translation_success}\n")
                f.write(f"  翻译失败: {translation_failed}\n")
                f.write(f"  待翻译:   {translation_pending}\n")
                f.write(f"  总计:     {translation_success + translation_failed + translation_pending}\n\n")
                
                # 计算翻译成功率
                total_translations = translation_success + translation_failed
                if total_translations > 0:
                    success_rate = (translation_success / total_translations) * 100
                    f.write(f"翻译成功率: {success_rate:.1f}%\n\n")
                
                # 导出各文件的翻译状态
                f.write("文件翻译详情:\n")
                f.write("-"*50 + "\n")
                for filename, file_info in task_data['files'].items():
                    translation_status = file_info.get('translation_status', 'unknown')
                    translation_error = file_info.get('translation_error', '')
                    f.write(f"{filename}:\n")
                    f.write(f"  翻译状态: {translation_status}\n")
                    if translation_error:
                        f.write(f"  错误信息: {translation_error}\n")
                    f.write("\n")
            
            logger.success(f'翻译统计已导出到: {output_file}')
        except Exception as e:
            logger.error(f'导出翻译统计失败: {e}')
    
    def export_status_log(self, task_id: str, status: str, output_file: str):
        task_data = self.tasks.get(task_id)
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        files = []
        for filename, file_info in task_data['files'].items():
            if file_info['status'] == status:
                files.append((filename, file_info.get('error', '')))
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for filename, error in files:
                    if error:
                        f.write(f'{filename} - {error}\n')
                    else:
                        f.write(f'{filename}\n')
            logger.success(f'已导出 {status} 状态的文件列表到: {output_file}')
        except Exception as e:
            logger.error(f'导出日志失败: {e}')


# ==================== 网络请求 ====================
# (NetClient 类保持不变)
class NetClient:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.get('base_url', 'https://nhentai.net')
    
    def _get_headers(self) -> Dict:
        return {
            'Referer': self.base_url,
            'User-Agent': self.config.get('useragent'),
            'Cookie': self.config.get('cookie', ''),
        }
    
    def _get_proxies(self) -> Optional[Dict]:
        proxy = self.config.get('proxy', '')
        if proxy:
            return {'https': proxy, 'http': proxy}
        return None
    
    def request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        headers = self._get_headers()
        proxies = self._get_proxies()
        timeout = self.config.get('timeout', 30)
        
        try:
            session = requests.Session(impersonate='chrome110')
            session.headers.update(headers)
            
            response = session.request(
                method,
                url,
                proxies=proxies,
                timeout=timeout,
                verify=False,
                **kwargs
            )
            
            return response
        
        except Exception as e:
            logger.error(f'网络请求失败: {e}')
            return None
    
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        return self.request('GET', url, **kwargs)


# ==================== 元数据解析 ====================
# (MetadataParser 类保持不变)
class MetadataParser:
    def __init__(self, client: NetClient):
        self.client = client
    
    def parse(self, manga_id: str) -> Optional[Dict]:
        url = f'{self.client.base_url}/g/{manga_id}/'
        
        try:
            response = self.client.get(url)
            
            if not response:
                return None
            
            if response.status_code == 404:
                logger.error(f'漫画 {manga_id} 不存在')
                return None
            
            if response.status_code != 200:
                logger.error(f'请求失败，状态码: {response.status_code}')
                return None
            
            if response.status_code == 403 and 'Just a moment' in response.text:
                logger.error('被Cloudflare阻止，请检查Cookie和User-Agent')
                return None
            
            metadata = self._parse_html(response.content, manga_id)
            return metadata
        
        except Exception as e:
            logger.error(f'获取元数据时出错: {e}')
            return None
    
    def _parse_html(self, html_content: bytes, manga_id: str) -> Optional[Dict]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            metadata = {'id': manga_id}
            
            info_div = soup.find('div', attrs={'id': 'info'})
            if not info_div:
                return None
            
            h1 = info_div.find('h1')
            if h1:
                metadata['title'] = h1.get_text(strip=True)
                pretty_name_span = h1.find('span', attrs={'class': 'pretty'})
                if pretty_name_span:
                    metadata['pretty_name'] = pretty_name_span.get_text(strip=True)
            
            h2 = info_div.find('h2')
            if h2:
                metadata['subtitle'] = h2.get_text(strip=True)
            
            nobold_span = info_div.find('span', class_='nobold')
            if nobold_span:
                favorite_text = nobold_span.get_text(strip=True)
                favorite_counts = favorite_text.strip('()').replace(',', '')
                try:
                    metadata['favorite_counts'] = int(favorite_counts)
                except ValueError:
                    metadata['favorite_counts'] = 0
            
            pages = 0
            for div in info_div.find_all('div', class_='tag-container field-name'):
                if 'Pages:' in div.get_text():
                    span = div.find('span', class_='name')
                    if span:
                        try:
                            pages = int(span.get_text(strip=True))
                        except ValueError:
                            pass
            metadata['pages'] = pages
            
            information_fields = info_div.find_all('div', attrs={'class': 'field-name'})
            needed_fields = {
                'Characters': 'characters',
                'Artists': 'artists',
                'Languages': 'languages',
                'Tags': 'tags',
                'Parodies': 'parodies',
                'Groups': 'groups',
                'Categories': 'categories'
            }
            
            for field_div in information_fields:
                if not field_div.contents:
                    continue
                field_name = field_div.contents[0].strip().strip(':')
                
                if field_name in needed_fields:
                    tags = field_div.find_all('a', attrs={'class': 'tag'})
                    data = [tag.find('span', attrs={'class': 'name'}).get_text(strip=True) for tag in tags]
                    metadata[needed_fields[field_name]] = data
            
            time_tag = info_div.find('time')
            if time_tag and time_tag.has_attr('datetime'):
                metadata['date'] = time_tag['datetime']
            
            return metadata
        
        except Exception as e:
            logger.error(f'解析HTML时出错: {e}')
            import traceback
            logger.debug(f'错误堆栈: {traceback.format_exc()}')
            return None


# ==================== ZIP处理 ====================

class ZipMetadataAdder:
    FILENAME_PATTERN = re.compile(r'^\[(\d{6})\](.+)\.(zip|cbz)$', re.IGNORECASE)
    
    def __init__(self, parser: MetadataParser, task_manager: TaskManager, 
                 dry_run: bool = False, to_cbz: bool = False, disk_limit: float = 0):
        self.parser = parser
        self.task_manager = task_manager
        self.dry_run = dry_run
        self.to_cbz = to_cbz
        self._lock = threading.Lock()
        self._shutdown_requested = threading.Event()
        self.translator = Translator()
        self.io_controller = DiskIOController(disk_limit)
        self.cache_manager = MemoryCacheManager(max_cache_size=50)  # 新增缓存管理器
    
    def extract_id(self, filename: str) -> Optional[str]:
        match = self.FILENAME_PATTERN.match(filename)
        if match:
            return match.group(1)
        return None
    
    def translate_metadata_in_cbz(self, cbz_path: str, task_id: str, filename: str) -> bool:
        """翻译CBZ文件中的元数据标签"""
        try:
            logger.debug(f'开始翻译: {filename}')
            
            # 尝试从缓存获取内容
            metadata_json = None
            comic_info_xml = None
            
            # 读取metadata.json
            metadata_content = self.cache_manager.get_cached_content(cbz_path, 'metadata.json')
            if metadata_content is None:
                with zipfile.ZipFile(cbz_path, 'r') as zf:
                    if 'metadata.json' in zf.namelist():
                        try:
                            metadata_content = zf.read('metadata.json')
                            self.cache_manager.cache_content(cbz_path, 'metadata.json', metadata_content)
                        except Exception as e:
                            logger.warning(f'  读取metadata.json失败: {e}')
            
            if metadata_content:
                try:
                    metadata_json = json.loads(metadata_content.decode('utf-8'))
                    logger.debug(f'  已读取metadata.json')
                except Exception as e:
                    logger.warning(f'  解析metadata.json失败: {e}')
            
            # 读取ComicInfo.xml
            xml_content = self.cache_manager.get_cached_content(cbz_path, 'ComicInfo.xml')
            if xml_content is None:
                with zipfile.ZipFile(cbz_path, 'r') as zf:
                    if 'ComicInfo.xml' in zf.namelist():
                        try:
                            xml_content = zf.read('ComicInfo.xml')
                            self.cache_manager.cache_content(cbz_path, 'ComicInfo.xml', xml_content)
                        except Exception as e:
                            logger.warning(f'  读取ComicInfo.xml失败: {e}')
            
            if xml_content:
                try:
                    # 优先使用xml解析器，失败则使用html.parser
                    try:
                        comic_info_xml = BeautifulSoup(xml_content.decode('utf-8'), 'xml')
                        logger.debug(f'  已读取ComicInfo.xml (xml解析器)')
                    except:
                        comic_info_xml = BeautifulSoup(xml_content.decode('utf-8'), 'html.parser')
                        logger.debug(f'  已读取ComicInfo.xml (html.parser)')
                except Exception as e:
                    logger.warning(f'  解析ComicInfo.xml失败: {e}')
            
            # 翻译标签
            if metadata_json or comic_info_xml:
                translated_metadata, translated_xml, all_translated = self._translate_content(
                    metadata_json, comic_info_xml
                )
                
                # 只有当所有标签都找到翻译时才更新文件
                if all_translated and not self.dry_run:
                    self._update_cbz_content(cbz_path, translated_metadata, translated_xml)
                    logger.debug(f'  已更新CBZ文件')
                    
                    with self._lock:
                        self.task_manager.update_file_translation_status(
                            task_id, filename, 'success', ''
                        )
                    logger.success(f'翻译完成: {filename}')
                    return True
                elif not all_translated:
                    logger.info(f'文件 {filename} 中有未翻译的标签，跳过翻译')
                    with self._lock:
                        self.task_manager.update_file_translation_status(
                            task_id, filename, 'failed', '存在未翻译的标签'
                        )
                    return False
                else:
                    # dry_run模式下，即使所有标签都翻译了也不实际更新文件
                    logger.info(f'[DRY RUN] 文件 {filename} 可以被完全翻译')
                    with self._lock:
                        self.task_manager.update_file_translation_status(
                            task_id, filename, 'success', ''
                        )
                    return True
            else:
                logger.warning(f'✗ {filename} - 未找到元数据文件')
                with self._lock:
                    self.task_manager.update_file_translation_status(
                        task_id, filename, 'failed', '未找到元数据文件'
                    )
                return False
                
        except Exception as e:
            logger.error(f'翻译CBZ文件失败 {filename}: {e}')
            logger.debug(f'错误详情 - task_id: {task_id}, filename: {filename}')
            import traceback
            logger.debug(f'完整错误堆栈: {traceback.format_exc()}')
            try:
                with self._lock:
                    self.task_manager.update_file_translation_status(
                        task_id, filename, 'failed', str(e)
                    )
            except Exception as status_error:
                logger.error(f'更新翻译状态也失败了: {status_error}')
            return False
    
    def _translate_content(self, metadata_json: dict, comic_info_xml: BeautifulSoup) -> Tuple[dict, BeautifulSoup, bool]:
        """翻译元数据内容，返回(翻译后的metadata, 翻译后的xml, 是否所有标签都找到翻译)"""
        all_tags_translated = True
        
        # 翻译metadata.json中的tags
        if metadata_json and 'tags' in metadata_json:
            original_tags = metadata_json['tags']
            if isinstance(original_tags, list):
                translated_tags, all_found = self.translator.translate_tags_list(original_tags)
                if all_found:
                    metadata_json['tags'] = translated_tags
                    logger.debug(f'  翻译metadata标签: {len(original_tags)} 个')
                else:
                    all_tags_translated = False
                    logger.debug(f'  metadata中有未翻译标签，跳过翻译')
            elif isinstance(original_tags, str):
                # 如果是逗号分隔的字符串
                tags_list = [tag.strip() for tag in original_tags.split(',') if tag.strip()]
                translated_tags, all_found = self.translator.translate_tags_list(tags_list)
                if all_found:
                    metadata_json['tags'] = ', '.join(translated_tags)
                    logger.debug(f'  翻译metadata标签字符串: {len(tags_list)} 个')
                else:
                    all_tags_translated = False
                    logger.debug(f'  metadata中有未翻译标签，跳过翻译')
        
        # 翻译ComicInfo.xml中的Tags
        if comic_info_xml:
            tags_element = comic_info_xml.find('Tags')
            if tags_element:
                tags_text = tags_element.get_text()
                if tags_text:
                    tags_list = [tag.strip() for tag in tags_text.split(',') if tag.strip()]
                    translated_tags, all_found = self.translator.translate_tags_list(tags_list)
                    if all_found:
                        tags_element.string = ', '.join(translated_tags)
                        logger.debug(f'  翻译ComicInfo标签: {len(tags_list)} 个')
                    else:
                        all_tags_translated = False
                        logger.debug(f'  ComicInfo中有未翻译标签，跳过翻译')
        
        return metadata_json, comic_info_xml, all_tags_translated
    
    def _update_cbz_content(self, cbz_path: str, metadata_json: dict, comic_info_xml: BeautifulSoup):
        """更新CBZ文件内容"""
        temp_cbz = cbz_path + '.tmp'
        
        try:
            # 计算原始文件大小用于I/O控制
            original_size = os.path.getsize(cbz_path) if os.path.exists(cbz_path) else 0
            
            with zipfile.ZipFile(cbz_path, 'r') as zip_read:
                with zipfile.ZipFile(temp_cbz, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                    # 复制除元数据文件外的所有文件
                    for item in zip_read.namelist():
                        if item not in ('metadata.json', 'ComicInfo.xml'):
                            content = zip_read.read(item)
                            # 控制读取速度
                            self.io_controller.throttle_io(len(content))
                            zip_write.writestr(item, content)
                            # 控制写入速度
                            self.io_controller.throttle_io(len(content))
                    
                    # 写入翻译后的metadata.json
                    if metadata_json:
                        json_content = json.dumps(metadata_json, ensure_ascii=False, separators=(',', ':'))
                        zip_write.writestr('metadata.json', json_content.encode('utf-8'))
                        self.io_controller.throttle_io(len(json_content))
                    
                    # 写入翻译后的ComicInfo.xml
                    if comic_info_xml:
                        xml_content = str(comic_info_xml)
                        zip_write.writestr('ComicInfo.xml', xml_content.encode('utf-8'))
                        self.io_controller.throttle_io(len(xml_content))
            
            # 替换原文件
            if os.path.exists(cbz_path):
                os.remove(cbz_path)
            os.rename(temp_cbz, cbz_path)
            
            # 控制写入完成后的速度
            self.io_controller.throttle_io(original_size)
            
        except Exception as e:
            # 清理临时文件
            if os.path.exists(temp_cbz):
                try:
                    os.remove(temp_cbz)
                except:
                    pass
            logger.error(f'_update_cbz_content 错误: {e}')
            raise e
    
    def process_zip_file(self, zip_path: str, metadata: Dict) -> Tuple[bool, Optional[str]]:
        try:
            if self.dry_run:
                logger.debug(f'[DRY RUN] 处理: {os.path.basename(zip_path)}')
                return True, None
            
            logger.debug(f'步骤1: 检测ZIP结构')
            structure_type, folder_name = ZipStructureConverter.analyze_zip_structure(zip_path)
            logger.debug(f'  结构类型: {structure_type}')
            
            if structure_type == 'nested':
                logger.debug(f'步骤2: 转换ZIP结构 (文件夹: {folder_name})')
                if not ZipStructureConverter.convert_nested_to_flat(zip_path):
                    logger.error('ZIP结构转换失败')
                    return False, None
            else:
                logger.debug(f'步骤2: 跳过 (已是扁平结构)')
            
            logger.debug(f'步骤3-5: 处理元数据')
            temp_zip = zip_path + '.tmp'
            
            with zipfile.ZipFile(zip_path, 'r') as zip_read:
                has_metadata = 'metadata.json' in zip_read.namelist()
                has_comic_info = 'ComicInfo.xml' in zip_read.namelist()
                
                if has_metadata:
                    logger.debug(f'  发现旧的metadata.json，将被删除')
                
                if has_comic_info:
                    logger.debug(f'  发现旧的ComicInfo.xml，将被删除')
                
                with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                    for item in zip_read.namelist():
                        if item in ('metadata.json', 'ComicInfo.xml'):
                            logger.debug(f'  删除: {item}')
                            continue
                        
                        zip_write.writestr(item, zip_read.read(item))
                    
                    json_content = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
                    zip_write.writestr('metadata.json', json_content.encode('utf-8'))
                    logger.debug(f'  添加: metadata.json')
                    
                    if self.to_cbz or zip_path.lower().endswith('.cbz'):
                        comic_info_content = ComicInfoGenerator.generate_comic_info(metadata)
                        if comic_info_content:
                            zip_write.writestr('ComicInfo.xml', comic_info_content.encode('utf-8'))
                            logger.debug(f'  添加: ComicInfo.xml')
            
            os.remove(zip_path)
            os.rename(temp_zip, zip_path)
            
            new_path = None
            if self.to_cbz and zip_path.lower().endswith('.zip'):
                cbz_path = zip_path[:-4] + '.cbz'
                os.rename(zip_path, cbz_path)
                logger.debug(f'  重命名: .zip -> .cbz')
                new_path = cbz_path
                logger.success(f'已转换为CBZ格式')
            else:
                logger.debug(f'ZIP文件处理完成')
            
            return True, new_path
        
        except Exception as e:
            logger.error(f'处理ZIP文件失败: {e}')
            import traceback
            logger.debug(f'错误堆栈: {traceback.format_exc()}')
            temp_zip = zip_path + '.tmp'
            if os.path.exists(temp_zip):
                try:
                    os.remove(temp_zip)
                except:
                    pass
            return False, None
    
    def process_file(self, task_id: str, archive_path: str, filename: str) -> bool:
        manga_id = self.extract_id(filename)
        
        if not manga_id:
            with self._lock:
                logger.warning(f'✗ {filename} - 文件名格式不符合')
                self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_SKIPPED, '文件名格式不符合')
            return False
        
        with self._lock:
            logger.info(f'处理: {filename}')
        
        # 获取元数据
        metadata = self.parser.parse(manga_id)
        
        if not metadata:
            with self._lock:
                self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_FAILED, '获取元数据失败')
            return False
        
        # 处理ZIP文件（添加元数据）
        success, new_path = self.process_zip_file(archive_path, metadata)
        
        # 翻译标签
        translation_success = False
        if success:
            file_to_translate = new_path if new_path and new_path != archive_path else archive_path
            try:
                translation_success = self.translate_metadata_in_cbz(file_to_translate, task_id, filename)
            except Exception as e:
                logger.error(f'翻译过程出错: {e}')
                translation_success = False
        
        # 更新最终状态
        final_success = success and translation_success
        
        with self._lock:
            if final_success:
                if new_path and new_path != archive_path:
                    new_filename = os.path.basename(new_path)
                    # 更新状态
                    self.task_manager.update_file_status(
                        task_id,
                        filename,
                        TaskManager.STATUS_SUCCESS
                    )
                    # 更新文件名映射
                    task_data = self.task_manager.get_task(task_id)
                    if filename in task_data['files']:
                        file_info = task_data['files'].pop(filename)
                        task_data['files'][new_filename] = file_info
                        self.task_manager._save_task(task_id, task_data)
                    logger.success(f'{filename} -> {new_filename} (含翻译)')
                else:
                    logger.success(f'{filename} (含翻译)')
                    self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_SUCCESS)
            else:
                error_msg = '处理文件失败' if not success else '翻译失败'
                self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_FAILED, error_msg)
        
        return final_success
    
    def process_task_batch(self, task_id: str, num_threads: int = 5, batch_size: int = 10):
        """批量处理任务以减少磁盘I/O"""
        task_data = self.task_manager.get_task(task_id)
        
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        folder_path = Path(task_data['folder_path'])
        files_to_process = self.task_manager.get_pending_files(task_id)
        
        if not files_to_process:
            logger.info('没有待处理的文件')
            return
        
        logger.info(f'开始批量处理任务: {task_id}')
        logger.info(f'总文件数: {len(files_to_process)}, 批大小: {batch_size}')
        
        # 分批处理
        for i in range(0, len(files_to_process), batch_size):
            batch_files = files_to_process[i:i + batch_size]
            logger.info(f'处理批次 {i//batch_size + 1}: {len(batch_files)} 个文件')
            
            # 预加载批次文件到缓存
            self._preload_batch_to_cache(folder_path, batch_files)
            
            # 处理当前批次
            self._process_batch(task_id, folder_path, batch_files, num_threads)
            
            # 清理缓存为下一批做准备
            self.cache_manager.cache.clear()
            self.cache_manager.access_count.clear()
    
    def _preload_batch_to_cache(self, folder_path: Path, batch_files: List[str]):
        """预加载批次文件到内存缓存"""
        logger.debug(f'预加载 {len(batch_files)} 个文件到缓存')
        
        for filename in batch_files:
            file_path = folder_path / filename
            if not file_path.exists():
                continue
                
            try:
                with zipfile.ZipFile(file_path, 'r') as zf:
                    # 预加载元数据文件
                    for meta_file in ['metadata.json', 'ComicInfo.xml']:
                        if meta_file in zf.namelist():
                            content = zf.read(meta_file)
                            self.cache_manager.cache_content(str(file_path), meta_file, content)
                            logger.debug(f'  缓存 {filename}/{meta_file}')
            except Exception as e:
                logger.debug(f'预加载 {filename} 失败: {e}')
    
    def _process_batch(self, task_id: str, folder_path: Path, batch_files: List[str], num_threads: int):
        """处理单个批次"""
        def signal_handler(signum, frame):
            logger.warning('接收到退出信号')
            self._shutdown_requested.set()

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {}
                
                for filename in batch_files:
                    if self._shutdown_requested.is_set():
                        break
                        
                    archive_path = folder_path / filename
                    future = executor.submit(self.process_file, task_id, str(archive_path), filename)
                    futures[future] = filename
                
                # 等待批次完成
                for future in as_completed(futures, timeout=30.0):
                    try:
                        future.result(timeout=15.0)
                    except Exception as e:
                        logger.error(f'批次处理异常: {e}')
                        
        except TimeoutError:
            logger.warning('批次处理超时')
        except KeyboardInterrupt:
            logger.warning('批次处理被中断')
            self._shutdown_requested.set()
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)
        """仅处理翻译任务"""
        task_data = self.task_manager.get_task(task_id)
        
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        folder_path = Path(task_data['folder_path'])
        
        if not folder_path.exists():
            logger.error(f'文件夹不存在: {folder_path}')
            return
        
        # 获取需要翻译的文件
        files_to_translate = self.task_manager.get_untranslated_files(task_id)
        
        if not files_to_translate:
            logger.info('没有需要翻译的文件')
            self.print_translation_summary(task_id)
            return
        
        logger.info(f'开始翻译任务: {task_id}')
        logger.info(f'文件夹路径: {folder_path}')
        logger.info(f'待翻译文件数: {len(files_to_translate)}\n')
        
        # 注册信号处理函数
        def signal_handler(signum, frame):
            logger.warning('\n!!! 接收到退出信号 (Ctrl+C) !!!')
            logger.warning('正在停止分发新任务，请等待当前正在运行的线程完成...')
            self._shutdown_requested.set()

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {}
                
                for idx, filename in enumerate(files_to_translate):
                    if self._shutdown_requested.is_set():
                        logger.warning('已停止分发新任务。')
                        break

                    archive_path = folder_path / filename
                    future = executor.submit(self._translate_single_file, task_id, str(archive_path), filename)
                    futures[future] = filename
                    
                    # 延迟逻辑
                    if (idx + 1) % num_threads == 0:
                        start_sleep = time.time()
                        while time.time() - start_sleep < 2.0:  # 默认2秒延迟
                            if self._shutdown_requested.is_set():
                                break
                            time.sleep(0.1)
                
                # 等待任务完成，但响应退出信号
                if futures:
                    completed_futures = 0
                    total_futures = len(futures)
                    
                    for future in as_completed(futures, timeout=1.0):  # 1秒超时
                        try:
                            future.result(timeout=5.0)  # 每个任务最多等待5秒
                            completed_futures += 1
                            if completed_futures % 10 == 0:  # 每完成10个任务报告一次进度
                                logger.info(f'已完成 {completed_futures}/{total_futures} 个任务')
                        except Exception as e:
                            logger.error(f'处理异常: {e}')
                        
                        # 检查是否需要退出
                        if self._shutdown_requested.is_set():
                            logger.warning('检测到退出请求，正在清理...')
                            # 取消未完成的任务
                            for pending_future in futures:
                                if not pending_future.done():
                                    pending_future.cancel()
                            break
                
        except TimeoutError:
            logger.warning('任务等待超时')
        except KeyboardInterrupt:
            logger.warning('接收到键盘中断')
            self._shutdown_requested.set()
        
        self.print_translation_summary(task_id)
    
    def retry_failed_translations(self, task_id: str, num_threads: int = 5):
        """重试翻译失败的文件"""
        task_data = self.task_manager.get_task(task_id)
        
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        folder_path = Path(task_data['folder_path'])
        
        if not folder_path.exists():
            logger.error(f'文件夹不存在: {folder_path}')
            return
        
        # 获取翻译失败的文件
        failed_files = self.task_manager.get_translation_failed_files(task_id)
        
        if not failed_files:
            logger.info('没有翻译失败的文件需要重试')
            return
        
        logger.info(f'重试翻译失败的文件: {task_id}')
        logger.info(f'文件夹路径: {folder_path}')
        logger.info(f'重试文件数: {len(failed_files)}\n')
        
        # 重置这些文件的翻译状态为pending
        for filename in failed_files:
            self.task_manager.update_file_translation_status(task_id, filename, 'pending', '')
        
        # 复用翻译处理逻辑
        self.process_translation_only(task_id, num_threads)
    
    def _translate_single_file(self, task_id: str, archive_path: str, filename: str) -> bool:
        """翻译单个文件"""
        with self._lock:
            logger.info(f'翻译: {filename}')
        
        success = self.translate_metadata_in_cbz(archive_path, task_id, filename)
        
        if not success:
            with self._lock:
                logger.error(f'✗ 翻译失败: {filename}')
        
        return success
    
    def process_task(self, task_id: str, num_threads: int = 5, retry_delay: float = 2.0, retry_failed: bool = False):
        task_data = self.task_manager.get_task(task_id)
        
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        folder_path = Path(task_data['folder_path'])
        
        if not folder_path.exists():
            logger.error(f'文件夹不存在: {folder_path}')
            return
        
        # 修改: 根据 retry_failed 标志选择待处理文件
        if retry_failed:
            files_to_process = self.task_manager.get_failed_files(task_id)
            logger.info('模式: 重试失败任务')
        else:
            files_to_process = self.task_manager.get_pending_files(task_id)
            logger.info('模式: 处理未完成任务')
        
        if not files_to_process:
            logger.info('该任务中没有符合条件的文件')
            self.print_task_summary(task_id)
            return
        
        mode = "CBZ转换" if self.to_cbz else "标准处理"
        logger.info(f'开始处理任务: {task_id} ({mode})')
        logger.info(f'文件夹路径: {folder_path}')
        logger.info(f'待处理文件数: {len(files_to_process)}\n')

        # 新增: 注册信号处理函数
        def signal_handler(signum, frame):
            logger.warning('\n!!! 接收到退出信号 (Ctrl+C) !!!')
            logger.warning('正在停止分发新任务，请等待当前正在运行的线程完成...')
            logger.warning('这可能需要几秒钟，请勿强制关闭窗口，以免损坏文件。')
            self._shutdown_requested.set()

        # 保存旧的信号处理程序并在结束后恢复
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {}
                
                for idx, filename in enumerate(files_to_process):
                    # 检查是否请求退出
                    if self._shutdown_requested.is_set():
                        logger.warning('已停止分发新任务。')
                        break

                    archive_path = folder_path / filename
                    # 提交任务
                    future = executor.submit(self.process_file, task_id, str(archive_path), filename)
                    futures[future] = filename
                    
                    # 延迟逻辑
                    if (idx + 1) % num_threads == 0:
                        # 使用循环 sleep 以便更快响应中断，而不是一次睡很久
                        start_sleep = time.time()
                        while time.time() - start_sleep < retry_delay:
                            if self._shutdown_requested.is_set():
                                break
                            time.sleep(0.1)
                
                # 等待任务完成，但响应退出信号
                if futures:
                    completed_futures = 0
                    total_futures = len(futures)
                    
                    try:
                        for future in as_completed(futures, timeout=1.0):  # 1秒超时
                            try:
                                future.result(timeout=10.0)  # 每个任务最多等待10秒
                                completed_futures += 1
                                if completed_futures % 5 == 0:  # 每完成5个任务报告一次进度
                                    logger.info(f'已完成 {completed_futures}/{total_futures} 个任务')
                            except Exception as e:
                                logger.error(f'处理异常: {e}')
                            
                            # 检查是否需要退出
                            if self._shutdown_requested.is_set():
                                logger.warning('检测到退出请求，正在清理...')
                                # 取消未完成的任务
                                cancelled_count = 0
                                for pending_future in futures:
                                    if not pending_future.done():
                                        pending_future.cancel()
                                        cancelled_count += 1
                                logger.warning(f'已取消 {cancelled_count} 个未完成任务')
                                break
                    except TimeoutError:
                        logger.warning('任务等待超时')
                    except KeyboardInterrupt:
                        logger.warning('接收到键盘中断')
                        self._shutdown_requested.set()
                
        finally:
            # 恢复原始信号处理
            signal.signal(signal.SIGINT, original_sigint_handler)
            if self._shutdown_requested.is_set():
                logger.success('安全退出: 所有进行中的任务已完成或已取消。')
                logger.info('提示: 您可以使用 --retry 参数继续处理未完成的文件。')

        self.print_task_summary(task_id)
    
    def print_task_summary(self, task_id: str):
        """打印任务处理摘要"""
        task_data = self.task_manager.get_task(task_id)
        if not task_data:
            return

        files = task_data['files']

        total = len(files)
        success = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_SUCCESS)
        failed = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_FAILED)
        skipped = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_SKIPPED)
        pending = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_PENDING)

        print("\n" + "="*60)
        print(f"任务处理完成！任务ID: {task_id}")
        print("="*60)
        print(f"总数:     {total}")
        print(f"成功:     {success}")
        print(f"失败:     {failed}")
        print(f"跳过:     {skipped}")
        print(f"待处理:   {pending}")
        print("="*60 + "\n")
    
    def print_translation_summary(self, task_id: str):
        """打印翻译任务摘要"""
        task_data = self.task_manager.get_task(task_id)
        if not task_data:
            return

        files = task_data['files']
        stats = task_data['statistics']

        # 初始化翻译统计
        translation_success = stats.get('translation_success', 0)
        translation_failed = stats.get('translation_failed', 0)
        translation_pending = stats.get('translation_pending', 0)

        print("\n" + "="*60)
        print(f"翻译任务完成！任务ID: {task_id}")
        print("="*60)
        print(f"翻译成功: {translation_success}")
        print(f"翻译失败: {translation_failed}")
        print(f"待翻译:   {translation_pending}")
        
        # 显示未翻译标签信息
        untranslated_file = os.path.join(task_data['folder_path'], 'untranslated_tags.json')
        if os.path.exists(untranslated_file):
            try:
                with open(untranslated_file, 'r', encoding='utf-8') as f:
                    untranslated_data = json.load(f)
                    tag_count = len(untranslated_data.get('tags', []))
                    print(f"未翻译标签: {tag_count} 个")
                    if tag_count > 0:
                        print(f"详情请查看: {untranslated_file}")
            except:
                pass
        
        print("="*60 + "\n")
        task_data = self.task_manager.get_task(task_id)
        if not task_data:
            return

        files = task_data['files']

        total = len(files)
        success = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_SUCCESS)
        failed = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_FAILED)
        skipped = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_SKIPPED)
        pending = sum(1 for f in files.values() if f['status'] == TaskManager.STATUS_PENDING)

        print("\n" + "="*60)
        print(f"任务处理完成！任务ID: {task_id}")
        print("="*60)
        print(f"总数:     {total}")
        print(f"成功:     {success}")
        print(f"失败:     {failed}")
        print(f"跳过:     {skipped}")
        print(f"待处理:   {pending}")
        print("="*60 + "\n")


# ==================== 辅助函数 ====================
# (辅助函数保持不变)
def display_tasks(task_manager: TaskManager):
    tasks = task_manager.list_tasks()
    
    if not tasks:
        print("\n没有任何任务\n")
        return
    
    print("\n" + "="*80)
    print("所有任务列表")
    print("="*80)
    
    for task_id, task_data in tasks.items():
        stats = task_data['statistics']
        print(f"\n任务ID:   {task_id}")
        print(f"文件夹:   {task_data['folder_path']}")
        print(f"创建时间: {task_data['created_at']}")
        print(f"更新时间: {task_data['updated_at']}")
        print(f"统计信息: 总数={stats['total']}, 成功={stats['success']}, " +
              f"失败={stats['failed']}, 跳过={stats['skipped']}, 待处理={stats['pending']}")
    
    print("\n" + "="*80 + "\n")


def display_task_info(task_manager: TaskManager, task_id: str):
    task_data = task_manager.get_task(task_id)
    
    if not task_data:
        logger.error(f'任务不存在: {task_id}')
        return
    
    stats = task_data['statistics']
    print("\n" + "="*80)
    print(f"任务详情 - {task_id}")
    print("="*80)
    print(f"文件夹路径: {task_data['folder_path']}")
    print(f"创建时间:   {task_data['created_at']}")
    print(f"更新时间:   {task_data['updated_at']}")
    print(f"\n统计信息:")
    print(f"  总数:     {stats['total']}")
    print(f"  成功:     {stats['success']}")
    print(f"  失败:     {stats['failed']}")
    print(f"  跳过:     {stats['skipped']}")
    print(f"  待处理:   {stats['pending']}")
    
    print(f"\n文件列表 (按状态分类):")
    
    for status in ['success', 'failed', 'skipped', 'pending']:
        files_with_status = [(fname, finfo) for fname, finfo in task_data['files'].items() 
                            if finfo['status'] == status]
        
        if files_with_status:
            print(f"\n  {status.upper()} ({len(files_with_status)}):")
            for filename, file_info in files_with_status[:10]:
                if file_info['error']:
                    print(f"    - {filename} ({file_info['error']})")
                else:
                    print(f"    - {filename}")
            
            if len(files_with_status) > 10:
                print(f"    ... 还有 {len(files_with_status) - 10} 个文件")
    
    print("\n" + "="*80 + "\n")


def confirm_delete(task_id: str, task_path: str) -> bool:
    print(f"\n⚠️  确认删除任务?")
    print(f"  任务ID: {task_id}")
    print(f"  文件夹: {task_path}")
    print(f"  注意: 这只会删除任务记录，不会删除文件夹中的ZIP/CBZ文件")
    
    confirm = input("\n确认删除? (yes/no): ").strip().lower()
    return confirm in ('yes', 'y')


# ==================== 主程序 ====================

def main():
    parser = argparse.ArgumentParser(
        description='为漫画ZIP包添加JSON元数据',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--setup', action='store_true', help='配置Cookie、UA和Proxy')
    parser.add_argument('--folder', '-f', help='为文件夹创建新任务')
    parser.add_argument('--list-tasks', action='store_true', help='列出所有任务')
    parser.add_argument('--task-info', help='查看指定任务的详情')
    parser.add_argument('--start-task', help='开始处理指定任务')
    parser.add_argument('--delete-task', help='删除指定任务')
    parser.add_argument('--debug-zip', help='分析指定ZIP文件的内部结构')
    parser.add_argument('--debug', action='store_true', help='启用Debug模式')
    parser.add_argument('--threads', '-t', type=int, default=None, help='线程数')
    parser.add_argument('--dry-run', action='store_true', help='预演模式')
    parser.add_argument('--delay', type=float, default=2.0, help='请求延迟')
    parser.add_argument('--export-failed', action='store_true', help='导出失败的文件')
    parser.add_argument('--export-success', action='store_true', help='导出成功的文件')
    parser.add_argument('--export-pending', action='store_true', help='导出待处理的文件')
    parser.add_argument('--export-translation-stats', action='store_true', help='导出翻译统计信息')
    parser.add_argument('--to-cbz', action='store_true', help='将ZIP转换为CBZ并添加ComicInfo.xml')
    # 新增: 重试参数
    parser.add_argument('--retry', action='store_true', help='仅重试状态为 Failed 的任务')
    # 新增: 翻译相关参数
    parser.add_argument('--translate-only', action='store_true', help='仅执行翻译，不进行其他元数据处理')
    parser.add_argument('--retry-failed-translations', action='store_true', help='重试翻译失败的文件')
    parser.add_argument('--translation-database', default='database.json', help='指定翻译词典文件路径')
    parser.add_argument('--disk-limit', type=int, default=0, help='磁盘I/O限制（MB/s），0表示无限制')
    parser.add_argument('--batch-size', type=int, default=10, help='批处理大小，默认10个文件')
    parser.add_argument('--cache-size', type=int, default=50, help='内存缓存大小，默认50个文件')
    parser.add_argument('--batch-mode', action='store_true', help='启用批处理模式（减少磁盘I/O）')
    
    args = parser.parse_args()
    
    config = Config()
    debug_log_file = None
    if args.debug:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        debug_log_file = os.path.join(config.debug_dir, f'debug_{timestamp}.log')
    
    init_logger(debug_mode=args.debug, debug_log_file=debug_log_file)
    
    try:
        if args.debug_zip:
            logger.info('进入Debug模式: 分析ZIP文件')
            if not os.path.exists(args.debug_zip):
                logger.error(f'文件不存在: {args.debug_zip}')
                return
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_name = os.path.splitext(os.path.basename(args.debug_zip))[0]
            report_file = os.path.join(config.debug_dir, f'{zip_name}_{timestamp}_analysis.txt')
            
            logger.info(f'正在分析ZIP文件: {args.debug_zip}')
            ZipDebugger.dump_zip_structure(args.debug_zip, report_file)
            logger.success(f'分析报告已生成: {report_file}')
            return
        
        if args.setup:
            config.setup()
            return
        
        if args.list_tasks:
            display_tasks(TaskManager(config))
            return
        
        if args.task_info:
            display_task_info(TaskManager(config), args.task_info)
            return
        
        if args.delete_task:
            task_manager = TaskManager(config)
            task_data = task_manager.get_task(args.delete_task)
            if task_data:
                if confirm_delete(args.delete_task, task_data['folder_path']):
                    task_manager.delete_task(args.delete_task)
            return
        
        if args.folder:
            task_manager = TaskManager(config)
            task_id = task_manager.create_task(args.folder)
            logger.success(f'任务已创建，任务ID: {task_id}')
            print(f'  使用命令开始处理: python add_manga_metadata.py --start-task {task_id}')
            print(f'  或查看任务详情: python add_manga_metadata.py --task-info {task_id}\n')
            return
        
        if args.start_task:
            task_manager = TaskManager(config)
            task_data = task_manager.get_task(args.start_task)
            if not task_data:
                logger.error(f'任务不存在: {args.start_task}')
                return
            
            if not config.get('cookie') and not config.get('useragent'):
                logger.warning('未检测到配置，建议先运行 --setup')
            
            if args.export_failed:
                output_file = os.path.join(task_data['folder_path'], 'failed_files.log')
                task_manager.export_status_log(args.start_task, TaskManager.STATUS_FAILED, output_file)
                return
            
            if args.export_success:
                output_file = os.path.join(task_data['folder_path'], 'success_files.log')
                task_manager.export_status_log(args.start_task, TaskManager.STATUS_SUCCESS, output_file)
                return
            
            if args.export_pending:
                output_file = os.path.join(task_data['folder_path'], 'pending_files.log')
                task_manager.export_status_log(args.start_task, TaskManager.STATUS_PENDING, output_file)
                return
            
            if args.export_translation_stats:
                output_file = os.path.join(task_data['folder_path'], 'translation_stats.txt')
                task_manager.export_translation_stats(args.start_task, output_file)
                return
            
            # 处理翻译相关参数
            if args.translate_only:
                # 仅翻译模式
                adder = ZipMetadataAdder(None, task_manager, dry_run=args.dry_run, to_cbz=args.to_cbz)
                adder.translator = Translator(args.translation_database)  # 使用指定的词典
                
                num_threads = args.threads or config.get('threads', 5)
                print(f"\n使用 {num_threads} 个线程进行翻译...\n")
                adder.process_translation_only(args.start_task, num_threads=num_threads)
                return
            
            if args.retry_failed_translations:
                # 重试翻译失败的文件
                adder = ZipMetadataAdder(None, task_manager, dry_run=args.dry_run, to_cbz=args.to_cbz)
                adder.translator = Translator(args.translation_database)  # 使用指定的词典
                
                num_threads = args.threads or config.get('threads', 5)
                print(f"\n使用 {num_threads} 个线程重试翻译失败的文件...\n")
                adder.retry_failed_translations(args.start_task, num_threads=num_threads)
                return
            
            # 标准处理模式（包含翻译）
            client = NetClient(config)
            parser_obj = MetadataParser(client)
            adder = ZipMetadataAdder(parser_obj, task_manager, dry_run=args.dry_run, 
                                   to_cbz=args.to_cbz, disk_limit=args.disk_limit)
            adder.translator = Translator(args.translation_database)  # 使用指定的词典
            
            num_threads = args.threads or config.get('threads', 5)
            
            mode = "CBZ转换" if args.to_cbz else "标准处理"
            if args.retry:
                mode += " (重试模式)"
            mode += " (含自动翻译)"
            
            # 添加I/O限制信息
            if args.disk_limit > 0:
                mode += f" (I/O限制: {args.disk_limit} MB/s)"
                
            print(f"\n使用 {num_threads} 个线程处理... ({mode})")
            if args.batch_size and args.batch_size > 0:
                print(f"批处理大小: {args.batch_size} 个文件")
            
            # 更新缓存大小
            adder.cache_manager.max_size = args.cache_size
            
            if args.batch_mode:
                # 使用批处理模式
                logger.info(f'启用批处理模式，批大小: {args.batch_size}')
                adder.process_task_batch(args.start_task, num_threads=num_threads, batch_size=args.batch_size)
            else:
                # 标准处理模式
                adder.process_task(args.start_task, num_threads=num_threads, 
                                 retry_delay=args.delay, retry_failed=args.retry)
            return
        
        parser.print_help()
    
    finally:
        if logger and hasattr(logger, 'close'):
            logger.close()


if __name__ == '__main__':
    main()
