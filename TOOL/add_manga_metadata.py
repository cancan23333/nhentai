# coding: utf-8
"""
为没有元数据的漫画ZIP包添加元数据 (增强版)

功能特性:
- 自动从nhentai获取元数据并注入ZIP/CBZ
- 支持多线程并发处理
- 支持断点续传和任务管理
- [新增] 支持重试失败的任务 (--retry-failed)
- [新增] 支持安全退出 (Ctrl+C)，防止文件损坏
- 支持转换为CBZ格式并生成ComicInfo.xml

依赖安装:
    pip install beautifulsoup4 curl-cffi

使用方法:
    # 首次配置
    python add_manga_metadata.py --setup

    # 创建任务
    python add_manga_metadata.py --folder "C:\Comics"

    # 开始任务
    python add_manga_metadata.py --start-task <TASK_ID>

    # 重试失败的文件
    python add_manga_metadata.py --start-task <TASK_ID> --retry-failed
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
import signal
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

IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.tiff', '.ico'}
SUPPORTED_EXTENSIONS = {'.zip', '.cbz'}


# ==================== 日志模块 ====================

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


# ==================== ComicInfo生成器 ====================

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
                
                if len(root_level_items) == 1:
                    single_item = list(root_level_items)[0]
                    is_folder = any('/' in item.rstrip('/') and item.rstrip('/').split('/')[0] == single_item 
                                   for item in file_list if '/' in item)
                    
                    if is_folder:
                        return 'nested', single_item
                    else:
                        return 'flat', None
                else:
                    return 'flat', None
        
        except Exception as e:
            logger.error(f'分析ZIP结构失败: {e}')
            return 'unknown', None
    
    @staticmethod
    def convert_nested_to_flat(zip_path: str) -> bool:
        try:
            structure_type, folder_name = ZipStructureConverter.analyze_zip_structure(zip_path)
            
            if structure_type != 'nested' or not folder_name:
                return True
            
            logger.info(f'正在转换ZIP结构: {os.path.basename(zip_path)}')
            
            temp_zip = zip_path + '.tmp'
            folder_prefix = folder_name + '/'
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_read:
                    with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                        for item in zip_read.namelist():
                            item_clean = item.rstrip('/')
                            
                            if item_clean == folder_name:
                                continue
                            
                            if item.startswith(folder_prefix):
                                new_name = item[len(folder_prefix):]
                                
                                if new_name and not item.endswith('/'):
                                    content = zip_read.read(item)
                                    zip_write.writestr(new_name, content)
                
                backup_zip = zip_path + '.backup'
                os.rename(zip_path, backup_zip)
                os.rename(temp_zip, zip_path)
                
                try:
                    os.remove(backup_zip)
                except:
                    pass
                
                logger.success(f'✓ ZIP结构已转换为扁平结构')
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
        if task_id not in self.tasks:
            return
        
        task_data = self.tasks[task_id]
        
        if filename not in task_data['files']:
            task_data['files'][filename] = {
                'status': self.STATUS_PENDING,
                'error': '',
                'updated_at': ''
            }
        
        old_status = task_data['files'][filename]['status']
        task_data['files'][filename]['status'] = status
        task_data['files'][filename]['error'] = error_msg
        task_data['files'][filename]['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if old_status == self.STATUS_PENDING:
            task_data['statistics']['pending'] -= 1
        elif old_status == self.STATUS_SUCCESS:
            task_data['statistics']['success'] -= 1
        elif old_status == self.STATUS_FAILED:
            task_data['statistics']['failed'] -= 1
        elif old_status == self.STATUS_SKIPPED:
            task_data['statistics']['skipped'] -= 1
        
        if status == self.STATUS_SUCCESS:
            task_data['statistics']['success'] += 1
        elif status == self.STATUS_FAILED:
            task_data['statistics']['failed'] += 1
        elif status == self.STATUS_SKIPPED:
            task_data['statistics']['skipped'] += 1
        elif status == self.STATUS_PENDING:
            task_data['statistics']['pending'] += 1
        
        task_data['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.tasks[task_id] = task_data
        self._save_task(task_id, task_data)
    
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
    
    def get_failed_files(self, task_id: str) -> List[str]:
        """获取任务中标记为failed的文件列表"""
        task_data = self.tasks.get(task_id)
        if not task_data:
            return []
        
        failed = []
        for filename, file_info in task_data['files'].items():
            if file_info['status'] == self.STATUS_FAILED:
                failed.append(filename)
        return failed
    
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
            return None


# ==================== ZIP处理 ====================

class ZipMetadataAdder:
    FILENAME_PATTERN = re.compile(r'^\[(\d{6})\](.+)\.(zip|cbz)$', re.IGNORECASE)
    
    def __init__(self, parser: MetadataParser, task_manager: TaskManager, dry_run: bool = False, to_cbz: bool = False):
        self.parser = parser
        self.task_manager = task_manager
        self.dry_run = dry_run
        self.to_cbz = to_cbz
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
    
    def extract_id(self, filename: str) -> Optional[str]:
        match = self.FILENAME_PATTERN.match(filename)
        if match:
            return match.group(1)
        return None
    
    def process_zip_file(self, zip_path: str, metadata: Dict) -> Tuple[bool, Optional[str]]:
        try:
            if self.dry_run:
                logger.debug(f'[DRY RUN] 处理: {os.path.basename(zip_path)}')
                return True, None
            
            structure_type, folder_name = ZipStructureConverter.analyze_zip_structure(zip_path)
            
            if structure_type == 'nested':
                if not ZipStructureConverter.convert_nested_to_flat(zip_path):
                    logger.error('ZIP结构转换失败')
                    return False, None
            
            temp_zip = zip_path + '.tmp'
            
            with zipfile.ZipFile(zip_path, 'r') as zip_read:
                with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zip_write:
                    for item in zip_read.namelist():
                        if item in ('metadata.json', 'ComicInfo.xml'):
                            continue
                        zip_write.writestr(item, zip_read.read(item))
                    
                    json_content = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
                    zip_write.writestr('metadata.json', json_content.encode('utf-8'))
                    
                    if self.to_cbz or zip_path.lower().endswith('.cbz'):
                        comic_info_content = ComicInfoGenerator.generate_comic_info(metadata)
                        if comic_info_content:
                            zip_write.writestr('ComicInfo.xml', comic_info_content.encode('utf-8'))
            
            os.remove(zip_path)
            os.rename(temp_zip, zip_path)
            
            new_path = None
            if self.to_cbz and zip_path.lower().endswith('.zip'):
                cbz_path = zip_path[:-4] + '.cbz'
                os.rename(zip_path, cbz_path)
                new_path = cbz_path
            
            return True, new_path
        
        except Exception as e:
            logger.error(f'处理ZIP文件失败: {e}')
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
        
        metadata = self.parser.parse(manga_id)
        
        if not metadata:
            with self._lock:
                self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_FAILED, '获取元数据失败')
            return False
        
        success, new_path = self.process_zip_file(archive_path, metadata)
        
        if success:
            with self._lock:
                if new_path and new_path != archive_path:
                    new_filename = os.path.basename(new_path)
                    if filename in self.task_manager.get_task(task_id)['files']:
                        file_info = self.task_manager.get_task(task_id)['files'].pop(filename)
                        file_info['status'] = TaskManager.STATUS_SUCCESS
                        self.task_manager.get_task(task_id)['files'][new_filename] = file_info
                        self.task_manager._save_task(task_id, self.task_manager.get_task(task_id))
                    logger.success(f'✓ {filename} -> {new_filename}')
                else:
                    logger.success(f'✓ {filename}')
                    self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_SUCCESS)
            return True
        else:
            with self._lock:
                self.task_manager.update_file_status(task_id, filename, TaskManager.STATUS_FAILED, '处理文件失败')
            return False
    
    def process_task(self, task_id: str, num_threads: int = 5, retry_delay: float = 2.0, retry_failed: bool = False):
        task_data = self.task_manager.get_task(task_id)
        
        if not task_data:
            logger.error(f'任务不存在: {task_id}')
            return
        
        folder_path = Path(task_data['folder_path'])
        
        if not folder_path.exists():
            logger.error(f'文件夹不存在: {folder_path}')
            return
        
        if retry_failed:
            files_to_process = self.task_manager.get_failed_files(task_id)
            target_status_desc = "失败"
        else:
            files_to_process = self.task_manager.get_pending_files(task_id)
            target_status_desc = "待处理"
        
        if not files_to_process:
            logger.info(f'该任务中没有{target_status_desc}的文件')
            self.print_task_summary(task_id)
            return
        
        mode = "CBZ转换" if self.to_cbz else "标准处理"
        logger.info(f'开始处理任务: {task_id} ({mode})')
        logger.info(f'模式: 处理{target_status_desc}文件')
        logger.info(f'文件夹路径: {folder_path}')
        logger.info(f'目标文件数: {len(files_to_process)}\n')
        
        # 信号处理逻辑
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        
        def signal_handler(sig, frame):
            print("\n\n" + "!"*60)
            print("收到停止信号 (Ctrl+C)")
            print("正在停止分发新任务，请等待当前正在运行的线程结束...")
            print("此过程可能需要几秒钟，请勿强制关闭窗口...")
            print("!"*60 + "\n")
            self._stop_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        self._stop_event.clear()
        
        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {}
                
                for idx, filename in enumerate(files_to_process):
                    if self._stop_event.is_set():
                        logger.warning("已停止分发新任务，正在等待线程池清空...")
                        break
                    
                    archive_path = folder_path / filename
                    future = executor.submit(self.process_file, task_id, str(archive_path), filename)
                    futures[future] = filename
                    
                    if (idx + 1) % num_threads == 0:
                        time.sleep(retry_delay)
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'处理异常: {e}')
        
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)
            if self._stop_event.is_set():
                print("\n已安全退出。未处理的文件保持原状态。")
        
        self.print_task_summary(task_id)
    
    def print_task_summary(self, task_id: str):
        task_data = self.task_manager.get_task(task_id)
        if not task_data:
            return
        
        stats = task_data['statistics']
        print("\n" + "="*60)
        print(f"任务处理完成！任务ID: {task_id}")
        print("="*60)
        print(f"总数:     {stats['total']}")
        print(f"成功:     {stats['success']}")
        print(f"失败:     {stats['failed']}")
        print(f"跳过:     {stats['skipped']}")
        print(f"待处理:   {stats['pending']}")
        print("="*60 + "\n")


# ==================== 辅助函数 ====================

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
    parser.add_argument('--to-cbz', action='store_true', help='将ZIP转换为CBZ并添加ComicInfo.xml')
    parser.add_argument('--retry-failed', action='store_true', help='重试任务中标记为失败的文件')
    
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
            
            client = NetClient(config)
            parser_obj = MetadataParser(client)
            adder = ZipMetadataAdder(parser_obj, task_manager, dry_run=args.dry_run, to_cbz=args.to_cbz)
            
            num_threads = args.threads or config.get('threads', 5)
            
            mode = "CBZ转换" if args.to_cbz else "标准处理"
            print(f"\n使用 {num_threads} 个线程处理... ({mode})\n")
            
            adder.process_task(
                args.start_task, 
                num_threads=num_threads, 
                retry_delay=args.delay,
                retry_failed=args.retry_failed
            )
            return
        
        parser.print_help()
    
    finally:
        if logger and hasattr(logger, 'close'):
            logger.close()


if __name__ == '__main__':
    main()
