import asyncio
import hashlib
import json
import logging
import os
import pathlib
import platform
import time
from multiprocessing import Process
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import websockets
from dotenv import load_dotenv
load_dotenv()

FORCE_IGNORED_FILES = {".devsync_init_done"}

# -----------------------------------------------------------------------------
#  SyncUtils - Утилиты для синхронизации
# -----------------------------------------------------------------------------

def get_file_hash(filepath: str) -> str:
    """Вычисляет SHA256 хеш-сумму содержимого файла."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def generate_file_event(action: str, filepath: str, base_path: str, node_id: str) -> dict:
    """Формирует стандартизованное событие файла."""
    is_directory = os.path.isdir(filepath)
    event = {
        "action": action,
        "path": os.path.relpath(filepath, base_path),
        "is_directory": is_directory,
        "timestamp": os.path.getmtime(filepath) if os.path.exists(filepath) else time.time(),
        "source_node_id": node_id,
        "content": None,
        "hash": None,
    }
    if not is_directory and action != "deleted" and os.path.exists(filepath):
        with open(filepath, "rb") as f:
            event["content"] = f.read().decode('utf-8', 'ignore')
        event["hash"] = get_file_hash(filepath)
    return event

def get_full_directory_state(base_path: str, node_id: str) -> list:
    """Собирает полный список файлов и директорий для начальной синхронизации."""
    state = []
    for root, dirs, files in os.walk(base_path):
        for name in files:
            filepath = os.path.join(root, name)
            if os.path.basename(filepath) not in FORCE_IGNORED_FILES:
                state.append(generate_file_event("created", filepath, base_path, node_id))
        for name in dirs:
            dirpath = os.path.join(root, name)
            state.append(generate_file_event("created", dirpath, base_path, node_id))
    return state

def apply_file_event(event_data: dict, base_path: str, local_node_id: str):
    """Применяет полученное событие к локальной файловой системе."""
    full_path = os.path.join(base_path, event_data["path"])
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    if event_data["action"] == "deleted":
        if os.path.exists(full_path):
            if event_data["is_directory"]:
                os.rmdir(full_path)
            else:
                os.remove(full_path)
        return

    if event_data["is_directory"]:
        os.makedirs(full_path, exist_ok=True)
        return

    # Логика разрешения конфликтов
    if os.path.exists(full_path):
        local_timestamp = os.path.getmtime(full_path)
        remote_timestamp = event_data["timestamp"]

        if remote_timestamp < local_timestamp:
            # Локальная версия новее, игнорируем
            return
        elif remote_timestamp == local_timestamp:
            local_hash = get_file_hash(full_path)
            remote_hash = event_data["hash"]
            if local_hash == remote_hash:
                # Файлы идентичны
                return
            else:
                # Конфликт!
                conflict_path = f"{full_path}.conflict-FROM-{event_data['source_node_id']}-{time.strftime('%Y%m%d_%H%M%S')}"
                with open(conflict_path, "wb") as f:
                    f.write(event_data["content"].encode('utf-8', 'ignore'))
                logging.warning(f"Conflict detected for {full_path}. Remote version saved to {conflict_path}")
                return

    with open(full_path, "wb") as f:
        f.write(event_data["content"].encode('utf-8', 'ignore'))
    os.utime(full_path, (event_data["timestamp"], event_data["timestamp"]))


# -----------------------------------------------------------------------------
#  FileMonitor - Мониторинг файловой системы
# -----------------------------------------------------------------------------

class FileMonitor(FileSystemEventHandler):
    def __init__(self, sync_worker):
        self.sync_worker = sync_worker
        self.ignore_patterns = []
        self.update_ignore_patterns()

    def update_ignore_patterns(self):
        """Загружает и обновляет паттерны игнорирования из файла .devsyncignore."""
        ignore_file = os.path.join(self.sync_worker.sync_root_dir, ".devsyncignore")
        if os.path.exists(ignore_file):
            with open(ignore_file, "r") as f:
                self.ignore_patterns = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    def is_ignored(self, path: str) -> bool:
        """Проверяет, должен ли путь быть проигнорирован."""
        # Жестко закодированная проверка для файла-маркера
        if os.path.basename(path) in FORCE_IGNORED_FILES:
            return True

        rel_path = os.path.relpath(path, self.sync_worker.sync_root_dir)
        for pattern in self.ignore_patterns:
            if pathlib.Path(rel_path).match(pattern):
                return True
        return False

    def on_any_event(self, event):
        if self.is_ignored(event.src_path):
            return

        action = None
        if event.event_type == 'created':
            action = 'created'
        elif event.event_type == 'modified':
            action = 'modified'
        elif event.event_type == 'deleted':
            action = 'deleted'
        elif event.event_type == 'moved':
            # Обработка как удаление и создание
            if not self.is_ignored(event.dest_path):
                delete_event = generate_file_event("deleted", event.src_path, self.sync_worker.sync_root_dir, self.sync_worker.node_id)
                self.sync_worker.broadcast(delete_event)
            action = 'created'
            event.src_path = event.dest_path

        if action:
            file_event = generate_file_event(action, event.src_path, self.sync_worker.sync_root_dir, self.sync_worker.node_id)
            self.sync_worker.broadcast(file_event)

# -----------------------------------------------------------------------------
#  SyncWorker - Узел-Воркер для синхронизации
# -----------------------------------------------------------------------------

class SyncWorker(Process):
    def __init__(self, project_config, node_config, all_nodes):
        super().__init__()
        self.project_config = project_config
        self.node_config = node_config
        self.all_nodes = all_nodes
        self.sync_root_dir = os.path.join(node_config["sync_root_base_dir"], project_config["name"])
        self.node_id = f"{node_config['id']}-{project_config['name']}"
        self.port = project_config["port"]
        self.target_nodes = self._get_target_nodes()
        self.clients = {}
        self.init_marker_path = os.path.join(self.sync_root_dir, ".devsync_init_done")

    def _get_target_nodes(self):
        """Получает список целевых узлов с их IP-адресами в порядке приоритета (LAN, затем VPN)."""
        nodes = []
        for node in self.all_nodes:
            if node["id"] != self.node_config["id"]:
                node_info = {
                    "id": node["id"],
                    "ips": [node["ip_lan"], node["ip_vpn"]]  # LAN имеет приоритет
                }
                nodes.append(node_info)
        return nodes

    def _get_all_target_urls(self):
        """Генерирует все возможные URL для подключения с приоритетом LAN."""
        urls = []
        for node in self.target_nodes:
            for ip in node["ips"]:
                urls.append(f"ws://{ip}:{self.port}")
        return urls

    async def handle_client(self, websocket, path=None):
        self.clients[websocket] = websocket.remote_address
        try:
            async for message in websocket:
                event_data = json.loads(message)

                # Обработка запроса на полную синхронизацию
                if event_data.get("action") == "request_full_sync":
                    logging.info(f"Received full sync request from {websocket.remote_address}. Preparing state...")
                    full_state = get_full_directory_state(self.sync_root_dir, self.node_id)
                    await websocket.send(json.dumps(full_state))
                    logging.info(f"Full state sent to {websocket.remote_address}.")
                    continue

                if event_data["source_node_id"] != self.node_id:
                    # Эхо-защита на уровне обработчика
                    if not self.is_change_local(event_data):
                         apply_file_event(event_data, self.sync_root_dir, self.node_id)
                         await self.broadcast_to_others(event_data, websocket)

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logging.error(f"Error in handle_client: {e}")
        finally:
            if websocket in self.clients:
                del self.clients[websocket]

    async def broadcast_to_others(self, event_data, sender_websocket):
        message = json.dumps(event_data)
        disconnected_clients = []
        for client in self.clients:
            if client != sender_websocket:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected_clients.append(client)
        
        # Удаляем отключенных клиентов
        for client in disconnected_clients:
            if client in self.clients:
                del self.clients[client]

    def broadcast(self, event_data):
        asyncio.run(self._async_broadcast(event_data))

    async def _async_broadcast(self, event_data):
        message = json.dumps(event_data)
        disconnected_clients = []
        for client in self.clients:
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.append(client)
        
        # Удаляем отключенных клиентов
        for client in disconnected_clients:
            if client in self.clients:
                del self.clients[client]

    async def connect_to_peers(self):
        while True:
            # Получаем список всех URL для попытки подключения
            all_urls = self._get_all_target_urls()
            
            for url in all_urls:
                try:
                    # Проверяем, не подключены ли мы уже к этому URL
                    is_connected = False
                    for client_ws in list(self.clients.keys()):
                        try:
                            if hasattr(client_ws, 'remote_address') and client_ws.remote_address:
                                remote_url = f"ws://{client_ws.remote_address[0]}:{self.port}"
                                if remote_url == url:
                                    is_connected = True
                                    break
                        except Exception:
                            pass
                    
                    if not is_connected:
                        websocket = await websockets.connect(url, open_timeout=5)
                        self.clients[websocket] = websocket.remote_address
                        # Запускаем обработчик для исходящего соединения
                        asyncio.create_task(self.handle_outgoing_connection(websocket))
                        logging.info(f"Successfully connected to {url}.")
                except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    logging.error(f"An unexpected error occurred when connecting to {url}: {e}")

            await asyncio.sleep(10)

    async def handle_outgoing_connection(self, websocket):
        """Обрабатывает исходящие соединения с пирами."""
        try:
            async for message in websocket:
                event_data = json.loads(message)
                
                # Обработка ответа на запрос полной синхронизации
                if isinstance(event_data, list):
                    # Это полный список состояния
                    for item in event_data:
                        if item["source_node_id"] != self.node_id:
                            apply_file_event(item, self.sync_root_dir, self.node_id)
                    continue

                if event_data["source_node_id"] != self.node_id:
                    apply_file_event(event_data, self.sync_root_dir, self.node_id)
                    await self.broadcast_to_others(event_data, websocket)

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logging.error(f"Error in handle_outgoing_connection: {e}")
        finally:
            if websocket in self.clients:
                del self.clients[websocket]

    async def initial_sync(self):
        if os.path.exists(self.init_marker_path):
            logging.info(f"Initial sync marker found for {self.project_config['name']}. Skipping initial sync.")
            return

        source_node_id = self.project_config.get("initial_sync_source_node_id")
        if not source_node_id or source_node_id == self.node_config["id"]:
            if not os.path.exists(self.init_marker_path):
                 with open(self.init_marker_path, 'w') as f:
                    pass
                 logging.info(f"This node is the initial source. Marker file created for {self.project_config['name']}.")
            return

        source_node = next((n for n in self.all_nodes if n["id"] == source_node_id), None)
        if not source_node:
            logging.warning(f"Initial sync source node '{source_node_id}' not found in config.")
            return

        # Пытаемся подключиться сначала к LAN, потом к VPN
        source_urls = [
            f"ws://{source_node['ip_lan']}:{self.port}",
            f"ws://{source_node['ip_vpn']}:{self.port}"
        ]
        
        while not os.path.exists(self.init_marker_path):
            sync_successful = False
            
            for url in source_urls:
                try:
                    logging.info(f"Attempting initial sync from {url}...")
                    async with websockets.connect(url, open_timeout=10) as websocket:
                        await websocket.send(json.dumps({"action": "request_full_sync"}))
                        full_state_json = await asyncio.wait_for(websocket.recv(), timeout=60.0)
                        full_state = json.loads(full_state_json)

                        for item in full_state:
                            apply_file_event(item, self.sync_root_dir, self.node_id)
                        
                        with open(self.init_marker_path, 'w') as f:
                            pass
                        logging.info(f"Initial sync completed for {self.project_config['name']} from {url}. Marker file created.")
                        sync_successful = True
                        break

                except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                    logging.info(f"Initial sync source {url} is not available. Trying next address...")
                except Exception as e:
                    logging.error(f"An unexpected error occurred during initial sync from {url}: {e}. Trying next address...")
            
            if sync_successful:
                break
                
            logging.info("All source addresses failed. Retrying in 30 seconds...")
            await asyncio.sleep(30)

    def run(self):
        os.makedirs(self.sync_root_dir, exist_ok=True)
        logging.basicConfig(level=self.project_config.get("log_level", "INFO"),
                            format=f'%(asctime)s - {self.node_id} - %(levelname)s - %(message)s')

        event_handler = FileMonitor(self)
        observer = Observer()
        observer.schedule(event_handler, self.sync_root_dir, recursive=True)
        observer.start()

        async def main():
            await self.initial_sync()
            start_server = websockets.serve(self.handle_client, "0.0.0.0", self.port)
            await asyncio.gather(
                start_server,
                self.connect_to_peers()
            )

        asyncio.run(main())

# -----------------------------------------------------------------------------
#  ProjectManager - Менеджер проектов
# -----------------------------------------------------------------------------

class ProjectManager:
    def __init__(self, config_path, current_node_id):
        with open(config_path, "r") as f:
            self.config = json.load(f)
        self.current_node_id = current_node_id
        self.current_node_config = self._get_current_node_config()
        self.workers: list[SyncWorker] = []

    def _get_current_node_config(self):
        node = next((n for n in self.config["nodes"] if n["id"] == self.current_node_id), None)
        if not node:
            raise ValueError(f"Node with id '{self.current_node_id}' not found in config")
        return node

    def start(self):
        for project_config in self.config["projects"]:
            worker = SyncWorker(project_config, self.current_node_config, self.config["nodes"])
            self.workers.append(worker)
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.terminate()
            worker.join()

# -----------------------------------------------------------------------------
#  Точка входа
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    config_path = os.environ.get("DEVSYNC_PROJECTS_CONFIG_PATH", "projects_config.json")
    current_node_id = os.environ.get("DEVSYNC_CURRENT_NODE_ID")

    if not current_node_id:
        print("Error: DEVSYNC_CURRENT_NODE_ID environment variable not set.")
        exit(1)

    manager = ProjectManager(config_path, current_node_id)
    manager.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()

