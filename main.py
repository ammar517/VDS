"""
================================================================================
 KEX SISTEM V2.0 PRO -- RAILWAY VDS CONTROL CENTER
================================================================================
Tek dosya, tek kullanicili (owner-only) Telegram tabanli VDS/proje yonetim botu.
Gelistirici : @BY_SADRAZAM
Motor       : v2.0.0-PRO

Bu dosyayi VDS'ye / Railway'e atacaksin. Baska hicbir python dosyasina
ihtiyac yok (requirements.txt ve Procfile ayri dosyalar olarak gelir).
Calistirma : python kex_bot.py
================================================================================
"""

import os
import sys
import subprocess

# ════════════════════════════════════════════════════════════════════════════
# 0) OTOMATIK BAGIMLILIK KURULUMU -- tek dosya, requirements.txt gerekmez.
#    Railway ilk acilista bu paketleri kendisi kurar.
# ════════════════════════════════════════════════════════════════════════════

def _ensure_package(import_name: str, pip_name: str = None):
    try:
        __import__(import_name)
    except ImportError:
        pip_name = pip_name or import_name
        print(f"📦 Eksik paket tespit edildi, kuruluyor: {pip_name} ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", pip_name])
        print(f"✅ Kuruldu: {pip_name}")


_ensure_package("telebot", "pyTelegramBotAPI")
_ensure_package("psutil", "psutil")

import ast
import time
import json
import shutil
import signal
import sqlite3
import logging
import zipfile
import platform
import tempfile
import functools
import threading
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

import psutil
import telebot
from telebot import types


# ════════════════════════════════════════════════════════════════════════════
# 1) AYARLAR (Railway'de bunlari Environment Variables olarak gir)
# ════════════════════════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "8633944596:AAGZcViGFrZ3IJPN2gBWkDDf0QlXD3ES8ag")
OWNER_ID = int(os.getenv("OWNER_ID", "8474072848"))
DEVELOPER_TAG = os.getenv("DEVELOPER_TAG", "@BY_SADRAZAM")

KEX_VERSION = "2.0.0-PRO"
BRAND_NAME = "⚡ KEX SİSTEM"
BRAND_SUBTITLE = "RAILWAY VDS CONTROL CENTER"

# --- Uyelik planlari -- kapasite = bir kullanicinin ayni anda barindirabilecegi
#     bot/proje sayisi. Planlar admin tarafindan /planver komutuyla elle atanir
#     (odemeler DM uzerinden alinip burada islenir, otomatik odeme entegrasyonu yok).
PLANS = {
    "free":    {"label": "🆓 Ücretsiz Plan",  "capacity": 1},
    "statu":   {"label": "⭐ Statü Plan",      "capacity": 3},
    "premium": {"label": "💎 Premium Üyelik", "capacity": 5},
}
DEFAULT_PLAN = "free"

# --- Yollar (Railway'de kalici disk/volume kullanmazsan bu klasorler her
#     yeniden deploy'da sifirlanir; kalici saklamak icin bir Volume baglamalisin) ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = DATA_DIR / "logs"
BACKUPS_DIR = DATA_DIR / "backups"
PROJECTS_DIR = BASE_DIR / "projects"
TEMP_DIR = BASE_DIR / "temp"
DB_PATH = DATA_DIR / "kex.db"

for _dir in (DATA_DIR, LOGS_DIR, BACKUPS_DIR, PROJECTS_DIR, TEMP_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# --- Limitler ---
MAX_UPLOAD_MB = 19          # Telegram Bot API dosya indirme siniri ~20MB
MAX_ZIP_TOTAL_MB = 60       # ZIP acilinca toplam boyut siniri
MAX_ZIP_FILES = 600         # ZIP icindeki maksimum dosya sayisi
LOG_TAIL_LINES = 30
LOG_MAX_FILE_SIZE_MB = 10
PROJECT_LIST_PAGE_SIZE = 6

ALLOWED_UPLOAD_EXT = {".py", ".zip", ".js"}

# ════════════════════════════════════════════════════════════════════════════
# 2) LOGGER (Rotating -- 10MB x 5 yedek)
# ════════════════════════════════════════════════════════════════════════════

kex_logger = logging.getLogger("kex")
kex_logger.setLevel(logging.INFO)

_file_handler = RotatingFileHandler(
    LOGS_DIR / "kex.log",
    maxBytes=LOG_MAX_FILE_SIZE_MB * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_console_handler = logging.StreamHandler()
_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_file_handler.setFormatter(_fmt)
_console_handler.setFormatter(_fmt)
kex_logger.addHandler(_file_handler)
kex_logger.addHandler(_console_handler)


# ════════════════════════════════════════════════════════════════════════════
# 3) VERITABANI (SQLite -- thread-safe, her cagrida ayri baglanti)
# ════════════════════════════════════════════════════════════════════════════

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id            TEXT PRIMARY KEY,
    owner_id      INTEGER NOT NULL DEFAULT 0,
    name          TEXT NOT NULL,
    project_type  TEXT NOT NULL,
    main_file     TEXT NOT NULL,
    source_path   TEXT NOT NULL,
    venv_path     TEXT,
    status        TEXT NOT NULL DEFAULT 'stopped',
    pid           INTEGER,
    auto_start    INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    TEXT,
    event_type    TEXT NOT NULL,
    message       TEXT,
    created_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER PRIMARY KEY,
    username      TEXT,
    plan          TEXT NOT NULL DEFAULT 'free',
    capacity      INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""

_db_lock = threading.Lock()


@contextmanager
def get_cursor(commit: bool = False):
    """Her cagrida yeni, thread-safe SQLite baglantisi acar."""
    conn = sqlite3.connect(str(DB_PATH), timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        with _db_lock:
            cur = conn.cursor()
            try:
                yield cur
                if commit:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
    finally:
        conn.close()


def init_db():
    try:
        with get_cursor(commit=True) as cur:
            cur.executescript(_DB_SCHEMA)
            # Eski (owner_id'siz) bir veritabaniyla uyumluluk icin -- sutun yoksa ekle.
            try:
                cur.execute("ALTER TABLE projects ADD COLUMN owner_id INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # sutun zaten var
        kex_logger.info("Veritabani hazir: %s", DB_PATH)
    except Exception as e:
        kex_logger.error("Veritabani baslatma hatasi: %s", e, exc_info=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db_insert_project(pid_row: dict):
    with get_cursor(commit=True) as cur:
        cur.execute(
            """INSERT INTO projects
               (id, owner_id, name, project_type, main_file, source_path, venv_path,
                status, pid, auto_start, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                pid_row["id"], pid_row["owner_id"], pid_row["name"], pid_row["project_type"],
                pid_row["main_file"], pid_row["source_path"], pid_row.get("venv_path"),
                pid_row.get("status", "stopped"), pid_row.get("pid"),
                pid_row.get("auto_start", 0), now_iso(), now_iso(),
            ),
        )


def db_update_project(project_id: str, **fields):
    if not fields:
        return
    fields["updated_at"] = now_iso()
    cols = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [project_id]
    with get_cursor(commit=True) as cur:
        cur.execute(f"UPDATE projects SET {cols} WHERE id = ?", vals)


def db_delete_project(project_id: str):
    with get_cursor(commit=True) as cur:
        cur.execute("DELETE FROM projects WHERE id = ?", (project_id,))


def db_get_project(project_id: str):
    with get_cursor() as cur:
        cur.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_projects():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM projects ORDER BY created_at DESC")
        return [dict(r) for r in cur.fetchall()]


def list_user_projects(owner_id: int):
    with get_cursor() as cur:
        cur.execute("SELECT * FROM projects WHERE owner_id = ? ORDER BY created_at DESC", (owner_id,))
        return [dict(r) for r in cur.fetchall()]


def db_count_user_projects(owner_id: int) -> int:
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM projects WHERE owner_id = ?", (owner_id,))
        return cur.fetchone()["c"]


def count_running_projects() -> int:
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM projects WHERE status = 'running'")
        return cur.fetchone()["c"]


# ---------- Kullanicilar / planlar ----------

def db_get_or_create_user(user_id: int, username: str = None) -> dict:
    with get_cursor(commit=True) as cur:
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if row:
            if username:
                cur.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
            return dict(row)
        capacity = PLANS[DEFAULT_PLAN]["capacity"]
        cur.execute(
            "INSERT INTO users (user_id, username, plan, capacity, created_at, updated_at) VALUES (?,?,?,?,?,?)",
            (user_id, username, DEFAULT_PLAN, capacity, now_iso(), now_iso()),
        )
        return {"user_id": user_id, "username": username, "plan": DEFAULT_PLAN, "capacity": capacity}


def db_set_user_plan(user_id: int, plan: str):
    if plan not in PLANS:
        return False
    capacity = PLANS[plan]["capacity"]
    with get_cursor(commit=True) as cur:
        cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        exists = cur.fetchone()
        if exists:
            cur.execute("UPDATE users SET plan = ?, capacity = ?, updated_at = ? WHERE user_id = ?",
                        (plan, capacity, now_iso(), user_id))
        else:
            cur.execute(
                "INSERT INTO users (user_id, username, plan, capacity, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                (user_id, None, plan, capacity, now_iso(), now_iso()),
            )
    return True


def list_all_users():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM users ORDER BY created_at DESC")
        return [dict(r) for r in cur.fetchall()]


def log_event(event_type: str, message: str, project_id: str = None):
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                "INSERT INTO events (project_id, event_type, message, created_at) VALUES (?,?,?,?)",
                (project_id, event_type, message, now_iso()),
            )
    except Exception as e:
        kex_logger.error("Event log hatasi: %s", e)


# ════════════════════════════════════════════════════════════════════════════
# 4) YETKI KONTROLU
#    - owner_only  -> sadece admin (OWNER_ID): sunucu geneli / yonetim ekranlari
#    - authorized   -> herhangi bir Telegram kullanicisi: ilk mesajda otomatik
#                      "free" planla kaydedilir, kendi botlarini yonetebilir
# ════════════════════════════════════════════════════════════════════════════

def owner_only(handler):
    @functools.wraps(handler)
    def wrapper(update, *args, **kwargs):
        user = getattr(update, "from_user", None)
        user_id = getattr(user, "id", None)
        if user_id != OWNER_ID:
            kex_logger.warning("Yonetici komutuna yetkisiz erisim denemesi -> user_id=%s zaman=%s", user_id, now_iso())
            try:
                chat_id = update.chat.id if hasattr(update, "chat") else update.message.chat.id
                bot.send_message(chat_id, "⛔ Bu işlem sadece yöneticiye özeldir.")
            except Exception:
                pass
            return None
        return handler(update, *args, **kwargs)
    return wrapper


def authorized(handler):
    """Her kullaniciya acik; ilk giriste otomatik olarak 'free' planla kaydeder."""
    @functools.wraps(handler)
    def wrapper(update, *args, **kwargs):
        user = getattr(update, "from_user", None)
        if user is None or getattr(user, "id", None) is None:
            return None
        db_get_or_create_user(user.id, getattr(user, "username", None))
        return handler(update, *args, **kwargs)
    return wrapper


def can_manage_project(project: dict, user_id: int) -> bool:
    """Bir kullanicinin belirli bir projeyi yonetip yonetemeyecegini kontrol eder
    (proje sahibi ya da admin)."""
    return bool(project) and (project.get("owner_id") == user_id or user_id == OWNER_ID)


def deny_project_access(call):
    bot.answer_callback_query(call.id, "⛔ Bu bot/proje sana ait değil.", show_alert=True)


# ════════════════════════════════════════════════════════════════════════════
# 5) SISTEM DURUMU (CPU / RAM / DISK / UPTIME)
# ════════════════════════════════════════════════════════════════════════════

_BOOT_TIME = time.time()

# psutil.cpu_percent, ilk cagrida referans olusturmak icin "primed" edilir; sonraki
# cagrilar interval=None ile ANINDA (blocking olmadan) sonuc doner. interval=0.3 ile
# blocking kullanmak bazi kisitli container'larda (Railway gibi) yanit vermeyip
# "N/A" gorunmesine sebep olabiliyordu.
try:
    psutil.cpu_percent(interval=None)
except Exception:
    pass


def _safe_metric(func, default="N/A"):
    try:
        return func()
    except Exception:
        return default


def fmt_uptime(seconds: int) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, _ = divmod(rem, 60)
    d, h = divmod(h, 24)
    if d:
        return f"{d}g {h}s {m}dk"
    return f"{h}s {m}dk"


def progress_bar(percent, length: int = 10) -> str:
    try:
        pct = max(0, min(100, float(percent)))
    except Exception:
        return "▱" * length
    filled = round(length * pct / 100)
    return "▰" * filled + "▱" * (length - filled)


def _fmt_gb(num_bytes) -> str:
    try:
        return f"{num_bytes / (1024 ** 3):.1f} GB"
    except Exception:
        return "N/A"


def _cpu_model_name() -> str:
    """CPU model adini platforma gore en iyi caba ile tespit eder."""
    try:
        if platform.system() == "Linux":
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.lower().startswith("model name"):
                        return line.split(":", 1)[1].strip()
        proc = platform.processor()
        if proc:
            return proc
    except Exception:
        pass
    return platform.machine() or "N/A"


def get_system_status_text() -> str:
    cpu_val = _safe_metric(lambda: psutil.cpu_percent(interval=None))
    vmem = _safe_metric(lambda: psutil.virtual_memory(), None)
    dusage = _safe_metric(lambda: psutil.disk_usage(str(BASE_DIR)), None)
    cpu_cores = _safe_metric(lambda: psutil.cpu_count(logical=True), "N/A")
    cpu_physical = _safe_metric(lambda: psutil.cpu_count(logical=False), "N/A")
    hostname = _safe_metric(lambda: platform.node(), "vds-sunucu")
    os_info = _safe_metric(lambda: f"{platform.system()} {platform.release()}", "N/A")
    sys_uptime = _safe_metric(lambda: fmt_uptime(time.time() - psutil.boot_time()), "N/A")

    ram_val = vmem.percent if vmem else "N/A"
    disk_val = dusage.percent if dusage else "N/A"

    cpu = f"{cpu_val:.0f}%" if isinstance(cpu_val, (int, float)) else cpu_val
    ram = f"{ram_val:.0f}%" if isinstance(ram_val, (int, float)) else ram_val
    disk = f"{disk_val:.0f}%" if isinstance(disk_val, (int, float)) else disk_val
    bot_uptime = fmt_uptime(time.time() - _BOOT_TIME)

    ram_detail = f"{_fmt_gb(vmem.used)} / {_fmt_gb(vmem.total)}" if vmem else "N/A"
    disk_detail = f"{_fmt_gb(dusage.used)} / {_fmt_gb(dusage.total)}" if dusage else "N/A"

    projects = list_projects()
    running = count_running_projects()

    cpu_bar = progress_bar(cpu_val if isinstance(cpu_val, (int, float)) else 0)
    ram_bar = progress_bar(ram_val if isinstance(ram_val, (int, float)) else 0)
    disk_bar = progress_bar(disk_val if isinstance(disk_val, (int, float)) else 0)

    return (
        f"╔═══════════════════════╗\n"
        f"   {BRAND_NAME}\n"
        f"   🖥️ {BRAND_SUBTITLE}\n"
        f"╚═══════════════════════╝\n\n"
        f"🟢 <b>Durum:</b> Sunucu çalışıyor\n"
        f"🏷️ <b>Sunucu Adı:</b> <code>{hostname}</code>\n"
        f"💽 <b>İşletim Sistemi:</b> {os_info}\n"
        f"🤖 <b>KEX Engine:</b> v{KEX_VERSION}   🐍 <b>Python:</b> {platform.python_version()}\n\n"
        f"📊 <b>KAYNAK KULLANIMI</b>\n"
        f"┌ 💻 CPU   {cpu_bar}  {cpu}\n"
        f"│    └ {cpu_physical} çekirdek / {cpu_cores} iş parçacığı\n"
        f"┌ 🧠 RAM   {ram_bar}  {ram}\n"
        f"│    └ {ram_detail} kullanılıyor\n"
        f"└ 💾 DİSK  {disk_bar}  {disk}\n"
        f"     └ {disk_detail} kullanılıyor\n\n"
        f"📦 <b>Projeler:</b> {len(projects)}   🚀 <b>Çalışan:</b> {running}\n"
        f"⏱️ <b>Sunucu Uptime:</b> {sys_uptime}\n"
        f"⚡ <b>Panel Uptime:</b> {bot_uptime}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👨‍💻 Geliştirici: {DEVELOPER_TAG}"
    )


def get_main_menu_keyboard(is_admin: bool = False) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    if is_admin:
        kb.add(
            types.InlineKeyboardButton("🖥️ SUNUCU", callback_data="menu_server"),
            types.InlineKeyboardButton("🤖 PROJELERİM", callback_data="projects_page:0"),
        )
        kb.add(
            types.InlineKeyboardButton("📥 YÜKLE", callback_data="menu_upload_info"),
            types.InlineKeyboardButton("💾 YEDEKLER", callback_data="menu_backups"),
        )
        kb.add(
            types.InlineKeyboardButton("📜 SİSTEM LOGLARI", callback_data="menu_syslogs"),
            types.InlineKeyboardButton("👥 KULLANICILAR", callback_data="menu_users"),
        )
        kb.add(
            types.InlineKeyboardButton("⚙️ AYARLAR", callback_data="menu_settings"),
            types.InlineKeyboardButton("🔄 YENİLE", callback_data="menu_refresh"),
        )
        kb.add(types.InlineKeyboardButton("❓ YARDIM", callback_data="menu_help"))
    else:
        kb.add(
            types.InlineKeyboardButton("🤖 BOTLARIM", callback_data="projects_page:0"),
            types.InlineKeyboardButton("📥 YÜKLE", callback_data="menu_upload_info"),
        )
        kb.add(
            types.InlineKeyboardButton("🎫 PLANIM", callback_data="menu_plan"),
            types.InlineKeyboardButton("💾 YEDEKLER", callback_data="menu_backups"),
        )
        kb.add(
            types.InlineKeyboardButton("❓ YARDIM", callback_data="menu_help"),
            types.InlineKeyboardButton("🔄 YENİLE", callback_data="menu_refresh"),
        )
    kb.add(types.InlineKeyboardButton("👨‍💻 GELİŞTİRİCİYE ULAŞ", url=f"https://t.me/{DEVELOPER_TAG.lstrip('@')}"))
    kb.add(types.InlineKeyboardButton(f"⚡ KEX SİSTEM v{KEX_VERSION} — PANEL", callback_data="menu_about"))
    return kb


def get_user_panel_text(user_id: int, user_row: dict) -> str:
    plan_key = user_row.get("plan", DEFAULT_PLAN)
    plan = PLANS.get(plan_key, PLANS[DEFAULT_PLAN])
    used = db_count_user_projects(user_id)
    cap = plan["capacity"]
    bar = progress_bar((used / cap) * 100 if cap else 0)
    return (
        f"╔═══════════════════════╗\n"
        f"   {BRAND_NAME}\n"
        f"╚═══════════════════════╝\n\n"
        f"🎫 <b>Planın:</b> {plan['label']}\n"
        f"📦 <b>Bot kapasiten:</b> {bar}  {used}/{cap}\n\n"
        + ("Kapasiten dolu — yeni bot eklemeden önce planını yükseltmen gerekir.\n\n"
           if used >= cap else
           "📥 YÜKLE'ye dokunarak yeni bir bot ekleyebilirsin.\n\n")
        + f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👨‍💻 Plan yükseltme / destek: {DEVELOPER_TAG}"
    )


def get_plan_info_text(user_row: dict) -> str:
    plan_key = user_row.get("plan", DEFAULT_PLAN)
    lines = ["🎫 <b>PLANLAR</b>", "━━━━━━━━━━━━━━━━━━━━━━━"]
    for key, info in PLANS.items():
        mark = "👉 " if key == plan_key else "   "
        lines.append(f"{mark}{info['label']} — {info['capacity']} bot kapasitesi")
    lines.append("")
    lines.append(f"Şu anki planın: <b>{PLANS.get(plan_key, PLANS[DEFAULT_PLAN])['label']}</b>")
    lines.append("")
    lines.append(f"Yükseltmek için {DEVELOPER_TAG} ile iletişime geç. Kendi Telegram "
                  "ID'ni öğrenmek için /id komutunu kullanabilirsin.")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# 6) BAGIMLILIK ANALIZORU (AST tabanli import -> PyPI eslesmesi)
# ════════════════════════════════════════════════════════════════════════════

PACKAGE_MAP = {
    "telebot": "pyTelegramBotAPI",
    "cv2": "opencv-python",
    "PIL": "Pillow",
    "yaml": "PyYAML",
    "dotenv": "python-dotenv",
    "bs4": "beautifulsoup4",
    "sklearn": "scikit-learn",
    "fitz": "PyMuPDF",
    "Crypto": "pycryptodome",
    "jwt": "PyJWT",
    "OpenSSL": "pyOpenSSL",
    "google": "google-api-python-client",
    "discord": "discord.py",
    "aiogram": "aiogram",
    "redis": "redis",
    "psycopg2": "psycopg2-binary",
    "serial": "pyserial",
    "docx": "python-docx",
    "pptx": "python-pptx",
    "win32api": "pywin32",
}

try:
    STDLIB_MODULES = set(sys.stdlib_module_names)  # Python 3.10+
except AttributeError:
    STDLIB_MODULES = {
        "os", "sys", "math", "time", "sqlite3", "logging", "threading", "subprocess",
        "zipfile", "datetime", "pathlib", "asyncio", "json", "re", "random", "collections",
        "itertools", "functools", "typing", "contextlib", "traceback", "shutil", "tempfile",
        "signal", "socket", "http", "urllib", "hashlib", "hmac", "base64", "uuid", "csv",
        "argparse", "enum", "abc", "copy", "io", "string", "struct", "queue", "multiprocessing",
        "platform", "getpass", "glob", "pickle", "sched", "warnings", "weakref", "array",
    }


def extract_imports_from_file(file_path: Path) -> set:
    """Regex kullanmadan, ast modulu ile bir .py dosyasindaki top-level importlari cikarir."""
    modules = set()
    try:
        source = file_path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(source, filename=str(file_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level and node.level > 0:
                    continue  # goreceli (relative) import -- lokal dosya, atla
                if node.module:
                    modules.add(node.module.split(".")[0])
    except Exception as e:
        kex_logger.warning("AST analiz hatasi (%s): %s", file_path.name, e)
    return modules


def analyze_project_dependencies(project_dir: Path) -> list:
    """Proje klasorundeki tum .py dosyalarini tarar, dis bagimliliklari PyPI adlarina cevirir."""
    local_module_names = set()
    py_files = list(project_dir.rglob("*.py"))
    for f in py_files:
        local_module_names.add(f.stem)
        if f.parent != project_dir:
            local_module_names.add(f.parent.name)

    all_imports = set()
    for f in py_files:
        all_imports |= extract_imports_from_file(f)

    external = all_imports - STDLIB_MODULES - local_module_names
    resolved = sorted({PACKAGE_MAP.get(m, m) for m in external})
    return resolved


def find_requirements_file(project_dir: Path):
    for name in ("requirements.txt", "pyproject.toml", "Pipfile"):
        p = project_dir / name
        if p.exists():
            return p
    return None


# ════════════════════════════════════════════════════════════════════════════
# 7) ENVIRONMENT / PROCESS MANAGER
# ════════════════════════════════════════════════════════════════════════════

running_procs = {}  # project_id -> subprocess.Popen


def venv_python_path(venv_dir: Path) -> Path:
    if platform.system() == "Windows":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def create_venv(project_dir: Path) -> Path:
    venv_dir = project_dir / "venv"
    if not venv_python_path(venv_dir).exists():
        result = subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            capture_output=True, text=True, timeout=180,
        )
        if result.returncode != 0:
            raise RuntimeError(f"venv olusturulamadi: {result.stderr[:300]}")
    return venv_dir


def install_requirements(project_dir: Path, venv_dir: Path, packages: list, progress_cb=None) -> tuple:
    """requirements.txt varsa onu, yoksa AST'den cikan paket listesini kurar."""
    py_exe = venv_python_path(venv_dir)
    subprocess.run([str(py_exe), "-m", "pip", "install", "-U", "pip"],
                    capture_output=True, text=True, timeout=180)

    req_file = find_requirements_file(project_dir)
    if req_file and req_file.name == "requirements.txt":
        if progress_cb:
            progress_cb("📄 requirements.txt bulundu, kuruluyor...")
        cmd = [str(py_exe), "-m", "pip", "install", "-r", str(req_file)]
    elif packages:
        if progress_cb:
            progress_cb(f"🧩 {len(packages)} paket AST analiziyle tespit edildi, kuruluyor...")
        cmd = [str(py_exe), "-m", "pip", "install", *packages]
    else:
        return True, "Kurulacak harici bağımlılık bulunamadı."

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    ok = result.returncode == 0
    output = (result.stdout[-1500:] + "\n" + result.stderr[-1500:]) if not ok else "OK"
    return ok, output


def start_process(project: dict) -> tuple:
    project_id = project["id"]
    if project_id in running_procs and running_procs[project_id].poll() is None:
        return False, "Zaten çalışıyor."

    source_path = Path(project["source_path"])
    main_file = source_path / project["main_file"]
    if not main_file.exists():
        return False, f"Ana dosya bulunamadı: {project['main_file']}"

    if project["project_type"] == "py":
        venv_dir = Path(project["venv_path"]) if project["venv_path"] else source_path / "venv"
        exe = str(venv_python_path(venv_dir))
        if not Path(exe).exists():
            exe = sys.executable
        cmd = [exe, str(main_file)]
    else:  # js
        node_exe = shutil.which("node") or "node"
        cmd = [node_exe, str(main_file)]

    log_path = source_path / "output.log"
    try:
        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"\n===== BAŞLATILDI: {now_iso()} =====\n")
            log_file.flush()
            env = os.environ.copy()
            proc = subprocess.Popen(
                cmd, cwd=str(source_path), stdout=log_file, stderr=subprocess.STDOUT,
                env=env, start_new_session=True,
            )
        running_procs[project_id] = proc
        db_update_project(project_id, status="running", pid=proc.pid)
        log_event("process_start", f"PID={proc.pid}", project_id)
        return True, f"Başlatıldı (PID: {proc.pid})"
    except Exception as e:
        kex_logger.error("Baslatma hatasi (%s): %s", project_id, e, exc_info=True)
        return False, f"Başlatma hatası: {e}"


def stop_process(project_id: str) -> tuple:
    proc = running_procs.get(project_id)
    pid = None
    if proc is None:
        project = db_get_project(project_id)
        pid = project.get("pid") if project else None
    else:
        pid = proc.pid

    if not pid:
        db_update_project(project_id, status="stopped", pid=None)
        return False, "Çalışan süreç bulunamadı."

    try:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        else:
            if psutil.pid_exists(pid):
                p = psutil.Process(pid)
                p.terminate()
                try:
                    p.wait(timeout=5)
                except psutil.TimeoutExpired:
                    p.kill()
        running_procs.pop(project_id, None)
        db_update_project(project_id, status="stopped", pid=None)
        log_event("process_stop", f"PID={pid}", project_id)
        return True, "Durduruldu."
    except Exception as e:
        kex_logger.error("Durdurma hatasi (%s): %s", project_id, e, exc_info=True)
        running_procs.pop(project_id, None)
        db_update_project(project_id, status="stopped", pid=None)
        return False, f"Durdurma hatası: {e}"


def is_process_alive(project: dict) -> bool:
    proc = running_procs.get(project["id"])
    if proc is not None:
        return proc.poll() is None
    pid = project.get("pid")
    if pid and psutil.pid_exists(pid):
        return True
    return False


def reconcile_statuses_on_boot():
    """VDS/Railway yeniden baslarsa eski PID'ler artik gecerli olmadigi icin
    DB'deki 'running' kayitlarini gercek duruma gore 'stopped' yapar."""
    for project in list_projects():
        if project["status"] == "running" and not is_process_alive(project):
            db_update_project(project["id"], status="stopped", pid=None)
            kex_logger.info("Onyukleme: %s 'stopped' olarak isaretlendi (surec bulunamadi)", project["name"])


def tail_log(project_dir: Path, n: int = LOG_TAIL_LINES) -> str:
    log_path = project_dir / "output.log"
    if not log_path.exists():
        return "(henüz log yok)"
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        tail = "".join(lines[-n:]).strip()
        return tail if tail else "(log boş)"
    except Exception as e:
        return f"Log okunamadı: {e}"


def get_process_resource_usage(pid: int):
    """Calisan bir projenin anlik CPU/RAM kullanimini dondurur."""
    try:
        if not pid or not psutil.pid_exists(pid):
            return None
        p = psutil.Process(pid)
        return {
            "cpu": p.cpu_percent(interval=0.1),
            "ram_mb": p.memory_info().rss / (1024 * 1024),
        }
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
# 7B) YEDEKLEME (BACKUP) YONETICISI
# ════════════════════════════════════════════════════════════════════════════

def create_project_backup(project: dict) -> tuple:
    """Projenin kaynak kodunu (venv haric) zip olarak backups/ klasorune yedekler."""
    try:
        source_path = Path(project["source_path"])
        if not source_path.exists():
            return False, "Proje klasörü bulunamadı."

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(c for c in project["name"] if c.isalnum() or c in ("_", "-", ".")) or project["id"]
        backup_name = f"{project['id']}__{safe_name[:30]}__{ts}.zip"
        backup_path = BACKUPS_DIR / backup_name

        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(source_path):
                dirs[:] = [d for d in dirs if d != "venv"]
                for f in files:
                    full = Path(root) / f
                    rel = full.relative_to(source_path)
                    zf.write(full, arcname=str(rel))

        log_event("backup_create", backup_name, project["id"])
        size_mb = backup_path.stat().st_size / (1024 * 1024)
        return True, f"{backup_name} ({size_mb:.1f}MB)"
    except Exception as e:
        kex_logger.error("Yedekleme hatasi: %s", e, exc_info=True)
        return False, str(e)


def list_backup_files() -> list:
    if not BACKUPS_DIR.exists():
        return []
    files = sorted(BACKUPS_DIR.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def delete_backup_file(filename: str) -> bool:
    try:
        target = (BACKUPS_DIR / filename).resolve()
        if not str(target).startswith(str(BACKUPS_DIR.resolve())):
            return False
        if target.exists():
            target.unlink()
            return True
        return False
    except Exception as e:
        kex_logger.error("Yedek silme hatasi: %s", e)
        return False


# ════════════════════════════════════════════════════════════════════════════
# 8) GUVENLI ZIP CIKARMA (path traversal / boyut / dosya sayisi korumasi)
# ════════════════════════════════════════════════════════════════════════════

def safe_extract_zip(zip_path: Path, dest_dir: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        infos = zf.infolist()
        if len(infos) > MAX_ZIP_FILES:
            raise ValueError(f"ZIP çok fazla dosya içeriyor ({len(infos)} > {MAX_ZIP_FILES}).")

        total_size = sum(i.file_size for i in infos)
        if total_size > MAX_ZIP_TOTAL_MB * 1024 * 1024:
            raise ValueError(f"ZIP açılınca çok büyük olacak ({total_size // (1024*1024)}MB).")

        dest_resolved = dest_dir.resolve()
        for info in infos:
            name = info.filename
            if name.startswith("/") or ".." in Path(name).parts or (len(name) > 1 and name[1] == ":"):
                raise ValueError(f"Güvensiz yol tespit edildi: {name}")
            target = (dest_dir / name).resolve()
            if not str(target).startswith(str(dest_resolved)):
                raise ValueError(f"Path traversal denemesi tespit edildi: {name}")

        zf.extractall(dest_dir)


def detect_main_file(project_dir: Path) -> tuple:
    """(main_file_relatif_ad, tip) dondurur. tip: 'py' veya 'js'."""
    entries = {p.name for p in project_dir.iterdir() if p.is_file()}

    for candidate in ("main.py", "bot.py", "app.py", "index.py"):
        if candidate in entries:
            return candidate, "py"
    for candidate in ("index.js", "main.js", "bot.js", "app.js"):
        if candidate in entries:
            return candidate, "js"

    all_py = list(project_dir.rglob("*.py"))
    if all_py:
        rel_path = all_py[0].relative_to(project_dir)
        return str(rel_path), "py"

    all_js = list(project_dir.rglob("*.js"))
    if all_js:
        rel_path = all_js[0].relative_to(project_dir)
        return str(rel_path), "js"

    return None, None


# ════════════════════════════════════════════════════════════════════════════
# 9) TELEGRAM BOT
# ════════════════════════════════════════════════════════════════════════════

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
_upload_locks = {}  # basit anti-spam: ayni anda tek yukleme islenir


def new_project_id() -> str:
    return f"proj_{int(time.time())}"


def project_button_label(p: dict) -> str:
    icon = "🟢" if p["status"] == "running" else "🔴"
    return f"{p['name'][:22]} ({p['project_type'].upper()}) {icon}"


def build_projects_keyboard(page: int = 0, owner_id: int = None) -> types.InlineKeyboardMarkup:
    """owner_id verilmezse (admin) TUM projeler; verilirse sadece o kullanicinin
    kendi botlari listelenir."""
    projects = list_projects() if owner_id is None else list_user_projects(owner_id)
    start = page * PROJECT_LIST_PAGE_SIZE
    chunk = projects[start:start + PROJECT_LIST_PAGE_SIZE]

    kb = types.InlineKeyboardMarkup(row_width=1)
    if not projects:
        kb.add(types.InlineKeyboardButton("📦 Henüz proje yok", callback_data="noop"))
    for p in chunk:
        kb.add(types.InlineKeyboardButton(project_button_label(p), callback_data=f"proj_detail:{p['id']}"))

    nav = []
    if start > 0:
        nav.append(types.InlineKeyboardButton("⬅️ Geri", callback_data=f"projects_page:{page-1}"))
    if start + PROJECT_LIST_PAGE_SIZE < len(projects):
        nav.append(types.InlineKeyboardButton("İleri ➡️", callback_data=f"projects_page:{page+1}"))
    if nav:
        kb.add(*nav)

    kb.add(types.InlineKeyboardButton("📥 YENİ PROJE YÜKLE (.py/.zip/.js gönder)", callback_data="noop"))
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    return kb


def build_project_detail_keyboard(project: dict) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    if project["status"] == "running":
        kb.add(types.InlineKeyboardButton("⏸️ DURDUR", callback_data=f"proj_stop:{project['id']}"))
    else:
        kb.add(types.InlineKeyboardButton("🚀 BAŞLAT", callback_data=f"proj_start:{project['id']}"))
    kb.add(types.InlineKeyboardButton("🔄 YENİDEN BAŞLAT", callback_data=f"proj_restart:{project['id']}"))
    kb.add(
        types.InlineKeyboardButton("📋 LOGLAR", callback_data=f"proj_logs:{project['id']}"),
        types.InlineKeyboardButton("💾 YEDEK AL", callback_data=f"proj_backup:{project['id']}"),
    )
    kb.add(types.InlineKeyboardButton("🗑️ SİL", callback_data=f"proj_delete_ask:{project['id']}"))
    kb.add(types.InlineKeyboardButton("⬅️ Proje Listesi", callback_data="projects_page:0"))
    return kb


def project_detail_text(project: dict) -> str:
    alive = is_process_alive(project)
    status_icon = "🟢 RUNNING" if alive else "🔴 STOPPED"
    usage_line = ""
    if alive and project.get("pid"):
        usage = get_process_resource_usage(project["pid"])
        if usage:
            usage_line = f"💻 CPU: {usage['cpu']:.0f}%  |  🧠 RAM: {usage['ram_mb']:.0f}MB\n"
    return (
        f"🤖 <b>{project['name']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{project['id']}</code>\n"
        f"🎯 Tip: {project['project_type'].upper()}\n"
        f"📄 Ana dosya: <code>{project['main_file']}</code>\n"
        f"📊 Durum: {status_icon}\n"
        f"{usage_line}"
        f"🕐 Oluşturulma: {project['created_at']}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )


# ---------- /start, /menu ----------

def get_welcome_text(is_admin: bool, user_row: dict) -> str:
    if is_admin:
        return (
            f"👋 <b>Hoş geldin, Bay Sadrazam.</b>\n\n"
            f"{BRAND_NAME} — sunucunu ve üzerindeki tüm kullanıcı botlarını "
            f"tek panelden yönetiyorsun.\n"
        )
    plan_label = PLANS.get(user_row.get("plan", DEFAULT_PLAN), PLANS[DEFAULT_PLAN])["label"]
    return (
        f"👋 <b>{BRAND_NAME}'e hoş geldin!</b>\n\n"
        f"Kendi Telegram botunu (.py / .zip / .js) buraya yükleyip 7/24 "
        f"çalıştırabilirsin. Mevcut planın: <b>{plan_label}</b>\n"
    )


@bot.message_handler(commands=["start", "menu"])
@authorized
def cmd_start(message):
    is_admin = message.from_user.id == OWNER_ID
    user_row = db_get_or_create_user(message.from_user.id, message.from_user.username)
    if is_admin:
        body = get_welcome_text(True, user_row) + "\n" + get_system_status_text()
    else:
        body = get_welcome_text(False, user_row) + "\n" + get_user_panel_text(message.from_user.id, user_row)
    bot.send_message(message.chat.id, body, reply_markup=get_main_menu_keyboard(is_admin))
    log_event("bot_menu", f"{message.from_user.id} ana menuyu acti")


# ---------- Ana menu callback'leri ----------

@bot.callback_query_handler(func=lambda c: c.data == "menu_refresh")
@authorized
def cb_refresh(call):
    is_admin = call.from_user.id == OWNER_ID
    try:
        if is_admin:
            text = get_system_status_text()
        else:
            user_row = db_get_or_create_user(call.from_user.id, call.from_user.username)
            text = get_user_panel_text(call.from_user.id, user_row)
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                               reply_markup=get_main_menu_keyboard(is_admin))
    except Exception:
        pass
    bot.answer_callback_query(call.id, "🔄 Yenilendi")


@bot.callback_query_handler(func=lambda c: c.data == "menu_plan")
@authorized
def cb_plan(call):
    bot.answer_callback_query(call.id)
    user_row = db_get_or_create_user(call.from_user.id, call.from_user.username)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("👨‍💻 Yükselt (Geliştiriciyle konuş)", url=f"https://t.me/{DEVELOPER_TAG.lstrip('@')}"))
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    bot.send_message(call.message.chat.id, get_plan_info_text(user_row), reply_markup=kb)


@bot.message_handler(commands=["id"])
@authorized
def cmd_id(message):
    u = message.from_user
    uname_line = f"👤 Kullanıcı adı: @{u.username}\n" if u.username else ""
    bot.reply_to(
        message,
        "🆔 <b>Telegram Bilgilerin</b>\n"
        f"{uname_line}"
        f"🔢 ID: <code>{u.id}</code>\n\n"
        "Plan yükseltmesi için bu ID'yi geliştiriciye iletebilirsin.",
    )


@bot.message_handler(commands=["planver"])
@owner_only
def cmd_set_plan(message):
    parts = message.text.split()
    if len(parts) != 3 or parts[2] not in PLANS:
        plan_list = ", ".join(PLANS.keys())
        bot.reply_to(message, f"Kullanım: <code>/planver &lt;telegram_id&gt; &lt;plan&gt;</code>\nPlanlar: {plan_list}")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ Geçersiz Telegram ID.")
        return
    plan = parts[2]
    db_set_user_plan(target_id, plan)
    label = PLANS[plan]["label"]
    cap = PLANS[plan]["capacity"]
    bot.reply_to(message, f"✅ <code>{target_id}</code> kullanıcısının planı {label} ({cap} kapasite) olarak güncellendi.")
    try:
        bot.send_message(target_id, f"🎉 Planın güncellendi: <b>{label}</b>\nArtık {cap} bot barındırabilirsin.")
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "menu_users")
@owner_only
def cb_users(call):
    bot.answer_callback_query(call.id)
    users = list_all_users()
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    if not users:
        text = "👥 <b>KULLANICILAR</b>\n━━━━━━━━━━━━━━━━━━━━━━━\nHenüz kayıtlı kullanıcı yok."
    else:
        lines = ["👥 <b>KULLANICILAR</b>", "━━━━━━━━━━━━━━━━━━━━━━━"]
        for u in users[:30]:
            used = db_count_user_projects(u["user_id"])
            plan = PLANS.get(u["plan"], PLANS[DEFAULT_PLAN])
            uname = f"@{u['username']}" if u.get("username") else "-"
            lines.append(f"🔢 <code>{u['user_id']}</code>  {uname}\n   {plan['label']}  ({used}/{plan['capacity']})")
        lines.append("\nPlan değiştirmek için: <code>/planver &lt;id&gt; &lt;free|statu|premium&gt;</code>")
        text = "\n".join(lines)
    bot.send_message(call.message.chat.id, text, reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "menu_server")
@owner_only
def cb_server(call):
    bot.answer_callback_query(call.id)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    bot.send_message(call.message.chat.id, get_system_status_text(), reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "menu_syslogs")
@owner_only
def cb_syslogs(call):
    bot.answer_callback_query(call.id)
    log_path = LOGS_DIR / "kex.log"
    text = "(log dosyası boş)"
    if log_path.exists():
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        text = "".join(lines[-40:]).strip() or text
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    safe_text = text[-3500:]
    bot.send_message(call.message.chat.id, f"📜 <b>KEX SİSTEM LOGLARI</b>\n<code>{telebot.util.escape(safe_text)}</code>",
                      reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "menu_upload_info")
@authorized
def cb_upload_info(call):
    bot.answer_callback_query(call.id)
    user_row = db_get_or_create_user(call.from_user.id, call.from_user.username)
    plan = PLANS.get(user_row.get("plan", DEFAULT_PLAN), PLANS[DEFAULT_PLAN])
    used = db_count_user_projects(call.from_user.id)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🤖 Botlarım", callback_data="projects_page:0"))
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    bot.send_message(
        call.message.chat.id,
        "📥 <b>YENİ BOT YÜKLEME</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Bu sohbete doğrudan bir dosya gönder, KEX otomatik algılasın:\n\n"
        "•  <code>.py</code>  — tek dosyalık bot/script\n"
        "•  <code>.zip</code>  — çok dosyalı proje (requirements.txt varsa otomatik kurulur)\n"
        "•  <code>.js</code>  — Node.js scripti\n\n"
        "<b>Yükleme bitince otomatik olarak:</b>\n"
        "1️⃣  Ana dosya tespit edilir\n"
        "2️⃣  (Python ise) bağımlılıklar AST ile analiz edilir\n"
        "3️⃣  İzole <code>venv</code> kurulur, paketler yüklenir\n"
        "4️⃣  'Şimdi Başlat' butonuyla tek tıkla çalıştırabilirsin\n\n"
        f"📦 Max dosya boyutu: {MAX_UPLOAD_MB} MB   •   ZIP limiti: {MAX_ZIP_TOTAL_MB} MB\n"
        f"🎫 Planın: {plan['label']} — {used}/{plan['capacity']} bot kullanımda",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "menu_help")
@authorized
def cb_help(call):
    bot.answer_callback_query(call.id)
    is_admin = call.from_user.id == OWNER_ID
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    admin_lines = (
        "🖥️ <b>SUNUCU</b> — CPU / RAM / Disk / uptime, gerçek zamanlı\n"
        "📜 <b>SİSTEM LOGLARI</b> — botun kendi log dosyası (son 40 satır)\n"
        "⚙️ <b>AYARLAR</b> — çalışma limitleri ve bilgiler\n"
        "👥 <b>KULLANICILAR</b> — kayıtlı kullanıcılar ve planları\n"
    ) if is_admin else (
        "🎫 <b>PLANIM</b> — mevcut planın ve yükseltme bilgisi\n"
    )
    bot.send_message(
        call.message.chat.id,
        "❓ <b>YARDIM</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{admin_lines}"
        "🤖 <b>BOTLARIM / PROJELERİM</b> — botlarını listeler, tıkla → yönet\n"
        "📥 <b>YÜKLE</b> — dosya gönderip yeni bot ekleme rehberi\n"
        "💾 <b>YEDEKLER</b> — bot bazlı zip yedek alma / indirme / silme\n"
        "🔄 <b>YENİLE</b> — ana ekranı günceller\n\n"
        "📄 <b>Bir bota girince:</b>\n"
        "🚀/⏸️ Başlat-Durdur   🔄 Yeniden Başlat   📋 Loglar (son "
        f"{LOG_TAIL_LINES} satır)   💾 Yedek Al   🗑️ Sil\n\n"
        "💬 /start veya /menu — her zaman ana menüye döner\n"
        "🆔 /id — Telegram ID'ni öğren (plan yükseltme için gerekir)",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "menu_settings")
@owner_only
def cb_settings(call):
    bot.answer_callback_query(call.id)
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    bot.send_message(
        call.message.chat.id,
        "⚙️ <b>AYARLAR</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 Owner ID: <code>{OWNER_ID}</code>\n"
        f"📦 Max yükleme boyutu: {MAX_UPLOAD_MB} MB\n"
        f"🗜️ Max ZIP toplam boyutu: {MAX_ZIP_TOTAL_MB} MB\n"
        f"📁 Max ZIP dosya sayısı: {MAX_ZIP_FILES}\n"
        f"📋 Log görüntüleme: son {LOG_TAIL_LINES} satır\n"
        f"📂 Veri klasörü: <code>{DATA_DIR}</code>\n"
        f"📂 Proje klasörü: <code>{PROJECTS_DIR}</code>\n\n"
        "Bu değerleri değiştirmek için <code>kex_bot.py</code> içindeki "
        "sabitleri (üstteki AYARLAR bölümü) düzenlemen yeterli.",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "menu_about")
@authorized
def cb_about(call):
    bot.answer_callback_query(call.id, "⚡ KEX SİSTEM")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("👨‍💻 Geliştiriciye Ulaş", url=f"https://t.me/{DEVELOPER_TAG.lstrip('@')}"))
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    bot.send_message(
        call.message.chat.id,
        f"⚡ <b>KEX SİSTEM v{KEX_VERSION}</b>\n"
        f"🖥️ {BRAND_SUBTITLE}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Telegram üzerinden VDS/Railway sunucunu yöneten kişisel kontrol merkezi.\n\n"
        "✓ Gerçek zamanlı CPU / RAM / Disk izleme\n"
        "✓ AST tabanlı otomatik bağımlılık analizi\n"
        "✓ Proje başına izole venv\n"
        "✓ Path-traversal korumalı ZIP yükleme\n"
        "✓ Canlı process + log yönetimi\n"
        "✓ Yedekleme sistemi\n\n"
        f"👨‍💻 Geliştirici: {DEVELOPER_TAG}",
        reply_markup=kb,
    )


# ---------- Yedekleme (Backup) menusu ----------
# NOT: Yedek dosyalari "{project_id}__isim__tarih.zip" adiyla saklanir; hangi
# kullaniciya ait oldugunu bulmak icin project_id uzerinden proje sahibine bakilir.
# Admin haric her kullanici sadece KENDI botlarinin yedeklerini gorur.

def _backup_owner_id(backup_path: Path):
    project_id = backup_path.name.split("__", 1)[0]
    project = db_get_project(project_id)
    return project["owner_id"] if project else None


def _visible_backups(caller_id: int, is_admin: bool):
    backups = list_backup_files()
    if is_admin:
        return backups
    return [b for b in backups if _backup_owner_id(b) == caller_id]


@bot.callback_query_handler(func=lambda c: c.data == "menu_backups")
@authorized
def cb_backups_menu(call):
    bot.answer_callback_query(call.id)
    is_admin = call.from_user.id == OWNER_ID
    backups = _visible_backups(call.from_user.id, is_admin)
    kb = types.InlineKeyboardMarkup(row_width=1)
    if not backups:
        kb.add(types.InlineKeyboardButton("📦 Henüz yedek yok", callback_data="noop"))
    else:
        for idx, b in enumerate(backups[:10]):
            size_mb = b.stat().st_size / (1024 * 1024)
            label = f"💾 {b.name[:35]} ({size_mb:.1f}MB)"
            # NOT: callback_data Telegram'da 64 bayt sınırlı, bu yuzden dosya adi yerine
            # kisa bir index kullaniliyor (backup_view:0, backup_view:1 ...).
            kb.add(types.InlineKeyboardButton(label, callback_data=f"backup_view:{idx}"))
    kb.add(types.InlineKeyboardButton("🤖 Botumdan Yedek Al", callback_data="projects_page:0"))
    kb.add(types.InlineKeyboardButton("🏠 Ana Menü", callback_data="menu_refresh"))
    text = (
        "💾 <b>YEDEKLER</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
        "Yeni yedek almak için bir botuna gir → 💾 YEDEK AL.\n\n"
        f"Son {min(len(backups), 10)} yedek aşağıda listelendi:"
    )
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        bot.send_message(call.message.chat.id, text, reply_markup=kb)


def _resolve_backup_by_index(idx_str: str, caller_id: int, is_admin: bool):
    try:
        idx = int(idx_str)
    except ValueError:
        return None
    backups = _visible_backups(caller_id, is_admin)[:10]
    if 0 <= idx < len(backups):
        return backups[idx]
    return None


@bot.callback_query_handler(func=lambda c: c.data.startswith("backup_view:"))
@authorized
def cb_backup_view(call):
    idx_str = call.data.split(":", 1)[1]
    is_admin = call.from_user.id == OWNER_ID
    backup_path = _resolve_backup_by_index(idx_str, call.from_user.id, is_admin)
    bot.answer_callback_query(call.id)
    if not backup_path or not backup_path.exists():
        bot.send_message(call.message.chat.id, "❌ Yedek bulunamadı (liste değişmiş olabilir, YEDEKLER'i tekrar aç).")
        return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📤 Dosyayı Gönder", callback_data=f"backup_send:{idx_str}"))
    kb.add(types.InlineKeyboardButton("🗑️ Yedeği Sil", callback_data=f"backup_delete:{idx_str}"))
    kb.add(types.InlineKeyboardButton("⬅️ Yedekler", callback_data="menu_backups"))
    size_mb = backup_path.stat().st_size / (1024 * 1024)
    bot.send_message(call.message.chat.id, f"💾 <code>{telebot.util.escape(backup_path.name)}</code>\n📏 {size_mb:.1f}MB",
                      reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("backup_send:"))
@authorized
def cb_backup_send(call):
    idx_str = call.data.split(":", 1)[1]
    is_admin = call.from_user.id == OWNER_ID
    backup_path = _resolve_backup_by_index(idx_str, call.from_user.id, is_admin)
    bot.answer_callback_query(call.id, "📤 Gönderiliyor...")
    if not backup_path or not backup_path.exists():
        bot.send_message(call.message.chat.id, "❌ Yedek bulunamadı (liste değişmiş olabilir, YEDEKLER'i tekrar aç).")
        return
    try:
        with open(backup_path, "rb") as f:
            bot.send_document(call.message.chat.id, f, caption=f"💾 {backup_path.name}")
    except Exception as e:
        kex_logger.error("Yedek gonderme hatasi: %s", e, exc_info=True)
        bot.send_message(call.message.chat.id, f"❌ Gönderilemedi: {e}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("backup_delete:"))
@authorized
def cb_backup_delete(call):
    idx_str = call.data.split(":", 1)[1]
    is_admin = call.from_user.id == OWNER_ID
    backup_path = _resolve_backup_by_index(idx_str, call.from_user.id, is_admin)
    bot.answer_callback_query(call.id, "🗑️ Siliniyor...")
    ok = delete_backup_file(backup_path.name) if backup_path else False
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⬅️ Yedekler", callback_data="menu_backups"))
    bot.send_message(call.message.chat.id, "✅ Yedek silindi." if ok else "❌ Yedek silinemedi.", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_backup:"))
@authorized
def cb_project_backup(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not project:
        bot.answer_callback_query(call.id, "Proje bulunamadı.", show_alert=True)
        return
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id, "💾 Yedekleniyor...")
    ok, msg = create_project_backup(project)
    kb = build_project_detail_keyboard(project)
    try:
        bot.edit_message_text(project_detail_text(project) + f"\n\n{'✅ Yedek alındı: ' if ok else '❌ '}{msg}",
                               call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        bot.send_message(call.message.chat.id, f"{'✅ Yedek alındı: ' if ok else '❌ '}{msg}", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "noop")
@authorized
def cb_noop(call):
    bot.answer_callback_query(call.id)


# ---------- Proje listesi / detay ----------

@bot.callback_query_handler(func=lambda c: c.data.startswith("projects_page:"))
@authorized
def cb_projects_page(call):
    page = int(call.data.split(":")[1])
    is_admin = call.from_user.id == OWNER_ID
    owner_filter = None if is_admin else call.from_user.id
    bot.answer_callback_query(call.id)
    title = "🤖 <b>PROJELERİM</b>" if is_admin else "🤖 <b>BOTLARIM</b>"
    text = f"{title}\n━━━━━━━━━━━━━━━━━━\nBir bota tıkla veya .py / .zip / .js dosyası gönderip yeni bot yükle."
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                               reply_markup=build_projects_keyboard(page, owner_filter))
    except Exception:
        bot.send_message(call.message.chat.id, text, reply_markup=build_projects_keyboard(page, owner_filter))


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_detail:"))
@authorized
def cb_project_detail(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    bot.answer_callback_query(call.id)
    if not project:
        bot.send_message(call.message.chat.id, "❌ Proje bulunamadı (silinmiş olabilir).")
        return
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    try:
        bot.edit_message_text(project_detail_text(project), call.message.chat.id, call.message.message_id,
                               reply_markup=build_project_detail_keyboard(project))
    except Exception:
        bot.send_message(call.message.chat.id, project_detail_text(project),
                          reply_markup=build_project_detail_keyboard(project))


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_start:"))
@authorized
def cb_project_start(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not project:
        bot.answer_callback_query(call.id, "Proje bulunamadı.", show_alert=True)
        return
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id, "🚀 Başlatılıyor...")
    ok, msg = start_process(project)
    project = db_get_project(project_id)
    try:
        bot.edit_message_text(project_detail_text(project) + f"\n\n{'✅' if ok else '❌'} {msg}",
                               call.message.chat.id, call.message.message_id,
                               reply_markup=build_project_detail_keyboard(project))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_stop:"))
@authorized
def cb_project_stop(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id, "⏸️ Durduruluyor...")
    ok, msg = stop_process(project_id)
    project = db_get_project(project_id)
    try:
        bot.edit_message_text(project_detail_text(project) + f"\n\n{'✅' if ok else '❌'} {msg}",
                               call.message.chat.id, call.message.message_id,
                               reply_markup=build_project_detail_keyboard(project))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_restart:"))
@authorized
def cb_project_restart(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id, "🔄 Yeniden başlatılıyor...")
    stop_process(project_id)
    time.sleep(1)
    project = db_get_project(project_id)
    ok, msg = start_process(project) if project else (False, "Proje bulunamadı.")
    project = db_get_project(project_id)
    try:
        bot.edit_message_text(project_detail_text(project) + f"\n\n{'✅' if ok else '❌'} {msg}",
                               call.message.chat.id, call.message.message_id,
                               reply_markup=build_project_detail_keyboard(project))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_logs:"))
@authorized
def cb_project_logs(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    bot.answer_callback_query(call.id)
    if not project:
        bot.send_message(call.message.chat.id, "❌ Proje bulunamadı.")
        return
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    log_text = tail_log(Path(project["source_path"]))
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⬅️ Proje", callback_data=f"proj_detail:{project_id}"))
    safe_log = telebot.util.escape(log_text[-3500:])
    bot.send_message(call.message.chat.id,
                      f"📋 <b>{project['name']} -- son {LOG_TAIL_LINES} satır</b>\n<code>{safe_log}</code>",
                      reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_delete_ask:"))
@authorized
def cb_project_delete_ask(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Evet, sil", callback_data=f"proj_delete_confirm:{project_id}"),
        types.InlineKeyboardButton("❌ Vazgeç", callback_data=f"proj_detail:{project_id}"),
    )
    bot.edit_message_text("⚠️ Bu projeyi ve tüm dosyalarını kalıcı olarak silmek istediğine emin misin?",
                           call.message.chat.id, call.message.message_id, reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("proj_delete_confirm:"))
@authorized
def cb_project_delete_confirm(call):
    project_id = call.data.split(":", 1)[1]
    project = db_get_project(project_id)
    if not can_manage_project(project, call.from_user.id):
        deny_project_access(call)
        return
    bot.answer_callback_query(call.id, "🗑️ Siliniyor...")
    if project:
        stop_process(project_id)
        try:
            shutil.rmtree(project["source_path"], ignore_errors=True)
        except Exception as e:
            kex_logger.error("Silme hatasi: %s", e)
        db_delete_project(project_id)
        log_event("project_delete", project["name"])
    is_admin = call.from_user.id == OWNER_ID
    owner_filter = None if is_admin else call.from_user.id
    try:
        bot.edit_message_text("✅ Proje silindi.", call.message.chat.id, call.message.message_id,
                               reply_markup=build_projects_keyboard(0, owner_filter))
    except Exception:
        bot.send_message(call.message.chat.id, "✅ Proje silindi.", reply_markup=build_projects_keyboard(0, owner_filter))


# ---------- Dosya yukleme (.py / .zip / .js) ----------

@bot.message_handler(content_types=["document"])
@authorized
def handle_document(message):
    uid = message.from_user.id
    if _upload_locks.get(uid):
        bot.reply_to(message, "⏳ Az önce bir yükleme işleniyor, lütfen bekle.")
        return

    is_admin = uid == OWNER_ID
    if not is_admin:
        user_row = db_get_or_create_user(uid, message.from_user.username)
        plan = PLANS.get(user_row.get("plan", DEFAULT_PLAN), PLANS[DEFAULT_PLAN])
        used = db_count_user_projects(uid)
        if used >= plan["capacity"]:
            bot.reply_to(
                message,
                f"⛔ <b>Bot kapasiten dolu.</b>\n"
                f"Planın: {plan['label']} — {used}/{plan['capacity']} kullanımda.\n\n"
                f"Yeni bot eklemek için planını yükseltmen gerekiyor. "
                f"Detay için 🎫 PLANIM'a bak ya da {DEVELOPER_TAG} ile iletişime geç.",
            )
            return

    doc = message.document
    file_name = doc.file_name or "unnamed"
    ext = Path(file_name).suffix.lower()

    if ext not in ALLOWED_UPLOAD_EXT:
        bot.reply_to(message, f"❌ Desteklenmeyen dosya türü: {ext}\nSadece .py, .zip, .js kabul edilir.")
        return

    if doc.file_size and doc.file_size > MAX_UPLOAD_MB * 1024 * 1024:
        bot.reply_to(message, f"❌ Dosya çok büyük (max {MAX_UPLOAD_MB}MB).")
        return

    status_msg = bot.reply_to(message, "📥 Dosya indiriliyor ve analiz ediliyor...")
    _upload_locks[uid] = True
    threading.Thread(target=_process_upload, args=(message, doc, file_name, ext, status_msg, uid), daemon=True).start()


def _edit_status(status_msg, text):
    try:
        bot.edit_message_text(text, status_msg.chat.id, status_msg.message_id)
    except Exception:
        try:
            bot.send_message(status_msg.chat.id, text)
        except Exception:
            pass


def _process_upload(message, doc, file_name, ext, status_msg, owner_id):
    project_id = new_project_id()
    project_dir = PROJECTS_DIR / project_id
    try:
        project_dir.mkdir(parents=True, exist_ok=True)

        file_info = bot.get_file(doc.file_id)
        content = bot.download_file(file_info.file_path)

        if ext == ".zip":
            tmp_zip = TEMP_DIR / f"{project_id}.zip"
            tmp_zip.write_bytes(content)
            try:
                safe_extract_zip(tmp_zip, project_dir)
            except Exception as e:
                shutil.rmtree(project_dir, ignore_errors=True)
                _edit_status(status_msg, f"❌ ZIP güvenlik hatası: {e}")
                return
            finally:
                tmp_zip.unlink(missing_ok=True)

            main_file, ptype = detect_main_file(project_dir)
            if not main_file:
                shutil.rmtree(project_dir, ignore_errors=True)
                _edit_status(status_msg, "❌ ZIP içinde çalıştırılabilir .py/.js dosyası bulunamadı.")
                return
        else:
            target = project_dir / file_name
            target.write_bytes(content)
            main_file = file_name
            ptype = "py" if ext == ".py" else "js"

        display_name = file_name

        if ptype == "py":
            _edit_status(status_msg, f"🔎 Proje analiz ediliyor...\n📄 Ana dosya: {main_file}")
            packages = analyze_project_dependencies(project_dir)

            _edit_status(status_msg, "🧪 Sanal ortam (venv) oluşturuluyor...")
            try:
                venv_dir = create_venv(project_dir)
            except Exception as e:
                shutil.rmtree(project_dir, ignore_errors=True)
                _edit_status(status_msg, f"❌ venv oluşturulamadı: {e}")
                return

            def progress(text):
                _edit_status(status_msg, text)

            _edit_status(status_msg, "📥 Bağımlılıklar kuruluyor (biraz sürebilir)...")
            ok, output = install_requirements(project_dir, venv_dir, packages, progress_cb=progress)
            if not ok:
                shutil.rmtree(project_dir, ignore_errors=True)
                _edit_status(status_msg, f"❌ Bağımlılık kurulumu başarısız:\n<code>{telebot.util.escape(output[-800:])}</code>")
                return
            venv_path_str = str(venv_dir)
        else:
            venv_path_str = None

        db_insert_project({
            "id": project_id,
            "owner_id": owner_id,
            "name": display_name,
            "project_type": ptype,
            "main_file": main_file,
            "source_path": str(project_dir),
            "venv_path": venv_path_str,
            "status": "stopped",
            "pid": None,
            "auto_start": 0,
        })
        log_event("project_upload", display_name, project_id)

        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🚀 Şimdi Başlat", callback_data=f"proj_start:{project_id}"))
        kb.add(types.InlineKeyboardButton("📦 Proje Detayı", callback_data=f"proj_detail:{project_id}"))
        _edit_status(status_msg, f"✅ <b>YÜKLENDİ!</b>\n📄 {display_name}\n🎯 {ptype.upper()}\n🆔 <code>{project_id}</code>")
        bot.send_message(message.chat.id, "Ne yapmak istersin?", reply_markup=kb)

    except Exception as e:
        kex_logger.error("Yukleme hatasi: %s", e, exc_info=True)
        shutil.rmtree(project_dir, ignore_errors=True)
        _edit_status(status_msg, f"❌ Beklenmeyen hata: {str(e)[:200]}")
    finally:
        _upload_locks[owner_id] = False


# ---------- Diger mesajlar ----------

@bot.message_handler(func=lambda m: True)
@authorized
def fallback_handler(message):
    bot.send_message(message.chat.id, "Anlaşılmayan komut. Ana menü için /start yaz.")


# ════════════════════════════════════════════════════════════════════════════
# 10) BASLATMA
# ════════════════════════════════════════════════════════════════════════════

def _graceful_shutdown(*_args):
    kex_logger.warning("Kapatma sinyali alındı, çalışan süreçler sonlandırılıyor...")
    for pid in list(running_procs.keys()):
        stop_process(pid)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    init_db()
    reconcile_statuses_on_boot()

    kex_logger.info("=" * 60)
    kex_logger.info("⚡ KEX SİSTEM v%s başlatılıyor...", KEX_VERSION)
    kex_logger.info("👑 Owner: %s", OWNER_ID)
    kex_logger.info("📂 Projeler: %s", PROJECTS_DIR)
    kex_logger.info("=" * 60)

    try:
        me = bot.get_me()
        kex_logger.info("🤖 Bot bağlandı: @%s (ID: %s)", me.username, me.id)
    except Exception as e:
        kex_logger.critical("Bot bağlantı hatası: %s", e)
        sys.exit(1)

    try:
        bot.send_message(OWNER_ID, f"⚡ KEX SİSTEM v{KEX_VERSION} devrede!\n/start yazarak paneli aç.")
    except Exception:
        pass

    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=20)
        except Exception as e:
            kex_logger.critical("infinity_polling hatası: %s", e)
            time.sleep(10)
