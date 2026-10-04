"""SQLite 事务隔离实验室 —— Flask 后端。

每个实验对应一个独立的 WAL 数据库（单行整数值 100）。
页面左右两栏（甲 / 乙）各绑定一条长期独立连接，事务完全由显式 SQL 控制；
第三条连接只读已提交值。状态刷新不执行任何 SELECT，
避免在 DEFERRED 事务中提前建立读快照。
"""

import os
import re
import shutil
import sqlite3
import threading
import time
import uuid

from flask import Flask, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("LAB_DATA_DIR", os.path.join(BASE_DIR, "data"))
MAX_LOG_ENTRIES = 200
MAX_ADD_ABS = 2 ** 62  # 单次增量绝对值上限
# 累加结果只允许落在有符号 64 位整数范围内，越界即拒绝，绝不允许 SQLite 转成 REAL
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1
# JavaScript Number 的安全整数范围：之内以 JSON 数字返回，之外以十进制字符串返回
JS_SAFE_INT_MIN = -(2 ** 53 - 1)
JS_SAFE_INT_MAX = 2 ** 53 - 1

_INTEGER_RE = re.compile(r"^[+-]?\d+$")


def decode_int(raw):
    """无损解析请求中的整数。

    接受 JSON 数字（int / 整数值 float）与十进制字符串；布尔、非整数浮点、
    非十进制字符串一律拒绝。超出 JS 安全整数范围的值只能以字符串传输。
    """
    if isinstance(raw, bool):  # bool 是 int 的子类，必须先排除
        raise ValueError("not an integer")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        # 兼容旧客户端发来的整数值浮点（如 10.0），非整数直接拒绝
        if not raw.is_integer():
            raise ValueError("not an integer")
        return int(raw)
    if isinstance(raw, str) and _INTEGER_RE.match(raw):
        return int(raw)
    raise ValueError("not an integer")


def wire_int(value):
    """整数的出参编码：JS 安全整数范围内返回数字，之外返回十进制字符串。"""
    if JS_SAFE_INT_MIN <= value <= JS_SAFE_INT_MAX:
        return value
    return str(value)

VALID_OPS = {"begin", "read", "add", "commit", "rollback"}
OP_LABELS = {
    "begin": "BEGIN DEFERRED",
    "read": "读值",
    "commit": "COMMIT",
    "rollback": "ROLLBACK",
}

app = Flask(__name__)


class PanelState:
    """某一栏的展示状态。last_read / log 记录在内存中，刷新状态不触碰 SQLite。"""

    def __init__(self):
        self.last_read = None
        self.log = []

    def add_log(self, text):
        self.log.append("%s  %s" % (time.strftime("%H:%M:%S"), text))
        if len(self.log) > MAX_LOG_ENTRIES:
            del self.log[: len(self.log) - MAX_LOG_ENTRIES]


class Experiment:
    def __init__(self, exp_id):
        self.id = exp_id
        self.db_path = os.path.join(DATA_DIR, "exp_%s.db" % exp_id)
        self._init_db()
        # 两栏各自的长期连接：isolation_level=None 进入自动提交模式，
        # 事务完全由显式 BEGIN/COMMIT/ROLLBACK 控制；
        # busy_timeout=0 让写锁冲突立即报错而不是等待。
        self.conns = {"a": self._open_conn(), "b": self._open_conn()}
        self.conn_locks = {"a": threading.Lock(), "b": threading.Lock()}
        # 第三条连接：只读已提交值
        self.observer = self._open_conn()
        self.observer_lock = threading.Lock()
        self.panels = {"a": PanelState(), "b": PanelState()}

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "CREATE TABLE kv (id INTEGER PRIMARY KEY CHECK (id = 1),"
                " val INTEGER NOT NULL, CHECK (typeof(val) = 'integer'))"
            )
            conn.execute("INSERT INTO kv (id, val) VALUES (1, 100)")
            conn.commit()
        finally:
            conn.close()

    def _open_conn(self):
        conn = sqlite3.connect(
            self.db_path, timeout=0, isolation_level=None, check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=0")
        return conn

    def panel_state(self, side):
        # 只读取内存状态与连接标志位，绝不执行 SELECT，
        # 避免为 DEFERRED 事务提前建立读快照。
        panel = self.panels[side]
        return {
            "in_transaction": self.conns[side].in_transaction,
            "last_read": None if panel.last_read is None else wire_int(panel.last_read),
            "log": list(panel.log),
        }

    def committed_value(self):
        with self.observer_lock:
            row = self.observer.execute(
                "SELECT val FROM kv WHERE id = 1"
            ).fetchone()
            return wire_int(row[0])


def run_op(exp, side, op, value):
    """在指定栏的长期连接上真实执行一个操作，返回 {ok, error, state}。"""
    panel = exp.panels[side]
    conn = exp.conns[side]
    label = "加上 %s" % value if op == "add" else OP_LABELS.get(op, op)

    def reject(message):
        panel.add_log("%s → 拒绝：%s" % (label, message))
        return {
            "ok": False,
            "error": {"name": None, "code": None, "message": message},
            "state": exp.panel_state(side),
        }

    with exp.conn_locks[side]:
        try:
            if op == "begin":
                if conn.in_transaction:
                    return reject("事务已开启，不能重复 BEGIN")
                conn.execute("BEGIN DEFERRED")
                panel.add_log("BEGIN DEFERRED → 成功，事务已开启")
            elif op == "read":
                row = conn.execute("SELECT val FROM kv WHERE id = 1").fetchone()
                panel.last_read = row[0]
                scope = "事务内" if conn.in_transaction else "事务外（自动提交）"
                panel.add_log("读值（%s）→ %s" % (scope, row[0]))
            elif op == "add":
                if not conn.in_transaction:
                    return reject("禁止事务外修改：请先 BEGIN DEFERRED")
                # 不做任何预读 SELECT（避免改变 DEFERRED 事务建立快照的时机），
                # 直接执行 UPDATE：表上的 CHECK(typeof(val)='integer') 约束保证
                # 整数加法一旦溢出成 REAL 就在亲和性转换前被数据库原子拒绝
                # （SQLITE_CONSTRAINT_CHECK），正、负越界都拦，边界本身合法。
                try:
                    conn.execute("UPDATE kv SET val = val + ? WHERE id = 1", (value,))
                except sqlite3.IntegrityError:
                    return reject(
                        "累加结果超出有符号 64 位整数范围（%d ~ %d），"
                        "本次加法未执行，事务仍保持开启"
                        % (INT64_MIN, INT64_MAX)
                    )
                panel.add_log("加上 %s → 成功（尚未提交）" % value)
            elif op == "commit":
                if not conn.in_transaction:
                    return reject("当前没有开启的事务")
                conn.execute("COMMIT")
                panel.add_log("COMMIT → 成功，事务已提交")
            elif op == "rollback":
                if not conn.in_transaction:
                    return reject("当前没有开启的事务")
                conn.execute("ROLLBACK")
                panel.add_log("ROLLBACK → 成功，事务已回滚")
            else:
                return reject("未知操作：%s" % op)
        except sqlite3.Error as exc:
            # 写锁冲突等 SQLite 错误：如实上报错误名与错误码，
            # 不做任何自动回滚，失败的事务保持开启。
            name = getattr(exc, "sqlite_errorname", None) or "SQLITE_ERROR"
            code = getattr(exc, "sqlite_errorcode", None)
            panel.add_log("%s → 失败 [%s (%s)] %s" % (label, name, code, exc))
            return {
                "ok": False,
                "error": {"name": name, "code": code, "message": str(exc)},
                "state": exp.panel_state(side),
            }
    return {"ok": True, "error": None, "state": exp.panel_state(side)}


# 实验注册表：仅保存在内存中，无需跨重启保留
experiments = {}
experiments_lock = threading.Lock()


def get_experiment(exp_id):
    with experiments_lock:
        return experiments.get(exp_id)


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/api/experiments")
def create_experiment():
    exp_id = uuid.uuid4().hex[:12]
    exp = Experiment(exp_id)
    with experiments_lock:
        experiments[exp_id] = exp
    return jsonify({"id": exp_id})


@app.get("/api/experiments/<exp_id>/status")
def experiment_status(exp_id):
    exp = get_experiment(exp_id)
    if exp is None:
        return jsonify({"error": "experiment not found"}), 404
    return jsonify(
        {"id": exp.id, "a": exp.panel_state("a"), "b": exp.panel_state("b")}
    )


@app.get("/api/experiments/<exp_id>/committed")
def experiment_committed(exp_id):
    exp = get_experiment(exp_id)
    if exp is None:
        return jsonify({"error": "experiment not found"}), 404
    return jsonify({"committed": exp.committed_value()})


@app.post("/api/experiments/<exp_id>/conn/<side>/op")
def experiment_op(exp_id, side):
    exp = get_experiment(exp_id)
    if exp is None:
        return jsonify({"error": "experiment not found"}), 404
    if side not in ("a", "b"):
        return jsonify({"error": "unknown side"}), 400
    data = request.get_json(silent=True) or {}
    op = data.get("op")
    if op not in VALID_OPS:
        return jsonify({"error": "unknown op"}), 400
    value = None
    if op == "add":
        try:
            value = decode_int(data.get("value"))
        except ValueError:
            return jsonify({"error": "value must be an integer"}), 400
        if abs(value) > MAX_ADD_ABS:
            return jsonify({"error": "value too large"}), 400
    return jsonify(run_op(exp, side, op, value))


def prepare_data_dir():
    # 实验无需跨重启保留：启动时清空旧数据库文件
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR, ignore_errors=True)
    os.makedirs(DATA_DIR, exist_ok=True)


prepare_data_dir()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
