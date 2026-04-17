import csv
import http.server
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get('PORT', 8090))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, 'ログCSV')
STORE_PATH = os.path.join(BASE_DIR, 'data', 'store.json')
SWITCHBOT_DIR = os.path.join(BASE_DIR, 'switchbot')
SWITCHBOT_DATA_DIR = os.path.join(SWITCHBOT_DIR, 'data')
SWITCHBOT_FETCH_SCRIPT = os.path.join(SWITCHBOT_DIR, 'fetch.py')
SWITCHBOT_CONFIG_PATH = os.path.join(SWITCHBOT_DIR, 'config.json')
SWITCHBOT_POLL_INTERVAL_SEC = 1800  # 30分


def load_store():
    """store.json を読み込む。なければ None を返す"""
    if not os.path.exists(STORE_PATH):
        return None
    try:
        with open(STORE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def save_store(store):
    """store.json にアトミック書き込み（OneDrive競合対策）"""
    os.makedirs(os.path.dirname(STORE_PATH), exist_ok=True)
    dir_ = os.path.dirname(STORE_PATH)
    fd, tmp_path = tempfile.mkstemp(dir=dir_, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STORE_PATH)  # アトミックなリネーム
    except Exception:
        os.unlink(tmp_path)
        raise


def parse_section(content, section_name):
    """Multi-section CSV からセクションを抽出して dict のリストで返す"""
    lines = content.split('\n')
    in_section = False
    section_lines = []

    for line in lines:
        if line.strip() == f'## {section_name}':
            in_section = True
            continue
        if in_section:
            if line.startswith('## '):
                break
            section_lines.append(line)

    non_empty = [l for l in section_lines if l.strip()]
    if len(non_empty) < 2:
        return []

    reader = csv.DictReader(io.StringIO('\n'.join(non_empty)))
    return list(reader)


def _activity_label(val):
    return 'やった' if val == 'yes' else ('できなかった' if val == 'no' else '')

def _exercise_label(val):
    return 'やった' if val == 'yes' else ('休み' if val == 'no' else '')

def _priority_label(val):
    return {'high': '高', 'medium': '中', 'low': '低'}.get(val, '')

def _status_label(val):
    return {'todo': '未着手', 'doing': '進行中', 'done': '完了'}.get(val, '')


def generate_csv_content(store, date):
    """store の内容から指定日付の CSV テキストを生成する"""
    buf = io.StringIO()

    # 朝ログ
    buf.write('## 朝ログ\n')
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(['日付', '起床時間', '気分', '体調', '睡眠の質', '朝活', '朝活内容', 'メモ'])
    for log in store.get('logs', []):
        if log.get('date') == date and log.get('type') == 'morning':
            w.writerow([
                date,
                log.get('wakeTime', ''),
                log.get('mood', ''),
                str(log.get('bodyCondition', '')),
                str(log.get('sleepQuality', '')),
                _activity_label(log.get('morningActivity', '')),
                log.get('morningActivityNote', ''),
                log.get('morningNote', ''),
            ])
    buf.write('\n')

    # 夜ログ
    buf.write('## 夜ログ\n')
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(['日付', '就寝予定', '気分', '体調', '運動', '運動内容', '明日の最優先タスク', '感謝・よかったこと', '振り返り'])
    for log in store.get('logs', []):
        if log.get('date') == date and log.get('type') == 'night':
            w.writerow([
                date,
                log.get('bedTime', ''),
                log.get('mood', ''),
                str(log.get('bodyCondition', '')),
                _exercise_label(log.get('exerciseDone', '')),
                log.get('exercise', ''),
                log.get('tomorrowGoal', ''),
                log.get('gratitude', ''),
                log.get('reflection', ''),
            ])
    buf.write('\n')

    # Todo（全件）
    buf.write('## Todo\n')
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(['タスク名', '重要度', '期限', '状態', 'メモ', '作成日'])
    for todo in store.get('todos', []):
        w.writerow([
            todo.get('text', ''),
            _priority_label(todo.get('priority', '')),
            todo.get('dueDate', ''),
            _status_label(todo.get('status', '')),
            todo.get('memo', ''),
            todo.get('date', ''),
        ])
    buf.write('\n')

    # 日記
    buf.write('## 日記\n')
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(['日付', '内容'])
    for diary in store.get('diaries', []):
        if diary.get('date') == date:
            w.writerow([date, diary.get('content', '')])
    buf.write('\n')

    # つぶやき
    buf.write('## つぶやき\n')
    w = csv.writer(buf, lineterminator='\n')
    w.writerow(['日時', '内容', '気分', 'タグ'])
    for m in store.get('murmurs', []):
        m_date = (m.get('createdAt') or '')[:10]
        if m_date == date:
            tags = ' '.join(m.get('tags') or [])
            w.writerow([m.get('createdAt', ''), m.get('text', ''), m.get('mood', ''), tags])
    buf.write('\n')

    return buf.getvalue()


def sync_all_csvs(store):
    """store.json の内容を元に全日付の CSV を再生成する"""
    os.makedirs(CSV_DIR, exist_ok=True)

    dates = set()
    for log in store.get('logs', []):
        if log.get('date'):
            dates.add(log['date'])
    for diary in store.get('diaries', []):
        if diary.get('date'):
            dates.add(diary['date'])

    for date in sorted(dates):
        content = generate_csv_content(store, date)
        csv_path = os.path.join(CSV_DIR, f'selfcare_log_{date}.csv')
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            f.write(content)


# ═══════════════════════════════════════════
#  SwitchBot 連携
# ═══════════════════════════════════════════

def read_switchbot_records(start_dt, end_dt):
    """指定期間のSwitchBotレコードをJSONLから読む"""
    if not os.path.isdir(SWITCHBOT_DATA_DIR):
        return []
    d = start_dt.date()
    end_date = end_dt.date()
    records = []
    while d <= end_date:
        path = os.path.join(SWITCHBOT_DATA_DIR, f'switchbot_{d.isoformat()}.jsonl')
        if os.path.exists(path):
            try:
                with open(path, encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            ts = datetime.fromisoformat(rec['timestamp'])
                            if start_dt <= ts <= end_dt:
                                records.append(rec)
                        except Exception:
                            continue
            except Exception:
                pass
        d = d + timedelta(days=1)
    return records


def _parse_hhmm(s):
    if not s:
        return None
    try:
        parts = s.split(':')
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def _stats_for_records(recs):
    if not recs:
        return None
    recs_sorted = sorted(recs, key=lambda x: x.get('timestamp', ''))
    temps = [r['temperature'] for r in recs if r.get('temperature') is not None]
    hums = [r['humidity'] for r in recs if r.get('humidity') is not None]
    def _avg(xs):
        return round(sum(xs) / len(xs), 1) if xs else None
    return {
        'count': len(recs),
        'first': {
            'timestamp': recs_sorted[0].get('timestamp'),
            'temperature': recs_sorted[0].get('temperature'),
            'humidity': recs_sorted[0].get('humidity'),
        },
        'last': {
            'timestamp': recs_sorted[-1].get('timestamp'),
            'temperature': recs_sorted[-1].get('temperature'),
            'humidity': recs_sorted[-1].get('humidity'),
        },
        'temp': {
            'min': min(temps) if temps else None,
            'max': max(temps) if temps else None,
            'avg': _avg(temps),
        },
        'humidity': {
            'min': min(hums) if hums else None,
            'max': max(hums) if hums else None,
            'avg': _avg(hums),
        },
        'battery': recs_sorted[-1].get('battery'),
    }


def compute_sleep_summary(date_str):
    """指定日付の朝ブリーフィング用データ

    期間: 前日のbedTime (無ければ22:00) から 当日のwakeTime (無ければ現在)
    """
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        return {'error': 'invalid date'}

    store = load_store() or {}
    logs = store.get('logs', [])
    prev_date = (date - timedelta(days=1)).isoformat()
    night_log = next((l for l in logs if l.get('date') == prev_date and l.get('type') == 'night'), None)
    morning_log = next((l for l in logs if l.get('date') == date_str and l.get('type') == 'morning'), None)

    bed_time_str = (night_log or {}).get('bedTime', '')
    wake_time_str = (morning_log or {}).get('wakeTime', '')

    prev_d = date - timedelta(days=1)
    bed_parsed = _parse_hhmm(bed_time_str)
    if bed_parsed:
        h, m = bed_parsed
        # 深夜(0-5時)は当日扱い、それ以外は前日扱い
        if h < 5:
            start_dt = datetime(date.year, date.month, date.day, h, m)
        else:
            start_dt = datetime(prev_d.year, prev_d.month, prev_d.day, h, m)
    else:
        start_dt = datetime(prev_d.year, prev_d.month, prev_d.day, 22, 0)

    wake_parsed = _parse_hhmm(wake_time_str)
    now = datetime.now()
    if wake_parsed:
        h, m = wake_parsed
        end_dt = datetime(date.year, date.month, date.day, h, m)
    else:
        if now.date() == date:
            end_dt = now
        else:
            end_dt = datetime(date.year, date.month, date.day, 9, 0)

    # 終了が開始より前なら最低1時間は確保
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)

    records = read_switchbot_records(start_dt, end_dt)

    # ラベル別に集計
    by_label = {}
    for r in records:
        by_label.setdefault(r.get('label', '?'), []).append(r)
    devices = {label: _stats_for_records(recs) for label, recs in by_label.items()}

    # 最新レコード（期間外も含めた直近）を取得
    latest_by_label = {}
    today_path = os.path.join(SWITCHBOT_DATA_DIR, f'switchbot_{date.isoformat()}.jsonl')
    if os.path.exists(today_path):
        try:
            with open(today_path, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        latest_by_label[rec.get('label', '?')] = rec
                    except Exception:
                        continue
        except Exception:
            pass

    # コメント生成（寝室のみ）
    bedroom = devices.get('寝室')
    comment = ''
    if bedroom and bedroom['temp']['avg'] is not None and bedroom['humidity']['avg'] is not None:
        t_avg = bedroom['temp']['avg']
        h_avg = bedroom['humidity']['avg']
        in_temp = 20 <= t_avg <= 23
        in_hum = 40 <= h_avg <= 60
        if in_temp and in_hum:
            comment = '💡 快眠レンジ内で推移、GOOD'
        elif in_temp and not in_hum:
            comment = f'💡 湿度{h_avg}% — ' + ('やや乾燥気味' if h_avg < 40 else 'やや高め') + '（快眠レンジ40-60%）'
        elif not in_temp and in_hum:
            comment = f'💡 寝室平均{t_avg}°C — ' + ('少し冷えた' if t_avg < 20 else '少し暖かい') + '（快眠レンジ20-23°C）'
        else:
            comment = f'💡 寝室{t_avg}°C / 湿度{h_avg}% — 快眠レンジ外'

    return {
        'date': date_str,
        'period': {
            'start': start_dt.isoformat(timespec='minutes'),
            'end': end_dt.isoformat(timespec='minutes'),
            'bedTime': bed_time_str,
            'wakeTime': wake_time_str,
        },
        'devices': devices,
        'latest': latest_by_label,
        'comment': comment,
        'morning': {
            'sleepQuality': (morning_log or {}).get('sleepQuality'),
            'bodyCondition': (morning_log or {}).get('bodyCondition'),
            'mood': (morning_log or {}).get('mood'),
        } if morning_log else None,
    }


def switchbot_poll_once():
    """fetch.py を一回実行して温湿度を記録"""
    if not os.path.exists(SWITCHBOT_FETCH_SCRIPT):
        return
    if not os.path.exists(SWITCHBOT_CONFIG_PATH):
        return
    try:
        kwargs = {
            'cwd': SWITCHBOT_DIR,
            'capture_output': True,
            'timeout': 60,
        }
        # Windowsの場合、ウィンドウを出さない
        if sys.platform == 'win32':
            kwargs['creationflags'] = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
        subprocess.run([sys.executable, SWITCHBOT_FETCH_SCRIPT], **kwargs)
    except Exception:
        pass


def switchbot_poll_loop():
    """バックグラウンドで定期的に温湿度を取得"""
    # 起動直後に一度取得
    switchbot_poll_once()
    while True:
        time.sleep(SWITCHBOT_POLL_INTERVAL_SEC)
        switchbot_poll_once()


def load_all_data():
    logs = []
    todos = []
    diaries = []
    seen_log_keys = set()
    seen_todo_ids = set()
    seen_diary_dates = set()

    if not os.path.exists(CSV_DIR):
        return {'logs': logs, 'todos': todos, 'diaries': diaries}

    for fname in sorted(os.listdir(CSV_DIR)):
        if not fname.endswith('.csv'):
            continue
        fpath = os.path.join(CSV_DIR, fname)
        with open(fpath, encoding='utf-8-sig') as f:  # utf-8-sig でBOMを除去
            content = f.read()

        # 朝ログ
        for row in parse_section(content, '朝ログ'):
            date = row.get('日付', '').strip()
            if not date:
                continue
            key = (date, 'morning')
            if key in seen_log_keys:
                continue
            seen_log_keys.add(key)
            bc = row.get('体調', '3').strip()
            sq = row.get('睡眠の質', '3').strip()
            def _to_num(s, default=3):
                try:
                    v = float(s)
                    return int(v) if v.is_integer() else v
                except Exception:
                    return default
            note = row.get('朝活内容', '').strip() or row.get('メモ', '').strip()
            activity_raw = row.get('朝活', '').strip()
            activity = 'yes' if activity_raw == 'やった' else ('no' if activity_raw == 'できなかった' else '')
            logs.append({
                'date': date,
                'type': 'morning',
                'wakeTime': row.get('起床時間', '').strip(),
                'mood': row.get('気分', '').strip(),
                'weight': row.get('体重', '').strip(),
                'bodyCondition': _to_num(bc),
                'sleepQuality': _to_num(sq),
                'morningActivity': activity,
                'morningActivityNote': row.get('朝活内容', '').strip(),
                'morningNote': row.get('メモ', '').strip(),
                'createdAt': f'{date}T00:00:00.000Z',
            })

        # 夜ログ
        for row in parse_section(content, '夜ログ'):
            date = row.get('日付', '').strip()
            if not date:
                continue
            key = (date, 'night')
            if key in seen_log_keys:
                continue
            seen_log_keys.add(key)
            ex_raw = row.get('運動', '').strip()
            exercise_done = 'yes' if ex_raw == 'やった' else ('no' if ex_raw == '休み' else '')
            bc_n = row.get('体調', '').strip()
            def _to_num2(s, default=None):
                try:
                    v = float(s)
                    return int(v) if v.is_integer() else v
                except Exception:
                    return default
            night_entry = {
                'date': date,
                'type': 'night',
                'bedTime': row.get('就寝予定', '').strip(),
                'mood': row.get('気分', '').strip(),
                'exerciseDone': exercise_done,
                'exercise': row.get('運動内容', '').strip(),
                'tomorrowGoal': row.get('明日の最優先タスク', '').strip(),
                'gratitude': row.get('感謝・よかったこと', '').strip(),
                'reflection': row.get('振り返り', '').strip(),
                'createdAt': f'{date}T00:00:00.000Z',
            }
            bc_parsed = _to_num2(bc_n)
            if bc_parsed is not None:
                night_entry['bodyCondition'] = bc_parsed
            logs.append(night_entry)

        # Todo
        for row in parse_section(content, 'Todo'):
            text = row.get('タスク名', '').strip()
            if not text:
                continue
            created = row.get('作成日', '').strip()
            todo_id = f'csv_{hash(text) & 0xFFFFFFFF:08x}_{created}'
            if todo_id in seen_todo_ids:
                continue
            seen_todo_ids.add(todo_id)
            status_map = {'未着手': 'todo', '進行中': 'doing', '完了': 'done'}
            prio_map = {'高': 'high', '中': 'medium', '低': 'low'}
            status_raw = row.get('状態', '').strip()
            prio_raw = row.get('重要度', '').strip()
            todos.append({
                'id': todo_id,
                'text': text,
                'status': status_map.get(status_raw, 'todo'),
                'priority': prio_map.get(prio_raw, 'medium'),
                'type': 'habit',
                'date': created,
                'dueDate': row.get('期限', '').strip(),
                'memo': row.get('メモ', '').strip(),
                'createdAt': f'{created}T00:00:00.000Z' if created else '',
            })

        # 日記
        for row in parse_section(content, '日記'):
            date = row.get('日付', '').strip()
            if not date or date in seen_diary_dates:
                continue
            seen_diary_dates.add(date)
            diaries.append({
                'date': date,
                'content': row.get('内容', '').strip(),
                'createdAt': f'{date}T00:00:00.000Z',
            })

    return {'logs': logs, 'todos': todos, 'diaries': diaries}


class Handler(http.server.SimpleHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length).decode('utf-8'))
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/switchbot/summary':
            try:
                qs = parse_qs(parsed.query)
                date_str = qs.get('date', [datetime.now().strftime('%Y-%m-%d')])[0]
                self._send_json(compute_sleep_summary(date_str))
            except Exception as e:
                self._send_json({'error': str(e)}, status=500)
            return
        if parsed.path == '/api/switchbot/refresh':
            try:
                switchbot_poll_once()
                self._send_json({'ok': True})
            except Exception as e:
                self._send_json({'ok': False, 'error': str(e)}, status=500)
            return
        if parsed.path == '/api/data':
            try:
                # store.json を優先、なければCSVフォールバック
                store = load_store()
                if store:
                    self._send_json({
                        'source': 'store',
                        'logs': store.get('logs', []),
                        'todos': store.get('todos', []),
                        'diaries': store.get('diaries', []),
                        'murmurs': store.get('murmurs', []),
                    })
                else:
                    data = load_all_data()
                    data['source'] = 'csv'
                    self._send_json(data)
            except Exception as e:
                self.send_error(500, str(e))
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/save':
            try:
                body = self._read_body()
                key = body.get('type')   # 'logs' | 'diary' | 'todos' | 'weather' | 'streak'
                data = body.get('data')

                # キー名を store.json のキーに統一
                key_map = {
                    'selfcare_logs': 'logs',
                    'selfcare_diary': 'diaries',
                    'selfcare_todos': 'todos',
                    'selfcare_weather': 'weather',
                    'selfcare_streak': 'streak',
                    'selfcare_murmurs': 'murmurs',
                }
                store_key = key_map.get(key, key)

                store = load_store() or {}
                store[store_key] = data
                save_store(store)
                sync_all_csvs(store)
                self._send_json({'ok': True})
            except Exception as e:
                self._send_json({'ok': False, 'error': str(e)}, status=500)
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # アクセスログを抑制


class DualStackServer(http.server.HTTPServer):
    """IPv4/IPv6 両対応サーバー"""
    address_family = socket.AF_INET6

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


if __name__ == '__main__':
    os.chdir(BASE_DIR)
    store = load_store()
    if store:
        sync_all_csvs(store)
        print('CSV sync complete.', flush=True)

    # SwitchBot 定期取得スレッド開始（config.json があるときだけ）
    if os.path.exists(SWITCHBOT_CONFIG_PATH) and os.path.exists(SWITCHBOT_FETCH_SCRIPT):
        poll_thread = threading.Thread(target=switchbot_poll_loop, daemon=True)
        poll_thread.start()
        print(f'SwitchBot polling enabled (every {SWITCHBOT_POLL_INTERVAL_SEC}s).', flush=True)

    server = DualStackServer(('::', PORT), Handler)
    print(f'Self Care Log → http://localhost:{PORT}', flush=True)
    server.serve_forever()
