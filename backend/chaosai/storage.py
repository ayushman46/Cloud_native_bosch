import sqlite3
from pathlib import Path
from threading import Lock
from .schemas import Experiment


class Store:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = Lock()
        with self._connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS experiments (id TEXT PRIMARY KEY, data TEXT NOT NULL)')

    def _connect(self):
        return sqlite3.connect(self.path)

    def save(self, exp: Experiment):
        with self.lock, self._connect() as db:
            db.execute('INSERT OR REPLACE INTO experiments VALUES (?,?)', (exp.experiment_id, exp.model_dump_json()))

    def get(self, experiment_id: str) -> Experiment | None:
        with self.lock, self._connect() as db:
            row = db.execute('SELECT data FROM experiments WHERE id=?', (experiment_id,)).fetchone()
        return Experiment.model_validate_json(row[0]) if row else None

    def list(self) -> list[Experiment]:
        with self.lock, self._connect() as db:
            rows = db.execute('SELECT data FROM experiments ORDER BY rowid DESC LIMIT 100').fetchall()
        return [Experiment.model_validate_json(x[0]) for x in rows]
