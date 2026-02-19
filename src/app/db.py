from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence, Any, Optional

from .config import settings


def _has_column(con, table, col) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    return any(r["name"] == col for r in cur.fetchall())

def _ensure_parent_dir(db_path: str) -> None:
    p = Path(db_path)
    if p.parent and str(p.parent) not in (".", ""):
        p.parent.mkdir(parents=True, exist_ok=True)


def get_db_path() -> str:
    db_path = os.getenv("DB_PATH", "./data/app.db")
    _ensure_parent_dir(db_path)
    return db_path


def connect() -> sqlite3.Connection:
    con = sqlite3.connect(get_db_path())
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_products (
            offer_id TEXT PRIMARY KEY,
            product_id INTEGER NOT NULL,
            archived INTEGER NOT NULL,

            moderate_status TEXT,         -- approved / declined / ...
            validation_status TEXT,       -- success / ...
            status TEXT,                  -- price_sent / ...
            
            description_category_id INTEGER,
            commission_fbs_percent REAL,
            markup_percent REAL,

            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    
    if not _has_column(con, "ozon_products", "moderate_status"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN moderate_status TEXT;")
    if not _has_column(con, "ozon_products", "validation_status"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN validation_status TEXT;")
    if not _has_column(con, "ozon_products", "status"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN status TEXT;")

    if not _has_column(con, "ozon_products", "description_category_id"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN description_category_id INTEGER;")
    if not _has_column(con, "ozon_products", "commission_fbs_percent"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN commission_fbs_percent REAL;")
    if not _has_column(con, "ozon_products", "markup_percent"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN markup_percent REAL;")
    
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ozon_product_details (
            offer_id TEXT PRIMARY KEY,
            product_id INTEGER NOT NULL,

            name TEXT,

            weight_g INTEGER,
            length_mm INTEGER,   -- depth из attributes -> length_mm
            width_mm INTEGER,
            height_mm INTEGER,

            updated_at TEXT NOT NULL DEFAULT (datetime('now')),

            FOREIGN KEY(offer_id) REFERENCES ozon_products(offer_id) ON DELETE CASCADE
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at TEXT,
            status TEXT,
            note TEXT
        );
        """
    )
    
    con.execute(
    """
    CREATE TABLE IF NOT EXISTS supplier_autorus (
        pcode TEXT PRIMARY KEY,
        brand TEXT,
        number TEXT,
        parts_url TEXT,
        price_rub REAL,
        qty INTEGER,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """
    )

    if not _has_column(con, "ozon_products", "price_current"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN price_current REAL;")
        
    # --- supplier mapping / data ---
    if not _has_column(con, "ozon_products", "supplier_pcode"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_pcode TEXT;")

    if not _has_column(con, "ozon_products", "supplier_brand"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_brand TEXT;")

    if not _has_column(con, "ozon_products", "supplier_number"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_number TEXT;")

    if not _has_column(con, "ozon_products", "supplier_parts_url"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_parts_url TEXT;")

    if not _has_column(con, "ozon_products", "supplier_price_rub"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_price_rub REAL;")

    if not _has_column(con, "ozon_products", "supplier_qty"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN supplier_qty INTEGER;")

    # --- calculated ozon price ---
    if not _has_column(con, "ozon_products", "ozon_price_calc"):
        con.execute("ALTER TABLE ozon_products ADD COLUMN ozon_price_calc INTEGER;")


    con.commit()
