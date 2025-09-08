
#!/usr/bin/env python3
"""expense_tracker.py
Robust starter CLI for a Personal Expense Tracker using PostgreSQL.

Features:
- init-db : create the expenses table
- add : add a new expense (interactive or via args)
- list : list recent expenses with filters
- summary : aggregate totals by week or month
- category-report : totals grouped by category
- export : export query results to CSV
- import : import expenses from a CSV
- Useful environment-driven DB config (DATABASE_URL or PGHOST/PGUSER/PGPASSWORD/PGDATABASE)
"""
from __future__ import annotations
import os
import sys
import argparse
import csv
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
try:
    import psycopg2
    import psycopg2.extras as extras
except Exception as e:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    raise

# Optional niceties
try:
    from rich import print as rprint
    from rich.table import Table
    HAS_RICH = True
except Exception:
    HAS_RICH = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

DEFAULT_TABLE = "expenses"

def get_dsn() -> str:
    """Build a DSN from DATABASE_URL or individual PG env vars."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    # fallback to individual env vars
    user = os.getenv("PGUSER", os.getenv("USER"))
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "expense_tracker")
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        return f"postgresql://{user}@{host}:{port}/{dbname}"

def get_conn():
    dsn = get_dsn()
    return psycopg2.connect(dsn, cursor_factory=extras.RealDictCursor)

def init_db(force: bool = False):
    sql = f"""CREATE TABLE IF NOT EXISTS {DEFAULT_TABLE} (
        id SERIAL PRIMARY KEY,
        amount NUMERIC(12,2) NOT NULL,
        category VARCHAR(120) NOT NULL,
        description TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );"""
    if force:
        drop = f"DROP TABLE IF EXISTS {DEFAULT_TABLE};"
    else:
        drop = None
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if drop:
                    cur.execute(drop)
                cur.execute(sql)
        logging.info("Initialized database and ensured table exists.")
    finally:
        conn.close()

def add_expense(amount: Decimal, category: str, description: Optional[str], created_at: Optional[datetime]):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if created_at:
                    cur.execute(
                        f"INSERT INTO {DEFAULT_TABLE} (amount, category, description, created_at) VALUES (%s,%s,%s,%s) RETURNING id;",
                        (amount, category, description, created_at)
                    )
                else:
                    cur.execute(
                        f"INSERT INTO {DEFAULT_TABLE} (amount, category, description) VALUES (%s,%s,%s) RETURNING id;",
                        (amount, category, description)
                    )
                row = cur.fetchone()
                logging.info("Inserted expense id=%s", row["id"])
    finally:
        conn.close()

def query_expenses(limit: int = 100, since: Optional[str] = None, until: Optional[str] = None, category: Optional[str] = None) -> List[Dict[str, Any]]:
    where_clauses = []
    params = []
    if since:
        where_clauses.append("created_at >= %s")
        params.append(parse_date(since))
    if until:
        where_clauses.append("created_at <= %s")
        params.append(parse_date(until))
    if category:
        where_clauses.append("category = %s")
        params.append(category)
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"SELECT id, amount, category, description, created_at FROM {DEFAULT_TABLE} {where} ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
    finally:
        conn.close()

def print_rows(rows: List[Dict[str, Any]]):
    if HAS_RICH:
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("id", justify="right")
        table.add_column("amount", justify="right")
        table.add_column("category")
        table.add_column("description")
        table.add_column("created_at")
        for r in rows:
            table.add_row(str(r['id']), f"{r['amount']}", r['category'] or '', r.get('description') or '', r['created_at'].astimezone().isoformat())
        rprint(table)
    else:
        for r in rows:
            print(f"{r['id']:>4} | {r['amount']:>10} | {r['category']:<20} | {r.get('description',''):<40} | {r['created_at']}")
    print(f"\nTotal rows: {len(rows)}")

def summary_by_period(period: str = 'month', limit: int = 12):
    if period not in ('month','week'):
        raise ValueError('period must be month or week')
    sql = f"SELECT date_trunc(%s, created_at) as period, count(*) as count, sum(amount)::numeric(12,2) as total FROM {DEFAULT_TABLE} GROUP BY period ORDER BY period DESC LIMIT %s"
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (period, limit))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
    finally:
        conn.close()

def category_report(since: Optional[str]=None, until: Optional[str]=None, limit: int = 100):
    where_clauses = []
    params = []
    if since:
        where_clauses.append("created_at >= %s")
        params.append(parse_date(since))
    if until:
        where_clauses.append("created_at <= %s")
        params.append(parse_date(until))
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"SELECT category, count(*) as cnt, sum(amount)::numeric(12,2) as total FROM {DEFAULT_TABLE} {where} GROUP BY category ORDER BY total DESC LIMIT %s"
    params.append(limit)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
    finally:
        conn.close()

def export_to_csv(rows: List[Dict[str, Any]], filepath: str):
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id','amount','category','description','created_at'])
        for r in rows:
            writer.writerow([r['id'], r['amount'], r['category'], r.get('description',''), r['created_at'].isoformat()])
    logging.info('Exported %s rows to %s', len(rows), filepath)

def import_from_csv(filepath: str, has_header: bool = True, date_col: Optional[str] = 'created_at'):
    imported = 0
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f) if has_header else csv.reader(f)
        for row in reader:
            try:
                if has_header:
                    amount = Decimal(row['amount'])
                    category = row['category']
                    description = row.get('description','')
                    created_at = parse_date(row.get(date_col)) if row.get(date_col) else None
                else:
                    # expecting: amount,category,description,created_at
                    amount = Decimal(row[0])
                    category = row[1]
                    description = row[2] if len(row) > 2 else ''
                    created_at = parse_date(row[3]) if len(row) > 3 and row[3] else None
                add_expense(amount, category, description, created_at)
                imported += 1
            except Exception as e:
                logging.warning('Skipping row due to error: %s', e)
    logging.info('Imported %s rows from %s', imported, filepath)

def parse_date(s: Optional[str]) -> Optional[datetime]:
    if s is None or s == '':
        return None
    # Accept ISO format or common date formats
    for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                # assume local timezone -> convert to UTC-aware (system tz)
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    # fallback: try fromtimestamp if numeric
    try:
        ts = float(s)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        raise ValueError(f"Unrecognized date format: {s}")

def parse_amount(s: str) -> Decimal:
    try:
        return Decimal(s)
    except InvalidOperation as e:
        raise argparse.ArgumentTypeError("Invalid amount format. Example valid: 12.50") from e

def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description='Expense Tracker CLI (Postgres)')
    sub = p.add_subparsers(dest='cmd')

    sp_init = sub.add_parser('init-db', help='Create the expenses table')
    sp_init.add_argument('--force', action='store_true', help='Drop and recreate the table')

    sp_add = sub.add_parser('add', help='Add an expense')
    sp_add.add_argument('--amount', type=parse_amount, help='Amount (e.g. 12.50)')
    sp_add.add_argument('--category', help='Category (e.g. Food)')
    sp_add.add_argument('--description', help='Description')
    sp_add.add_argument('--date', help='Date (ISO e.g. 2023-06-01 or 2023-06-01T12:00:00)')

    sp_list = sub.add_parser('list', help='List recent expenses')
    sp_list.add_argument('--limit', type=int, default=50)
    sp_list.add_argument('--since', help='Start date filter (inclusive)')
    sp_list.add_argument('--until', help='End date filter (inclusive)')
    sp_list.add_argument('--category', help='Filter by category')

    sp_summary = sub.add_parser('summary', help='Show summary by week or month')
    sp_summary.add_argument('--period', choices=['week','month'], default='month')
    sp_summary.add_argument('--limit', type=int, default=12)

    sp_cat = sub.add_parser('category-report', help='Totals grouped by category')
    sp_cat.add_argument('--since', help='Start date filter (inclusive)')
    sp_cat.add_argument('--until', help='End date filter (inclusive)')
    sp_cat.add_argument('--limit', type=int, default=100)

    sp_export = sub.add_parser('export', help='Export query results to CSV (uses same filters as list)')
    sp_export.add_argument('--file', required=True)
    sp_export.add_argument('--limit', type=int, default=100)
    sp_export.add_argument('--since', help='Start date filter (inclusive)')
    sp_export.add_argument('--until', help='End date filter (inclusive)')
    sp_export.add_argument('--category', help='Filter by category')

    sp_import = sub.add_parser('import', help='Import expenses from CSV')
    sp_import.add_argument('--file', required=True)
    sp_import.add_argument('--header', action='store_true', help='CSV has header row')

    args = p.parse_args(argv)

    try:
        if args.cmd == 'init-db':
            init_db(force=args.force)
        elif args.cmd == 'add':
            if args.amount is None:
                amt = input('Amount: ').strip()
                amount = parse_amount(amt)
            else:
                amount = args.amount
            category = args.category or input('Category: ').strip()
            description = args.description or input('Description (optional): ').strip()
            created_at = parse_date(args.date) if args.date else None
            add_expense(amount, category, description, created_at)
            print('Expense added.')
        elif args.cmd == 'list':
            rows = query_expenses(limit=args.limit, since=args.since, until=args.until, category=args.category)
            print_rows(rows)
        elif args.cmd == 'summary':
            rows = summary_by_period(args.period, limit=args.limit)
            if HAS_RICH:
                table = Table(show_header=True, header_style='bold green')
                table.add_column('period')
                table.add_column('count', justify='right')
                table.add_column('total', justify='right')
                for r in rows:
                    table.add_row(str(r['period']), str(r['count']), str(r['total']))
                rprint(table)
            else:
                for r in rows:
                    print(f"{r['period']} | count={r['count']} total={r['total']}")
        elif args.cmd == 'category-report':
            rows = category_report(since=args.since, until=args.until, limit=args.limit)
            if HAS_RICH:
                table = Table(show_header=True, header_style='bold blue')
                table.add_column('category')
                table.add_column('count', justify='right')
                table.add_column('total', justify='right')
                for r in rows:
                    table.add_row(r['category'], str(r['cnt']), str(r['total']))
                rprint(table)
            else:
                for r in rows:
                    print(f"{r['category']:<30} count={r['cnt']} total={r['total']}")
        elif args.cmd == 'export':
            rows = query_expenses(limit=args.limit, since=args.since, until=args.until, category=args.category)
            export_to_csv(rows, args.file)
        elif args.cmd == 'import':
            import_from_csv(args.file, has_header=args.header)
        else:
            p.print_help()
    except Exception as e:
        logging.exception('Command failed: %s', e)
        sys.exit(1)

if __name__ == '__main__':
    main()
