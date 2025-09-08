#!/usr/bin/env python3
"""
Expense Tracker CLI (Postgres)
"""

import os
import argparse
import logging
from decimal import Decimal
from datetime import datetime
import csv

import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Load .env automatically
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        # fallback to individual PG env vars
        dsn = {
            "host": os.getenv("PGHOST", "localhost"),
            "port": os.getenv("PGPORT", 5432),
            "database": os.getenv("PGDATABASE", "expense_tracker"),
            "user": os.getenv("PGUSER"),
            "password": os.getenv("PGPASSWORD"),
        }
    return psycopg2.connect(dsn, cursor_factory=extras.RealDictCursor)

def init_db(force=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE IF NOT EXISTS expenses (
                id SERIAL PRIMARY KEY,
                amount NUMERIC(10,2) NOT NULL CHECK (amount > 0),
                category VARCHAR(50) NOT NULL,
                description VARCHAR(200),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
            cur.execute(sql)
            conn.commit()
            logging.info("Initialized database and ensured table exists.")

def parse_amount(s):
    try:
        val = Decimal(s)
        if val <= 0:
            raise argparse.ArgumentTypeError("Amount must be positive. Example valid: 12.50")
        return val
    except Exception as e:
        raise argparse.ArgumentTypeError("Invalid amount format. Example valid: 12.50") from e

def add_expense(amount=None, category=None, description=None, created_at=None):
    # Prompt if missing
    if amount is None:
        amt = input("Amount: ")
        amount = parse_amount(amt)
    if category is None:
        category = input("Category: ").strip()
        if not category:
            raise ValueError("Category cannot be empty")
        if len(category) > 50:
            raise ValueError("Category too long (max 50 chars)")
    if description is None:
        description = input("Description (optional): ").strip()
        if len(description) > 200:
            description = description[:200]
    if created_at is None:
        created_at = datetime.now()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO expenses (amount, category, description, created_at) VALUES (%s,%s,%s,%s) RETURNING id",
                (amount, category, description, created_at)
            )
            row = cur.fetchone()
            if row:
                logging.info("Inserted expense id=%s", row["id"])
            else:
                logging.warning("Insert returned no row")
            conn.commit()
    print("Expense added.")

def query_expenses(limit=50, since=None, until=None, category=None):
    sql = "SELECT * FROM expenses WHERE 1=1"
    params = []

    if since:
        sql += " AND created_at >= %s"
        params.append(since)
    if until:
        sql += " AND created_at <= %s"
        params.append(until)
    if category:
        sql += " AND category = %s"
        params.append(category)
    sql += " ORDER BY created_at DESC"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

def print_expenses(rows):
    if not rows:
        print("No expenses found.")
        return
    from tabulate import tabulate
    table = [[r["id"], r["amount"], r["category"], r["description"], r["created_at"]] for r in rows]
    headers = ["id", "amount", "category", "description", "created_at"]
    print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

def summary():
    sql = """
    SELECT
        TO_CHAR(created_at, 'Mon-YYYY') AS month,
        COUNT(*) AS count,
        SUM(amount) AS total
    FROM expenses
    GROUP BY month
    ORDER BY MIN(created_at) ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not rows:
        print("No data for summary.")
        return
    from tabulate import tabulate
    table = [[r["month"], r["count"], r["total"]] for r in rows]
    headers = ["Month", "Count", "Total"]
    print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

def category_report():
    sql = """
    SELECT category, COUNT(*) AS count, SUM(amount) AS total
    FROM expenses
    GROUP BY category
    ORDER BY total DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not rows:
        print("No data for category report.")
        return
    from tabulate import tabulate
    table = [[r["category"], r["count"], r["total"]] for r in rows]
    headers = ["Category", "Count", "Total"]
    print(tabulate(table, headers=headers, tablefmt="fancy_grid"))

def export_csv(filename="expenses_export.csv", **filters):
    rows = query_expenses(**filters)
    if not rows:
        print("No data to export.")
        return
    keys = rows[0].keys()
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Exported {len(rows)} rows to {filename}")

def import_csv(filename="expenses_export.csv"):
    if not os.path.exists(filename):
        print(f"File {filename} does not exist")
        return
    with open(filename, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            add_expense(
                amount=Decimal(row["amount"]),
                category=row["category"],
                description=row.get("description"),
                created_at=row.get("created_at")
            )
    print("Import finished.")

def main():
    parser = argparse.ArgumentParser(description="Expense Tracker CLI (Postgres)")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init-db", help="Create the expenses table")
    subparsers.add_parser("add", help="Add an expense")
    subparsers.add_parser("list", help="List recent expenses")
    subparsers.add_parser("summary", help="Show summary by month")
    subparsers.add_parser("category-report", help="Totals grouped by category")
    subparsers.add_parser("export", help="Export query results to CSV")
    subparsers.add_parser("import", help="Import expenses from CSV")

    args = parser.parse_args()

    try:
        if args.command == "init-db":
            init_db()
        elif args.command == "add":
            add_expense()
        elif args.command == "list":
            rows = query_expenses()
            print_expenses(rows)
        elif args.command == "summary":
            summary()
        elif args.command == "category-report":
            category_report()
        elif args.command == "export":
            export_csv()
        elif args.command == "import":
            import_csv()
        else:
            parser.print_help()
    except Exception as e:
        logging.error("Command failed: %s", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
