import os
import json
import time
from decimal import Decimal
from datetime import date, datetime

from Inspector import Inspector

import pymysql


# ---------- Helpers for JSON-safe conversion ----------

def _convert_value(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _rows_to_dicts(cursor, rows):
    cols = [col[0] for col in cursor.description]
    result = []
    for row in rows:
        obj = {}
        for col, val in zip(cols, row):
            obj[col] = _convert_value(val)
        result.append(obj)
    return result


# ---------- Aurora connection helper ----------

def get_aurora_connection():
    host = os.environ["DB_HOST"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    db_name = os.environ.get("DB_NAME", "airbnb")

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        connect_timeout=5,
        cursorclass=pymysql.cursors.Cursor,
    )


# ---------- Individual query runners ----------

def run_summary_query(cursor):
    sql = """
        SELECT 
            COUNT(*)                  AS total_listings,
            AVG(price)                AS avg_price,
            MIN(price)                AS min_price,
            MAX(price)                AS max_price,
            AVG(review_scores_rating) AS avg_rating
        FROM listings_clean;
    """
    cursor.execute(sql)
    row = cursor.fetchone()
    cols = [col[0] for col in cursor.description]
    return {col: _convert_value(val) for col, val in zip(cols, row)}


def run_top_neighbourhoods_query(cursor, limit=15):
    sql = f"""
        SELECT 
            neighbourhood,
            COUNT(*)                  AS num_listings,
            AVG(price)                AS avg_price,
            AVG(review_scores_rating) AS avg_rating
        FROM listings_clean
        GROUP BY neighbourhood
        ORDER BY num_listings DESC
        LIMIT {int(limit)};
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    return _rows_to_dicts(cursor, rows)


def run_price_categories_query(cursor):
    sql = """
        SELECT
            price_category,
            COUNT(*) AS num_listings
        FROM listings_clean
        GROUP BY price_category
        ORDER BY num_listings DESC;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    return _rows_to_dicts(cursor, rows)


def run_room_type_breakdown_query(cursor):
    sql = """
        SELECT
            room_type_simplified,
            COUNT(*)                  AS num_listings,
            AVG(price)                AS avg_price,
            AVG(review_scores_rating) AS avg_rating
        FROM listings_clean
        GROUP BY room_type_simplified
        ORDER BY num_listings DESC;
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    return _rows_to_dicts(cursor, rows)


# ---------- Backend runners ----------

def run_queries_aurora(event):
    timings = {}
    t0 = time.time()

    t_connect_start = time.time()
    conn = get_aurora_connection()
    timings["connect"] = (time.time() - t_connect_start) * 1000.0

    try:
        with conn.cursor() as cursor:
            t_q1 = time.time()
            summary = run_summary_query(cursor)
            timings["summary"] = (time.time() - t_q1) * 1000.0

            t_q2 = time.time()
            top_neighbourhoods = run_top_neighbourhoods_query(cursor)
            timings["top_neighbourhoods"] = (time.time() - t_q2) * 1000.0

            t_q3 = time.time()
            price_categories = run_price_categories_query(cursor)
            timings["price_categories"] = (time.time() - t_q3) * 1000.0

            t_q4 = time.time()
            room_type_breakdown = run_room_type_breakdown_query(cursor)
            timings["room_type_breakdown"] = (time.time() - t_q4) * 1000.0

    finally:
        conn.close()

    timings["total"] = (time.time() - t0) * 1000.0

    return {
        "backend": "aurora",
        "summary": summary,
        "top_neighbourhoods": top_neighbourhoods,
        "price_categories": price_categories,
        "room_type_breakdown": room_type_breakdown,
        "timings_ms": {k: round(v, 2) for k, v in timings.items()},
    }


def run_queries_sqlite():
    """
    Placeholder for future benchmarking.

    Later, if you want to compare Aurora vs SQLite, you can:
    - Read the same cleaned CSV from S3 into a local SQLite DB, OR
    - Use an existing SQLite file on EFS.

    Implement that here, but keep the same return structure.
    """
    raise NotImplementedError("SQLite backend not implemented yet")


# ---------- Lambda handler ---------

from Inspector import Inspector

def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()

    try:
        result = run_queries_aurora(event)  # your dict result

        inspector.addAttribute("success", True)
        inspector.addAttribute("db_backend", event.get("db_backend", "aurora"))
        inspector.addAttribute("query_results", result)

    except Exception as e:
        inspector.addAttribute("success", False)
        inspector.addAttribute("errorMessage", str(e))

    # IMPORTANT: return SAAF object as the entire response
    return inspector.finish()


