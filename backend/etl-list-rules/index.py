import json
import os
import psycopg2


def handler(event: dict, context) -> dict:
    """Возвращает список всех ETL-правил из БД."""
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type'}, 'body': ''}

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    cur.execute("""
        SELECT r.id, r.name, r.description, r.code_b64, r.enabled, r.created_at,
               (SELECT COUNT(*) FROM etl_run_logs l WHERE l.rule_id = r.id) as run_count,
               (SELECT l.status FROM etl_run_logs l WHERE l.rule_id = r.id ORDER BY l.started_at DESC LIMIT 1) as last_status,
               (SELECT l.started_at FROM etl_run_logs l WHERE l.rule_id = r.id ORDER BY l.started_at DESC LIMIT 1) as last_run
        FROM etl_rules r
        ORDER BY r.id DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    rules = []
    for row in rows:
        rules.append({
            'id': row[0],
            'name': row[1],
            'description': row[2],
            'code_b64': row[3],
            'enabled': row[4],
            'created_at': row[5].isoformat() if row[5] else None,
            'run_count': row[6],
            'last_status': row[7],
            'last_run': row[8].isoformat() if row[8] else None,
        })

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({'rules': rules})
    }
