import json
import os
import psycopg2


def handler(event: dict, context) -> dict:
    """Возвращает последние 50 логов выполнения правила (или всех правил)."""
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type'}, 'body': ''}

    params = event.get('queryStringParameters') or {}
    rule_id = params.get('rule_id')

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    if rule_id:
        cur.execute("""
            SELECT l.id, l.rule_id, r.name, l.status, l.result, l.error, l.started_at, l.finished_at
            FROM etl_run_logs l
            JOIN etl_rules r ON r.id = l.rule_id
            WHERE l.rule_id = %s
            ORDER BY l.started_at DESC
            LIMIT 50
        """ % int(rule_id))
    else:
        cur.execute("""
            SELECT l.id, l.rule_id, r.name, l.status, l.result, l.error, l.started_at, l.finished_at
            FROM etl_run_logs l
            JOIN etl_rules r ON r.id = l.rule_id
            ORDER BY l.started_at DESC
            LIMIT 50
        """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    logs = []
    for row in rows:
        duration_ms = None
        if row[6] and row[7]:
            duration_ms = int((row[7] - row[6]).total_seconds() * 1000)
        logs.append({
            'id': row[0],
            'rule_id': row[1],
            'rule_name': row[2],
            'status': row[3],
            'result': row[4],
            'error': row[5],
            'started_at': row[6].isoformat() if row[6] else None,
            'finished_at': row[7].isoformat() if row[7] else None,
            'duration_ms': duration_ms,
        })

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({'logs': logs})
    }
