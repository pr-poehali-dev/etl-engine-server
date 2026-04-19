import json
import os
import psycopg2
from datetime import datetime, timezone


def handler(event: dict, context) -> dict:
    """Создаёт или обновляет ETL-правило в БД (name, description, code_b64, enabled)."""
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type'}, 'body': ''}

    body = json.loads(event.get('body') or '{}')
    rule_id = body.get('id')
    name = body.get('name', '').strip()
    description = body.get('description', '').strip()
    code_b64 = body.get('code_b64', '').strip()
    enabled = body.get('enabled', True)

    if not name or not code_b64:
        return {'statusCode': 400, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'name and code_b64 are required'})}

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    if rule_id:
        cur.execute(
            "UPDATE etl_rules SET name='%s', description=%s, code_b64='%s', enabled=%s, updated_at='%s' WHERE id=%s RETURNING id" % (
                name.replace("'", "''"),
                ("'%s'" % description.replace("'", "''")) if description else 'NULL',
                code_b64.replace("'", "''"),
                'true' if enabled else 'false',
                now,
                int(rule_id)
            )
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return {'statusCode': 404, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'Rule not found'})}
        result_id = row[0]
    else:
        cur.execute(
            "INSERT INTO etl_rules (name, description, code_b64, enabled, created_at, updated_at) VALUES ('%s', %s, '%s', %s, '%s', '%s') RETURNING id" % (
                name.replace("'", "''"),
                ("'%s'" % description.replace("'", "''")) if description else 'NULL',
                code_b64.replace("'", "''"),
                'true' if enabled else 'false',
                now,
                now
            )
        )
        result_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({'id': result_id, 'status': 'saved'})
    }
