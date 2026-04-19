import json
import os
import base64
import psycopg2
from datetime import datetime, timezone


def handler(event: dict, context) -> dict:
    """Запускает ETL-правило: декодирует base64-код и выполняет его через exec."""
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type'}, 'body': ''}

    body = json.loads(event.get('body') or '{}')
    rule_id = body.get('rule_id')

    if not rule_id:
        return {'statusCode': 400, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'rule_id required'})}

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    cur.execute("SELECT id, name, code_b64, enabled FROM etl_rules WHERE id = %s" % int(rule_id))
    rule = cur.fetchone()

    if not rule:
        cur.close()
        conn.close()
        return {'statusCode': 404, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'Rule not found'})}

    if not rule[3]:
        cur.close()
        conn.close()
        return {'statusCode': 400, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'Rule is disabled'})}

    started_at = datetime.now(timezone.utc)
    status = 'error'
    result = None
    error = None

    try:
        code = base64.b64decode(rule[2]).decode('utf-8')
        local_vars = {}
        exec(code, {'__builtins__': __builtins__}, local_vars)
        result = local_vars.get('output', 'OK')
        status = 'success'
    except Exception as e:
        error = str(e)
        status = 'error'

    finished_at = datetime.now(timezone.utc)

    cur.execute(
        "INSERT INTO etl_run_logs (rule_id, status, result, error, started_at, finished_at) VALUES (%s, '%s', %s, %s, '%s', '%s') RETURNING id" % (
            rule[0],
            status,
            ("'%s'" % str(result).replace("'", "''")) if result else 'NULL',
            ("'%s'" % str(error).replace("'", "''")) if error else 'NULL',
            started_at.isoformat(),
            finished_at.isoformat()
        )
    )
    log_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    duration_ms = int((finished_at - started_at).total_seconds() * 1000)

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({
            'log_id': log_id,
            'rule_id': rule[0],
            'rule_name': rule[1],
            'status': status,
            'result': result,
            'error': error,
            'duration_ms': duration_ms
        })
    }
