import json
import os
import base64
import urllib.request
import urllib.error
import psycopg2
from datetime import datetime, timezone

SCHEMA = os.environ.get('MAIN_DB_SCHEMA', 't_p22417965_etl_engine_server')
CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
}


def auth_error():
    return {
        'statusCode': 401,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({'error': 'Unauthorized'}),
    }


def handler(event: dict, context) -> dict:
    """Запускает правило по rule_id: достаёт rule_code из etl_rules,
    отправляет POST на EXECUTOR_URL, ответ пишет в etl_run_logs.
    Требует заголовок Authorization: Bearer <ADMIN_TOKEN>."""

    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    # Auth
    auth_header = event.get('headers', {}).get('Authorization') or event.get('headers', {}).get('X-Authorization', '')
    admin_token = os.environ.get('ADMIN_TOKEN', '')
    if not admin_token or auth_header != 'Bearer ' + admin_token:
        return auth_error()

    body = json.loads(event.get('body') or '{}')
    rule_id = body.get('rule_id')
    extra_data = body.get('data', {})

    if not rule_id:
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'rule_id is required'}),
        }

    executor_url = os.environ.get('EXECUTOR_URL', '')
    if not executor_url:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'EXECUTOR_URL not configured'}),
        }

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, code_b64, enabled FROM %s.etl_rules WHERE id = %s" % (SCHEMA, int(rule_id))
    )
    rule = cur.fetchone()

    if not rule:
        cur.close()
        conn.close()
        return {
            'statusCode': 404,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Rule not found'}),
        }

    if not rule[3]:
        cur.close()
        conn.close()
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Rule is disabled'}),
        }

    rule_code = base64.b64decode(rule[2]).decode('utf-8')

    started_at = datetime.now(timezone.utc)
    status = 'error'
    result = None
    error = None
    executor_status = None

    try:
        payload = json.dumps({'code': rule_code, 'data': extra_data}).encode('utf-8')
        req = urllib.request.Request(
            executor_url,
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            executor_status = resp.status
            raw = resp.read().decode('utf-8')
            result = raw
            status = 'success'
    except urllib.error.HTTPError as e:
        executor_status = e.code
        error = 'HTTP %s: %s' % (e.code, e.read().decode('utf-8', errors='replace'))
    except Exception as e:
        error = str(e)

    finished_at = datetime.now(timezone.utc)
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)

    cur.execute(
        "INSERT INTO %s.etl_run_logs (rule_id, status, result, error, started_at, finished_at) VALUES (%s, '%s', %s, %s, '%s', '%s') RETURNING id" % (
            SCHEMA,
            rule[0],
            status,
            ("'%s'" % result.replace("'", "''")) if result else 'NULL',
            ("'%s'" % error.replace("'", "''")) if error else 'NULL',
            started_at.isoformat(),
            finished_at.isoformat(),
        )
    )
    log_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'},
        'body': json.dumps({
            'log_id': log_id,
            'rule_id': rule[0],
            'rule_name': rule[1],
            'status': status,
            'executor_status': executor_status,
            'result': result,
            'error': error,
            'duration_ms': duration_ms,
        }),
    }
