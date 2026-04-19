import json
import os
import base64
import psycopg2
from datetime import datetime, timezone


SCHEMA = os.environ.get('MAIN_DB_SCHEMA', 't_p22417965_etl_engine_server')


def handler(event: dict, context) -> dict:
    """Запускает все активные ETL-правила из БД. Каждое правило получает {'data': body.data} в контексте exec. Результат пишется в etl_run_logs."""
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '86400'
            },
            'body': ''
        }

    body = json.loads(event.get('body') or '{}')
    data = body.get('data', {})

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, code_b64 FROM %s.etl_rules WHERE enabled = true ORDER BY id" % SCHEMA
    )
    rules = cur.fetchall()

    results = []

    for rule_id, rule_name, code_b64 in rules:
        started_at = datetime.now(timezone.utc)
        status = 'error'
        output = None
        error = None

        try:
            code = base64.b64decode(code_b64).decode('utf-8')
            local_vars = {}
            exec(code, {'__builtins__': __builtins__, 'data': data}, local_vars)
            output = local_vars.get('output', None)
            if output is not None:
                output = str(output)
            status = 'success'
        except Exception as e:
            error = str(e)

        finished_at = datetime.now(timezone.utc)
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)

        cur.execute(
            "INSERT INTO %s.etl_run_logs (rule_id, status, result, error, started_at, finished_at) VALUES (%s, '%s', %s, %s, '%s', '%s') RETURNING id" % (
                SCHEMA,
                rule_id,
                status,
                ("'%s'" % output.replace("'", "''")) if output else 'NULL',
                ("'%s'" % error.replace("'", "''")) if error else 'NULL',
                started_at.isoformat(),
                finished_at.isoformat()
            )
        )
        log_id = cur.fetchone()[0]
        conn.commit()

        results.append({
            'log_id': log_id,
            'rule_id': rule_id,
            'rule_name': rule_name,
            'status': status,
            'output': output,
            'error': error,
            'duration_ms': duration_ms
        })

    cur.close()
    conn.close()

    total = len(results)
    success = sum(1 for r in results if r['status'] == 'success')

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'total': total,
            'success': success,
            'failed': total - success,
            'results': results
        })
    }
