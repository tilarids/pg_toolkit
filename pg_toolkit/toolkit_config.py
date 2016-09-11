_BASE_DIR = None
_PG_CONN_STRING = None

def base_dir():
  if _BASE_DIR is None:
    raise RuntimeError('Please set base dir first using toolkit_config.set_base_dir.')
  return _BASE_DIR

def set_base_dir(d):
  global _BASE_DIR
  _BASE_DIR = d

def pg_conn_string():
  if _PG_CONN_STRING is None:
    raise RuntimeError('Please set conn string first using toolkit_config.set_pg_conn_string.')
  return _PG_CONN_STRING

def set_pg_conn_string(pg_conn_string):
  global _PG_CONN_STRING
  _PG_CONN_STRING = pg_conn_string
