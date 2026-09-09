# coding: utf-8
"""BigQMT 客户端配置模板。

复制为同目录 ``bigqmt_signal_trader_client_config.py`` 后填写账号与传输参数。
真实配置含账号，已 gitignore，勿提交。
须与大 QMT 端 ``bigqmt_signal_trader_local_config.py`` 一致。
"""

BIGQMT_ACCOUNT_ID = "YOUR_ACCOUNT_ID"

BIGQMT_RPC_TIMEOUT_SECONDS = 30.0
BIGQMT_DOWNLOAD_WAIT_SECONDS = 1800
BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS = 0.5

BIGQMT_QUOTE_CLIENT_ID = None

BIGQMT_REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "username": "",
    "password": "",
    "protocol": 2,
    "transport": "redis",  # redis / zmq / mysql，须与 QMT 端一致
}

BIGQMT_FULL_TICK_CACHE_CONFIG = {
    "enabled": False,
    "demand_ttl_seconds": 10,
    "cache_ttl_seconds": 10,
    "wait_seconds": 3.5,
    "poll_interval_seconds": 0.2,
}

BIGQMT_LOCAL_CACHE_CONFIG = {
    "enabled": True,
    "dir": None,
    "fallback_rpc": False,
    "format": "auto",
}
