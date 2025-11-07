# app.py
import os
import logging
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import requests
from requests.adapters import HTTPAdapter, Retry
from cachetools import TTLCache, cached

# === Config ===
OPEN_METEO_BASE = os.getenv("OPEN_METEO_BASE", "https://api.open-meteo.com/v1")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5"))  # seconds
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))  # seconds
CACHE_MAXSIZE = int(os.getenv("CACHE_MAXSIZE", "1024"))

# === Logging ===
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("weather-backend")

# === HTTP session with retries ===
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset(["GET"]))
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# === Cache for proxied responses ===
cache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL)

app = Flask(__name__)
CORS(app)

def _forward_get(path: str, params: dict):
    """
    Faz GET para Open-Meteo com session e retorna (status_code, json_or_text).
    """
    url = f"{OPEN_METEO_BASE}/{path.lstrip('/')}"
    logger.info("Forwarding request to Open-Meteo: %s params=%s", url, params)
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        # Aqui assumimos JSON; se não for JSON, retornamos texto bruto
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, resp.text
    except requests.exceptions.RequestException as e:
        logger.exception("Error fetching from Open-Meteo: %s", e)
        # Retorna 502 para o middleware
        return 502, {"error": "upstream_error", "detail": str(e)}


# === CACHING wrapper: chave baseada em path + sorted params ===
def _cache_key(path, params):
    # cria uma tupla ordenada estável de params -> chave
    items = tuple(sorted(params.items()))
    return f"{path}|{items}"


@cached(cache=cache, key=lambda path, params: _cache_key(path, params))
def _cached_forward(path, params):
    """Chamada com cache (TTL)"""
    return _forward_get(path, params)


# ===== Endpoints =====

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/weather", methods=["GET"])
def weather_proxy():
    """
    Proxy para /v1/forecast do Open-Meteo (exemplo).
    Espera ao menos lat e lon como query params, e encaminha o resto.
    """
    lat = request.args.get("latitude") or request.args.get("lat")
    lon = request.args.get("longitude") or request.args.get("lon") or request.args.get("long")

    if not lat or not lon:
        return jsonify({"error": "missing_parameters", "detail": "latitude and longitude required (lat/lon)"}), 400

    # Aceitamos e repassamos quaisquer params adicionais (ex: hourly, daily, timezone)
    forward_params = dict(request.args)
    # Normaliza nomes possíveis
    if "lat" in forward_params:
        forward_params["latitude"] = forward_params.pop("lat")
    if "lon" in forward_params:
        forward_params["longitude"] = forward_params.pop("lon")

    # Exemplo: usamos endpoint forecast
    status, data = _cached_forward("forecast", forward_params)
    return make_response(jsonify(data) if isinstance(data, dict) else data, status)


@app.route("/forecast", methods=["GET"])
def forecast_proxy():
    """
    Proxy genérico que deixa o middleware passar o 'endpoint' do Open-Meteo via query param.
    Parâmetro 'endpoint' indica o sufixo após /v1/ (ex: 'forecast', 'archive', etc).
    """
    endpoint = request.args.get("endpoint", "forecast")
    # removemos param endpoint antes de repassar
    forward_params = dict(request.args)
    forward_params.pop("endpoint", None)

    status, data = _cached_forward(endpoint, forward_params)
    return make_response(jsonify(data) if isinstance(data, dict) else data, status)


# Exemplo fácil de adicionar novo endpoint:
@app.route("/raw/<path:subpath>", methods=["GET"])
def raw_proxy(subpath: str):
    """
    Encaminha qualquer subpath para Open-Meteo (ex: /raw/forecast).
    Mantém todos query params.
    """
    forward_params = dict(request.args)
    status, data = _cached_forward(subpath, forward_params)
    return make_response(jsonify(data) if isinstance(data, dict) else data, status)


# ===== Error handlers =====
@app.errorhandler(500)
def internal_error(e):
    logger.exception("Internal server error: %s", e)
    return jsonify({"error": "internal_server_error", "detail": str(e)}), 500


if __name__ == "__main__":
    # Apenas para desenvolvimento (usar Gunicorn em produção)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=(os.getenv("FLASK_ENV") == "development"))
