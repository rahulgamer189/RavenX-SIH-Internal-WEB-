from __future__ import annotations
import concurrent.futures, csv, gzip, io, json, math, os, re, sqlite3, threading, time, urllib.parse, urllib.request, zipfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from datetime import date, timedelta
import shapefile

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / 'static'
DB = ROOT / 'raven.db'
PORT = int(os.environ.get('PORT', '5173'))
REFRESH_SECONDS = int(os.environ.get('RAVEN_REFRESH_SECONDS', '900'))
DEFAULT_LAT, DEFAULT_LON = 19.17, 83.42
USER_AGENT = 'RAVEN/1.0 disaster-intelligence-demo'


def now(): return int(time.time())

def db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as con:
        con.executescript('''
        CREATE TABLE IF NOT EXISTS weather_snapshots(
          id INTEGER PRIMARY KEY, latitude REAL NOT NULL, longitude REAL NOT NULL,
          payload TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
          captured_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS cyclone_observations(
          id INTEGER PRIMARY KEY AUTOINCREMENT, storm_id TEXT NOT NULL, observed_at INTEGER NOT NULL,
          latitude REAL NOT NULL, longitude REAL NOT NULL, wind_kmh REAL, pressure_hpa REAL,
          source TEXT NOT NULL, raw_json TEXT DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS forecasts(
          id INTEGER PRIMARY KEY AUTOINCREMENT, generated_at INTEGER NOT NULL, status TEXT NOT NULL,
          payload TEXT NOT NULL, model TEXT NOT NULL, source TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS alerts(
          id INTEGER PRIMARY KEY AUTOINCREMENT, severity TEXT NOT NULL, title TEXT NOT NULL,
          message TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
          created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS shelters(
          id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, district TEXT NOT NULL,
          latitude REAL NOT NULL, longitude REAL NOT NULL, capacity INTEGER NOT NULL,
          source TEXT NOT NULL, updated_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS broadcasts(
          id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, body TEXT NOT NULL,
          severity TEXT NOT NULL, source TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS model_versions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, version TEXT NOT NULL,
          training_dataset TEXT NOT NULL, feature_description TEXT NOT NULL, status TEXT NOT NULL,
          created_at INTEGER NOT NULL, UNIQUE(name, version));
        CREATE TABLE IF NOT EXISTS satellite_observations(
          id INTEGER PRIMARY KEY AUTOINCREMENT, satellite TEXT NOT NULL, sensor TEXT NOT NULL,
          observed_at INTEGER NOT NULL, source_url TEXT NOT NULL, coverage_json TEXT NOT NULL,
          resolution_km REAL, channel TEXT, processing_status TEXT NOT NULL, quality_status TEXT NOT NULL,
          checksum TEXT, created_at INTEGER NOT NULL, UNIQUE(satellite, sensor, observed_at, channel));
        CREATE TABLE IF NOT EXISTS predictions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, storm_id TEXT NOT NULL, generated_at INTEGER NOT NULL,
          model_name TEXT NOT NULL, model_version TEXT NOT NULL, input_timestamp INTEGER NOT NULL,
          payload TEXT NOT NULL, confidence REAL NOT NULL, uncertainty_km REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS validation_metrics(
          id INTEGER PRIMARY KEY AUTOINCREMENT, model_name TEXT NOT NULL, model_version TEXT NOT NULL,
          dataset_version TEXT NOT NULL, split_name TEXT NOT NULL, metric_name TEXT NOT NULL,
          metric_value REAL NOT NULL, sample_count INTEGER NOT NULL, evaluated_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_obs_storm_time ON cyclone_observations(storm_id, observed_at);
        CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_snapshots(captured_at);
        CREATE INDEX IF NOT EXISTS idx_satellite_time ON satellite_observations(observed_at);
        CREATE INDEX IF NOT EXISTS idx_predictions_storm_time ON predictions(storm_id, generated_at);
        CREATE TABLE IF NOT EXISTS satellite_datasets(
          id INTEGER PRIMARY KEY AUTOINCREMENT, dataset_key TEXT NOT NULL UNIQUE, provider TEXT NOT NULL,
          platform TEXT NOT NULL, product TEXT NOT NULL, coverage TEXT NOT NULL, cadence TEXT NOT NULL,
          license_status TEXT NOT NULL, source_url TEXT NOT NULL, description TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_runs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER, dataset_id INTEGER, started_at INTEGER NOT NULL,
          completed_at INTEGER, status TEXT NOT NULL, current_stage TEXT NOT NULL, input_uri TEXT,
          qc_score REAL, detection_label TEXT, classification_label TEXT, confidence REAL,
          risk_level TEXT, track_json TEXT NOT NULL DEFAULT '{}', model_version TEXT, error_message TEXT,
          FOREIGN KEY(observation_id) REFERENCES satellite_observations(id), FOREIGN KEY(dataset_id) REFERENCES satellite_datasets(id));
        CREATE TABLE IF NOT EXISTS pipeline_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL,
          started_at INTEGER NOT NULL, completed_at INTEGER, message TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(run_id) REFERENCES pipeline_runs(id));
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);
        CREATE INDEX IF NOT EXISTS idx_pipeline_events_run ON pipeline_events(run_id, id);
        CREATE TABLE IF NOT EXISTS weather_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT, latitude REAL NOT NULL, longitude REAL NOT NULL,
          observation_date TEXT NOT NULL, temperature_c REAL, pressure_hpa REAL, wind_kmh REAL,
          wind_direction_deg REAL, humidity_percent REAL, precipitation_mm REAL, sea_surface_temperature_c REAL,
          source TEXT NOT NULL, source_url TEXT NOT NULL, fetched_at INTEGER NOT NULL, raw_json TEXT NOT NULL DEFAULT '{}',
          UNIQUE(latitude, longitude, observation_date));
        CREATE TABLE IF NOT EXISTS risk_predictions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, latitude REAL NOT NULL, longitude REAL NOT NULL,
          observation_start TEXT NOT NULL, observation_end TEXT NOT NULL, forecast_window_days INTEGER NOT NULL,
          risk_level TEXT NOT NULL, probability_estimate REAL NOT NULL, confidence_percent REAL NOT NULL,
          expected_period TEXT NOT NULL, model_name TEXT NOT NULL, model_version TEXT NOT NULL,
          features_json TEXT NOT NULL, source_urls_json TEXT NOT NULL, generated_at INTEGER NOT NULL,
          disclaimer TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_weather_history_location_date ON weather_history(latitude, longitude, observation_date);
        CREATE INDEX IF NOT EXISTS idx_risk_predictions_location_time ON risk_predictions(latitude, longitude, generated_at);
        CREATE TABLE IF NOT EXISTS ocean_grid_reports(
          id INTEGER PRIMARY KEY AUTOINCREMENT, report_key TEXT NOT NULL UNIQUE, start_date TEXT,
          end_date TEXT, points_json TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
          fetched_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_ocean_grid_reports_fetched ON ocean_grid_reports(fetched_at);
        ''')
        con.execute("INSERT OR IGNORE INTO model_versions(name,version,training_dataset,feature_description,status,created_at) VALUES(?,?,?,?,?,?)", ('ols-trajectory-baseline','0.1.0','Persisted verified cyclone observations','Time-indexed latitude, longitude, and wind speed; no satellite imagery features','baseline-not-validated',now()))
        if con.execute('SELECT COUNT(*) FROM shelters').fetchone()[0] == 0:
            con.executemany('INSERT INTO shelters(name,district,latitude,longitude,capacity,source,updated_at) VALUES(?,?,?,?,?,?,?)', [
              ('Gunupur Relief Centre', 'Rayagada', 19.171, 83.416, 300, 'District administration directory (seeded reference)', now()),
              ('Bhubaneswar Cyclone Shelter', 'Khordha', 20.296, 85.824, 500, 'District administration directory (seeded reference)', now()),
            ])
        satellite_datasets = [
          ('noaa-goes-nhc', 'NOAA / NESDIS', 'GOES-R Series', 'ABI visible/infrared/water vapor', 'Western Hemisphere', '5–15 minutes', 'reference-only', 'https://www.noaa.gov/jetstream/satellites', 'Geostationary imagery and derived products for tropical cyclone monitoring; pixel access requires an approved operational feed.'),
          ('jma-himawari', 'Japan Meteorological Agency', 'Himawari-8/9', 'AHI visible/infrared/water vapor', 'Asia-Pacific', '10 minutes', 'reference-only', 'https://www.data.jma.go.jp/mscweb/en/himawari89/', 'High-frequency Asia-Pacific geostationary imagery and metadata; RAVEN stores provenance until a licensed pixel feed is connected.'),
          ('eumetsat-meteosat', 'EUMETSAT', 'Meteosat', 'SEVIRI visible/infrared', 'Indian Ocean / Europe / Africa', '15 minutes', 'reference-only', 'https://www.eumetsat.int/', 'Meteosat imagery and derived storm products with provider-controlled access.'),
          ('isro-insat-mosdac', 'ISRO / MOSDAC', 'INSAT-3D/3DR', 'Imager visible/infrared', 'Indian Ocean / South Asia', '15 minutes', 'reference-only', 'https://www.mosdac.gov.in/', 'India-focused geostationary imagery and products for regional cyclone monitoring.'),
          ('nasa-earthdata', 'NASA Earthdata', 'EOS / JPSS / GPM', 'Multispectral / microwave / precipitation', 'Global', 'Variable', 'reference-only', 'https://earthdata.nasa.gov/', 'Research and operational Earth-observation archives; credentials and product licensing vary by collection.'),
          ('noaa-ibtracs', 'NOAA NCEI', 'IBTrACS v04r01', 'Historical best-track labels', 'Global', 'Archive', 'open-data', 'https://www.ncei.noaa.gov/products/international-best-track-archive', 'Historical storm tracks used as labels and validation context, not as satellite imagery.')
        ]
        con.executemany('INSERT OR IGNORE INTO satellite_datasets(dataset_key,provider,platform,product,coverage,cadence,license_status,source_url,description,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)', [row + (now(),) for row in satellite_datasets])
        if con.execute('SELECT COUNT(*) FROM broadcasts').fetchone()[0] == 0:
            con.execute('INSERT INTO broadcasts(title,body,severity,source,created_at) VALUES(?,?,?,?,?)', ('RAVEN service status', 'Forecasts are decision-support outputs. Follow IMD and local authority instructions for official warnings.', 'info', 'RAVEN system', now()))


def fetch_json(url, timeout=12):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


CYCLOCANE_ORIGIN = 'https://www.cyclocane.com'
NOAA_TROPICAL_SERVICE = 'https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/NHC_tropical_weather/MapServer'
NOAA_CURRENT_STORMS = 'https://www.nhc.noaa.gov/CurrentStorms.json'
_live_cyclone_cache = {'expires': 0, 'payload': None}
_live_cyclone_lock = threading.Lock()


def cyclocane_proxy(handler, target_path, query):
    target = CYCLOCANE_ORIGIN + ('/' if not target_path else '/' + target_path.lstrip('/'))
    if query: target += '?' + query
    req = urllib.request.Request(target, headers={'User-Agent':'Mozilla/5.0','Referer':CYCLOCANE_ORIGIN+'/' ,'Accept':'*/*'})
    with urllib.request.urlopen(req, timeout=30) as response:
        raw=response.read(); content_type=response.headers.get('Content-Type','application/octet-stream')
    if target_path in ('', '/') or target_path.endswith('.html'):
        text=raw.decode('utf-8','replace')
        text=text.replace('https://www.cyclocane.com/', '/cyclocane-proxy/')
        text=re.sub(r'(?P<attr>\b(?:src|href|action)=)(?P<quote>["\'])/(?!/)', r'\g<attr>\g<quote>/cyclocane-proxy/', text)
        text=text.replace('/cyclocane-proxy//cdnjs.cloudflare.com/', 'https://cdnjs.cloudflare.com/')
        raw=text.encode('utf-8'); content_type='text/html; charset=utf-8'
    handler.send_response(200); handler.send_header('Content-Type',content_type); handler.send_header('Cache-Control','no-store'); handler.send_header('Content-Length',str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


def flatten_noaa_layers(layers):
    result=[]
    for layer in layers:
        result.append(layer)
        result.extend(flatten_noaa_layers(layer.get('layers', [])))
    return result


def noaa_forecast_geojson():
    index=fetch_json(NOAA_TROPICAL_SERVICE + '/layers?f=pjson', timeout=20)
    point_layers=[x for x in flatten_noaa_layers(index.get('layers', [])) if 'forecast points' in x.get('name','').lower()]
    def load_layer(layer):
        query=urllib.parse.urlencode({'where':'1=1','outFields':'*','returnGeometry':'true','f':'geojson'})
        try:
            data=fetch_json(f"{NOAA_TROPICAL_SERVICE}/{layer['id']}/query?{query}", timeout=20)
            return data.get('features', [])
        except Exception:
            return []
    features=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for batch in pool.map(load_layer, point_layers): features.extend(batch)
    return {'type':'FeatureCollection','features':features,'source':'NOAA/NHC Tropical Weather GIS Forecast Points','source_url':NOAA_TROPICAL_SERVICE,'captured_at':now(),'data_status':'live'}


def live_cyclone_geojson():
    """Return current NHC storms plus their official five-day forecast points.

    This is the Cyclocane-style map feed: one current red point and an official
    forecast path per active storm. NOAA's published GIS archives are parsed
    server-side so the browser never depends on a CORS or API-key tile request.
    """
    with _live_cyclone_lock:
        if _live_cyclone_cache['payload'] and _live_cyclone_cache['expires'] > now():
            return _live_cyclone_cache['payload']
    active=fetch_json(NOAA_CURRENT_STORMS, timeout=15).get('activeStorms', [])
    features=[]
    for storm in active:
        name=storm.get('name') or 'Unnamed storm'
        current={'type':'Feature','geometry':{'type':'Point','coordinates':[storm.get('longitudeNumeric'),storm.get('latitudeNumeric')]},'properties':{
            'stormname':name,'stormtype':storm.get('classification') or 'Tropical cyclone','tcdvlp':storm.get('classification') or 'Active storm','tau':0,
            'maxwind':storm.get('intensity'),'pressure':storm.get('pressure'),'validtime':storm.get('lastUpdate'),'current':True,
            'source':'NOAA/NHC CurrentStorms.json'}}
        if current['geometry']['coordinates'][0] is not None and current['geometry']['coordinates'][1] is not None: features.append(current)
        archive=(storm.get('forecastTrack') or {}).get('zipFile')
        if not archive: continue
        try:
            raw=urllib.request.urlopen(urllib.request.Request(archive,headers={'User-Agent':USER_AGENT}),timeout=20).read()
            z=zipfile.ZipFile(io.BytesIO(raw)); stem=next(n[:-4] for n in z.namelist() if n.endswith('_5day_pts.shp'))
            reader=shapefile.Reader(shp=io.BytesIO(z.read(stem+'.shp')),shx=io.BytesIO(z.read(stem+'.shx')),dbf=io.BytesIO(z.read(stem+'.dbf')))
            fields=[f[0] for f in reader.fields[1:]]
            for record in reader.iterShapeRecords():
                values=dict(zip(fields,list(record.record))); point=record.shape.points[0]
                features.append({'type':'Feature','geometry':{'type':'Point','coordinates':[point[0],point[1]]},'properties':{
                    'stormname':values.get('STORMNAME') or name,'stormtype':values.get('STORMTYPE') or values.get('TCDVLP') or 'Tropical cyclone','tcdvlp':values.get('TCDVLP'),
                    'tau':values.get('TAU',0),'maxwind':values.get('MAXWIND'),'pressure':values.get('MSLP'),'validtime':values.get('FLDATELBL') or values.get('VALIDTIME'),
                    'current':False,'source':'NOAA/NHC official five-day forecast GIS'}})
        except Exception:
            continue
    payload={'type':'FeatureCollection','features':features,'source':'NOAA/NHC CurrentStorms + official forecast GIS','source_url':NOAA_CURRENT_STORMS,'captured_at':now(),'data_status':'live'}
    with _live_cyclone_lock:
        _live_cyclone_cache.update({'expires':now()+600,'payload':payload})
    return payload


def validate_location(lat, lon):
    lat, lon = float(lat), float(lon)
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError('latitude or longitude is outside valid geographic ranges')
    return round(lat, 4), round(lon, 4)


def weather_history_sources(lat, lon, start_date, end_date):
    weather_query = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'start_date': start_date, 'end_date': end_date, 'daily': 'temperature_2m_mean,relative_humidity_2m_mean,precipitation_sum,wind_speed_10m_max,wind_direction_10m_dominant,surface_pressure_mean', 'timezone': 'UTC'})
    marine_query = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'start_date': start_date, 'end_date': end_date, 'daily': 'sea_surface_temperature_mean', 'timezone': 'UTC'})
    return 'https://archive-api.open-meteo.com/v1/archive?' + weather_query, 'https://marine-api.open-meteo.com/v1/marine?' + marine_query


def fetch_weather_history(lat=DEFAULT_LAT, lon=DEFAULT_LON):
    lat, lon = validate_location(lat, lon)
    end_date = date.today()
    start_date = end_date - timedelta(days=29)
    start, end = start_date.isoformat(), end_date.isoformat()
    weather_url, marine_url = weather_history_sources(lat, lon, start, end)
    weather_error = None
    marine_error = None
    try:
        weather_data = fetch_json(weather_url, timeout=35)
    except Exception as exc:
        weather_data = {}
        weather_error = str(exc)
    try:
        marine_data = fetch_json(marine_url, timeout=35)
    except Exception as exc:
        marine_data = {}
        marine_error = str(exc)
    daily = weather_data.get('daily') or {}
    dates = daily.get('time') or []
    if not dates:
        raise RuntimeError('The weather archive returned no daily records for the selected location and period.' + (f' {weather_error}' if weather_error else ''))
    marine_daily = marine_data.get('daily') or {}
    marine_dates = marine_daily.get('time') or []
    marine_sst = dict(zip(marine_dates, marine_daily.get('sea_surface_temperature_mean') or []))
    rows = []
    fetched = now()
    keys = {
        'temperature_c': 'temperature_2m_mean', 'humidity_percent': 'relative_humidity_2m_mean',
        'precipitation_mm': 'precipitation_sum', 'wind_kmh': 'wind_speed_10m_max',
        'wind_direction_deg': 'wind_direction_10m_dominant', 'pressure_hpa': 'surface_pressure_mean'
    }
    for i, day in enumerate(dates):
        row = {name: (daily.get(key) or [None] * len(dates))[i] for name, key in keys.items()}
        row.update({'date': day, 'sea_surface_temperature_c': marine_sst.get(day)})
        rows.append(row)
    with db() as con:
        for row in rows:
            con.execute('INSERT INTO weather_history(latitude,longitude,observation_date,temperature_c,pressure_hpa,wind_kmh,wind_direction_deg,humidity_percent,precipitation_mm,sea_surface_temperature_c,source,source_url,fetched_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(latitude,longitude,observation_date) DO UPDATE SET temperature_c=excluded.temperature_c,pressure_hpa=excluded.pressure_hpa,wind_kmh=excluded.wind_kmh,wind_direction_deg=excluded.wind_direction_deg,humidity_percent=excluded.humidity_percent,precipitation_mm=excluded.precipitation_mm,sea_surface_temperature_c=excluded.sea_surface_temperature_c,source=excluded.source,source_url=excluded.source_url,fetched_at=excluded.fetched_at,raw_json=excluded.raw_json', (lat, lon, row['date'], row['temperature_c'], row['pressure_hpa'], row['wind_kmh'], row['wind_direction_deg'], row['humidity_percent'], row['precipitation_mm'], row['sea_surface_temperature_c'], 'Open-Meteo Historical Archive + Marine API', weather_url, fetched, json.dumps(row)))
    return {'status': 'available', 'location': {'latitude': lat, 'longitude': lon}, 'period': {'start': start, 'end': end, 'days': len(rows)}, 'records': rows, 'source': 'Open-Meteo Historical Archive', 'source_url': weather_url, 'marine_source': 'Open-Meteo Marine API', 'marine_source_url': marine_url, 'fetched_at': fetched, 'marine_status': 'live' if marine_sst else 'unavailable', 'marine_error': marine_error, 'weather_error': weather_error, 'data_status': 'live'}


def stored_weather_history(lat, lon):
    lat, lon = validate_location(lat, lon)
    with db() as con:
        rows = [dict(r) for r in con.execute('SELECT observation_date AS date,temperature_c,pressure_hpa,wind_kmh,wind_direction_deg,humidity_percent,precipitation_mm,sea_surface_temperature_c,source,source_url,fetched_at FROM weather_history WHERE latitude=? AND longitude=? ORDER BY observation_date', (lat, lon))]
    return rows


def cyclone_risk_prediction(lat=DEFAULT_LAT, lon=DEFAULT_LON):
    lat, lon = validate_location(lat, lon)
    history = fetch_weather_history(lat, lon)
    rows = history['records']
    valid = lambda key: [float(r[key]) for r in rows if r.get(key) is not None]
    pressure = valid('pressure_hpa'); wind = valid('wind_kmh'); humidity = valid('humidity_percent'); rain = valid('precipitation_mm'); sst = valid('sea_surface_temperature_c')
    pressure_drop = max(0.0, (pressure[0] - pressure[-1])) if len(pressure) >= 2 else 0.0
    wind_max = max(wind) if wind else 0.0
    wind_rise = max(0.0, wind[-1] - wind[0]) if len(wind) >= 2 else 0.0
    humidity_mean = sum(humidity) / len(humidity) if humidity else 0.0
    rain_total = sum(rain) if rain else 0.0
    sst_mean = sum(sst) / len(sst) if sst else None
    # Transparent, deterministic baseline. This is a probability estimate, not a validated climatological probability.
    score = -5.1 + min(1.8, pressure_drop / 12.0) + min(1.4, wind_max / 55.0) + min(0.8, wind_rise / 25.0) + (0.7 if humidity_mean >= 78 else 0.0) + min(0.6, rain_total / 240.0) + (0.9 if sst_mean is not None and sst_mean >= 26.5 else 0.0)
    probability = 1.0 / (1.0 + math.exp(-score))
    if probability >= 0.65: risk = 'high'
    elif probability >= 0.35: risk = 'moderate'
    else: risk = 'low'
    confidence = min(72.0, 35.0 + len(rows) * 1.1 + (8 if sst else 0))
    expected = f'{date.today().isoformat()} to {(date.today() + timedelta(days=7)).isoformat()}'
    features = {'pressure_drop_hpa': round(pressure_drop, 2), 'maximum_wind_kmh': round(wind_max, 2), 'wind_rise_kmh': round(wind_rise, 2), 'mean_humidity_percent': round(humidity_mean, 2), 'rain_total_mm': round(rain_total, 2), 'mean_sst_c': round(sst_mean, 2) if sst_mean is not None else None, 'record_count': len(rows)}
    source_urls = [history['source_url'], history['marine_source_url']]
    disclaimer = 'AI/ML-style baseline estimate from the previous 30 days of retrieved weather data. It is not an official meteorological warning, cyclone formation confirmation, or landfall prediction. Verify with IMD, NOAA, and local authorities.'
    result = {'status': 'available', 'location': history['location'], 'observation_period': history['period'], 'forecast_window_days': 7, 'risk_level': risk, 'probability_estimate_percent': round(probability * 100, 1), 'confidence_percent': round(confidence, 1), 'expected_period': expected, 'model': {'name': 'CycloneRiskBaseline', 'version': '0.1.0', 'type': 'transparent feature-scored ML baseline', 'features': features}, 'sources': [{'name': history['source'], 'type': 'daily weather archive', 'url': history['source_url'], 'period': history['period'], 'fetched_at': history['fetched_at']}, {'name': history['marine_source'], 'type': 'daily sea-surface temperature where available', 'url': history['marine_source_url'], 'period': history['period'], 'fetched_at': history['fetched_at'], 'status': history['marine_status']}], 'generated_at': now(), 'disclaimer': disclaimer}
    with db() as con:
        con.execute('INSERT INTO risk_predictions(latitude,longitude,observation_start,observation_end,forecast_window_days,risk_level,probability_estimate,confidence_percent,expected_period,model_name,model_version,features_json,source_urls_json,generated_at,disclaimer) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (lat, lon, history['period']['start'], history['period']['end'], 7, risk, probability * 100, confidence, expected, 'CycloneRiskBaseline', '0.1.0', json.dumps(features), json.dumps(source_urls), result['generated_at'], disclaimer))
    return result


def open_meteo(lat=DEFAULT_LAT, lon=DEFAULT_LON):
    q = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'current': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,surface_pressure', 'hourly': 'precipitation,wind_speed_10m,pressure_msl', 'forecast_days': 2, 'timezone': 'auto'})
    weather = fetch_json('https://api.open-meteo.com/v1/forecast?' + q)
    mq = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'current': 'wave_height,sea_surface_temperature', 'hourly': 'wave_height,sea_surface_temperature', 'forecast_days': 2, 'timezone': 'auto'})
    marine = fetch_json('https://marine-api.open-meteo.com/v1/marine?' + mq)
    c = weather.get('current', {})
    mc = marine.get('current', {})
    payload = {
      'location': {'latitude': lat, 'longitude': lon},
      'weather': {'temperature_c': c.get('temperature_2m'), 'humidity_percent': c.get('relative_humidity_2m'), 'rainfall_mm': c.get('precipitation'), 'wind_kmh': c.get('wind_speed_10m'), 'wind_direction_deg': c.get('wind_direction_10m'), 'pressure_hpa': c.get('surface_pressure')},
      'ocean': {'wave_height_m': mc.get('wave_height'), 'sea_surface_temperature_c': mc.get('sea_surface_temperature'), 'storm_surge_risk': 'Not provided by source; no value inferred'},
      'forecast_hours': [{'time': t, 'rainfall_mm': r, 'wind_kmh': w, 'pressure_hpa': p} for t,r,w,p in zip(weather.get('hourly',{}).get('time',[])[:24], weather.get('hourly',{}).get('precipitation',[])[:24], weather.get('hourly',{}).get('wind_speed_10m',[])[:24], weather.get('hourly',{}).get('pressure_msl',[])[:24])],
      'source': 'Open-Meteo', 'source_url': 'https://open-meteo.com/', 'captured_at': now(), 'data_status': 'live'
    }
    return payload


def refresh_weather():
    try:
        payload = open_meteo()
        with db() as con:
            con.execute('INSERT INTO weather_snapshots(latitude,longitude,payload,source,status,captured_at) VALUES(?,?,?,?,?,?)', (DEFAULT_LAT, DEFAULT_LON, json.dumps(payload), 'Open-Meteo', 'live', now()))
        return payload
    except Exception as exc:
        with db() as con:
            row = con.execute('SELECT payload FROM weather_snapshots ORDER BY captured_at DESC LIMIT 1').fetchone()
        if row:
            payload = json.loads(row['payload']); payload['data_status'] = 'cached'; payload['error'] = str(exc)
            return payload
        return {'data_status': 'unavailable', 'error': str(exc), 'source': 'Open-Meteo'}


def ingest_noaa():
    # NOAA's active-storm feed is a verified public source. It may not contain Indian Ocean systems.
    try:
        data = fetch_json('https://www.nhc.noaa.gov/CurrentStorms.json', timeout=10)
        count = 0
        for storm in data.get('activeStorms', []):
            lat = storm.get('latitudeNumeric'); lon = storm.get('longitudeNumeric')
            if lat is None or lon is None: continue
            with db() as con:
                con.execute('INSERT INTO cyclone_observations(storm_id,observed_at,latitude,longitude,wind_kmh,pressure_hpa,source,raw_json) VALUES(?,?,?,?,?,?,?,?)', (str(storm.get('id') or storm.get('name') or 'NOAA-storm'), now(), float(lat), float(lon), float(storm.get('maxWindMPH') or 0)*1.60934, None, 'NOAA/NHC CurrentStorms', json.dumps(storm)))
            count += 1
        return count
    except Exception:
        return 0


def marine_at(lat, lon):
    q = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'current': 'wave_height,sea_surface_temperature', 'forecast_days': 1, 'timezone': 'UTC'})
    try:
        data = fetch_json('https://marine-api.open-meteo.com/v1/marine?' + q, timeout=10)
        current = data.get('current', {})
        return {'sea_surface_temperature_c': current.get('sea_surface_temperature'), 'wave_height_m': current.get('wave_height'), 'source': 'Open-Meteo Marine', 'source_url': 'https://open-meteo.com/en/docs/marine-weather-api', 'captured_at': now(), 'data_status': 'live'}
    except Exception as exc:
        return {'sea_surface_temperature_c': None, 'wave_height_m': None, 'source': 'Open-Meteo Marine', 'source_url': 'https://open-meteo.com/en/docs/marine-weather-api', 'captured_at': now(), 'data_status': 'unavailable', 'error': str(exc)}


def ocean_grid_points():
    return [(lat, lon) for lat in range(-60, 61, 20) for lon in range(-180, 181, 20)]


def _marine_value(values, mode='last'):
    valid = [float(x) for x in (values or []) if x is not None]
    if not valid: return None
    return max(valid) if mode == 'max' else valid[-1]


def ocean_grid_report(start_date=None, end_date=None, force=False):
    """Return the 7x19 Open-Meteo Marine field and persist the exact report payload."""
    today = date.today()
    if start_date or end_date:
        start_date = start_date or end_date
        end_date = end_date or start_date
        try:
            start = date.fromisoformat(start_date); end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise ValueError('Dates must use YYYY-MM-DD format') from exc
        if start > end: raise ValueError('start_date must be on or before end_date')
        if (end - start).days > 7: raise ValueError('Historical Marine requests are limited to 7 days')
        if end > today: raise ValueError('Historical Marine reports cannot be in the future')
        key = f'{start_date}:{end_date}'
        historical = True
    else:
        start_date = end_date = today.isoformat(); key = 'live'; historical = False
    if not force and historical:
        with db() as con:
            row = con.execute('SELECT * FROM ocean_grid_reports WHERE report_key=?', (key,)).fetchone()
        if row:
            return {'status':row['status'], 'mode':'historical', 'start_date':row['start_date'], 'end_date':row['end_date'], 'points':json.loads(row['points_json']), 'count':len(json.loads(row['points_json'])), 'source':row['source'], 'source_url':'https://open-meteo.com/en/docs/marine-weather-api', 'fetched_at':row['fetched_at'], 'cached':True, 'disclaimer':'Historical Marine requests are limited to 7 days.'}
    coords = ocean_grid_points()
    params = {'latitude':','.join(str(x[0]) for x in coords), 'longitude':','.join(str(x[1]) for x in coords), 'timezone':'UTC'}
    if historical: params.update({'hourly':'wave_height,swell_wave_height,sea_surface_temperature','start_date':start_date,'end_date':end_date})
    else: params.update({'current':'wave_height,swell_wave_height,sea_surface_temperature'})
    url = 'https://marine-api.open-meteo.com/v1/marine?' + urllib.parse.urlencode(params)
    try:
        raw = fetch_json(url, timeout=55)
        rows = raw if isinstance(raw, list) else [raw]
        points=[]
        for i, (lat, lon) in enumerate(coords):
            item = rows[i] if i < len(rows) else {}
            if historical:
                hourly=item.get('hourly',{}); wave=_marine_value(hourly.get('wave_height'), 'max'); swell=_marine_value(hourly.get('swell_wave_height'), 'max'); sst=_marine_value(hourly.get('sea_surface_temperature'))
            else:
                current=item.get('current',{}); wave=current.get('wave_height'); swell=current.get('swell_wave_height'); sst=current.get('sea_surface_temperature')
            points.append({'latitude':lat,'longitude':lon,'wave_height_m':wave,'swell_height_m':swell,'sea_surface_temperature_c':sst,'status':'available' if any(v is not None for v in (wave,swell,sst)) else 'unavailable'})
        status='available' if any(p['status']=='available' for p in points) else 'unavailable'
        fetched=now()
        with db() as con:
            con.execute('INSERT INTO ocean_grid_reports(report_key,start_date,end_date,points_json,source,status,fetched_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(report_key) DO UPDATE SET points_json=excluded.points_json,source=excluded.source,status=excluded.status,fetched_at=excluded.fetched_at', (key,start_date,end_date,json.dumps(points),'Open-Meteo Marine',status,fetched))
        return {'status':status,'mode':'historical' if historical else 'live','start_date':start_date,'end_date':end_date,'points':points,'count':len(points),'source':'Open-Meteo Marine','source_url':'https://open-meteo.com/en/docs/marine-weather-api','fetched_at':fetched,'cached':False,'request_url':url,'disclaimer':'Historical Marine requests are limited to 7 days. Land cells may be unavailable; no values are inferred.'}
    except Exception as exc:
        with db() as con:
            row=con.execute('SELECT * FROM ocean_grid_reports WHERE report_key=? ORDER BY fetched_at DESC LIMIT 1', (key,)).fetchone()
        if row:
            points=json.loads(row['points_json']); return {'status':'cached','mode':'historical' if historical else 'live','start_date':row['start_date'],'end_date':row['end_date'],'points':points,'count':len(points),'source':row['source'],'source_url':'https://open-meteo.com/en/docs/marine-weather-api','fetched_at':row['fetched_at'],'cached':True,'error':str(exc),'disclaimer':'Live source unavailable; showing the last stored report.'}
        raise


def ocean_context(ocean):
    sst = ocean.get('sea_surface_temperature_c')
    wave = ocean.get('wave_height_m')
    if sst is None:
        return {'classification': 'unavailable', 'explanation': 'No live sea-surface temperature was returned; no ocean adjustment is applied.', 'adjustment_percent': 0}
    if sst >= 28:
        classification, adjustment = 'warm-supportive-context', 3
    elif sst >= 26:
        classification, adjustment = 'neutral-context', 0
    else:
        classification, adjustment = 'cooler-context', -3
    return {'classification': classification, 'explanation': f'Live SST {sst}°C and wave height {wave if wave is not None else "unavailable"}m are included as ocean-context features. The small adjustment is an unvalidated context heuristic, not an official intensity forecast.', 'adjustment_percent': adjustment}

def ols(values, targets):
    # Ordinary least squares y = a + b*t; enough for a transparent baseline trajectory model.
    n = len(values); mean_x = sum(values)/n; mean_y = sum(targets)/n
    den = sum((x-mean_x)**2 for x in values)
    b = sum((x-mean_x)*(y-mean_y) for x,y in zip(values,targets))/den if den else 0.0
    return mean_y - b*mean_x, b


def build_forecast():
    with db() as con:
        storms = [r['storm_id'] for r in con.execute('SELECT storm_id FROM cyclone_observations GROUP BY storm_id ORDER BY COUNT(*) DESC, MAX(observed_at) DESC')]
        if not storms:
            payload = {'status':'unavailable','label':'AI-generated forecast unavailable','message':'No verified cyclone observations are currently available from configured sources. No track or intensity is inferred.','confidence_percent':0,'sources':['NOAA/NHC CurrentStorms','IMD RSMC New Delhi bulletins when configured'],'generated_at':now()}
            con.execute('INSERT INTO forecasts(generated_at,status,payload,model,source) VALUES(?,?,?,?,?)',(now(),'unavailable',json.dumps(payload),'OLS trajectory baseline','No observation'))
            return payload
        storm = storms[0]
        rows = con.execute('SELECT * FROM cyclone_observations WHERE storm_id=? ORDER BY observed_at', (storm,)).fetchall()
    if len(rows) < 2:
        return {'status':'insufficient_data','label':'AI-generated forecast pending more observations','message':'At least two verified observations are required before a track is extrapolated.','confidence_percent':0,'sources':sorted({r['source'] for r in rows}),'generated_at':now()}
    t0 = rows[0]['observed_at']; xs = [(r['observed_at']-t0)/3600 for r in rows]
    la, lb = ols(xs, [r['latitude'] for r in rows]); loa, lob = ols(xs, [r['longitude'] for r in rows])
    winds = [r['wind_kmh'] for r in rows if r['wind_kmh'] is not None]
    wa, wb = ols(xs[:len(winds)], winds) if len(winds)>1 else (winds[0] if winds else 0, 0)
    ocean = marine_at(rows[-1]['latitude'], rows[-1]['longitude'])
    ocean_signal = ocean_context(ocean)
    ocean_factor = 1 + ocean_signal['adjustment_percent'] / 100
    track=[]
    for horizon in (24,48,72,96,120):
        lat, lon = la+lb*horizon, loa+lob*horizon
        wind = max(0, (wa+wb*horizon) * ocean_factor)
        track.append({'hours_ahead':horizon,'timestamp':t0+horizon*3600,'latitude':round(lat,3),'longitude':round(lon,3),'wind_kmh':round(wind,1),'uncertainty_km':round(40+horizon*2.2,1),'confidence_percent':max(20,round(82-horizon*.35))})
    payload={'status':'available','label':'AI-generated ocean-informed forecast','model':'OLS trajectory and intensity baseline with live marine context','model_version':'0.2.0-ocean-context','training_dataset':'persisted verified cyclone observations','features_used':['observed latitude/longitude','observed wind speed','live sea-surface temperature','live wave height'],'ocean_context':{'observation':ocean,'interpretation':ocean_signal},'current_position':{'latitude':rows[-1]['latitude'],'longitude':rows[-1]['longitude'],'observed_at':rows[-1]['observed_at']},'predicted_track':track,'intensity_trend':'increasing' if wb>0.2 or ocean_signal['adjustment_percent']>0 else 'decreasing' if wb<-0.2 or ocean_signal['adjustment_percent']<0 else 'stable','confidence_percent':track[0]['confidence_percent'],'sources':sorted({r['source'] for r in rows}) + [ocean['source']], 'generated_at':now(),'disclaimer':'Decision-support estimate, not an official warning. Ocean context is live but the adjustment is not a validated physical intensity model. Verify with IMD and local authorities.'}
    with db() as con:
        con.execute('INSERT INTO forecasts(generated_at,status,payload,model,source) VALUES(?,?,?,?,?)',(now(),'available',json.dumps(payload),payload['model'],','.join(payload['sources'])))
    return payload


def validate_observation(data):
    errors=[]
    try: lat=float(data['latitude']); lon=float(data['longitude'])
    except (KeyError, TypeError, ValueError): return ['latitude and longitude must be numeric']
    if not -90 <= lat <= 90 or not -180 <= lon <= 180: errors.append('coordinates outside valid geographic ranges')
    observed=int(data.get('observed_at', now()))
    if observed > now()+86400 or observed < 0: errors.append('invalid observation timestamp')
    for key, low, high in (('wind_kmh',0,400),('pressure_hpa',800,1100),('resolution_km',0,1000)):
        if data.get(key) is not None:
            try:
                if not low <= float(data[key]) <= high: errors.append(f'{key} outside accepted range')
            except (TypeError, ValueError): errors.append(f'{key} must be numeric')
    return errors


def satellite_catalog():
    return [
      {'name':'NOAA GOES / NHC','channels':['visible','infrared','water vapor'],'status':'reference-only','url':'https://www.noaa.gov/jetstream/satellites'},
      {'name':'JMA Himawari','channels':['visible','infrared','water vapor'],'status':'reference-only','url':'https://www.data.jma.go.jp/mscweb/en/himawari89/'},
      {'name':'EUMETSAT Meteosat','channels':['visible','infrared','water vapor'],'status':'reference-only','url':'https://www.eumetsat.int/'},
      {'name':'ISRO INSAT / MOSDAC','channels':['visible','infrared'],'status':'reference-only','url':'https://www.mosdac.gov.in/'},
      {'name':'NASA Earthdata','channels':['multispectral','microwave'],'status':'reference-only','url':'https://earthdata.nasa.gov/'},
    ]


PIPELINE_STAGES = [
    ('ingestion', 'DATA INGESTION'),
    ('qc', 'PREPROCESSING / QC'),
    ('dataset', 'SATELLITE IMAGE DATASET'),
    ('model', 'AI / ML MODEL'),
    ('identification', 'IDENTIFICATION'),
    ('classification', 'CLASSIFICATION'),
    ('prediction', 'PREDICTION'),
    ('risk_track', 'RISK / TRACK OUTPUT'),
    ('map', 'RAVEN MAP'),
]


def satellite_pipeline_overview():
    with db() as con:
        datasets = [dict(r) for r in con.execute('SELECT * FROM satellite_datasets ORDER BY provider, platform')]
        runs = [dict(r) for r in con.execute('SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 10')]
        for run in runs:
            run['track_json'] = json.loads(run.get('track_json') or '{}')
            run['events'] = [dict(e) for e in con.execute('SELECT * FROM pipeline_events WHERE run_id=? ORDER BY id', (run['id'],))]
            for event in run['events']:
                event['metadata_json'] = json.loads(event.get('metadata_json') or '{}')
        obs_count = con.execute('SELECT COUNT(*) AS n FROM satellite_observations').fetchone()['n']
        accepted_count = con.execute('SELECT COUNT(*) AS n FROM satellite_observations WHERE quality_status="accepted"').fetchone()['n']
    return {'status': 'available', 'stages': [{'key': key, 'label': label} for key, label in PIPELINE_STAGES], 'datasets': datasets, 'runs': runs, 'counts': {'datasets': len(datasets), 'observations': obs_count, 'quality_accepted': accepted_count, 'pipeline_runs': len(runs)}, 'truthfulness': 'RAVEN records metadata and provenance for every source. It does not claim image detection or classification until licensed pixels and a validated image model are connected.'}


def _pipeline_event(con, run_id, stage, status, message, metadata=None):
    ts = now()
    con.execute('INSERT INTO pipeline_events(run_id,stage,status,started_at,completed_at,message,metadata_json) VALUES(?,?,?,?,?,?,?)', (run_id, stage, status, ts, ts, message, json.dumps(metadata or {})))


def run_satellite_pipeline(observation_id=None):
    with db() as con:
        obs = con.execute('SELECT * FROM satellite_observations WHERE id=?', (observation_id,)).fetchone() if observation_id else con.execute('SELECT * FROM satellite_observations ORDER BY observed_at DESC LIMIT 1').fetchone()
        if not obs:
            raise ValueError('No satellite observation is available. POST /api/satellite/observations first.')
        dataset = con.execute('SELECT * FROM satellite_datasets WHERE lower(dataset_key)=lower(?) OR lower(provider) LIKE lower(?) LIMIT 1', (str(obs['satellite']).replace(' ', '-'), '%' + str(obs['satellite']) + '%')).fetchone()
        if not dataset:
            dataset = con.execute('SELECT * FROM satellite_datasets WHERE lower(provider) LIKE lower(?) LIMIT 1', ('%' + str(obs['satellite']) + '%',)).fetchone()
        if not dataset:
            dataset = con.execute('SELECT * FROM satellite_datasets ORDER BY id LIMIT 1').fetchone()
        started = now()
        cur = con.execute('INSERT INTO pipeline_runs(observation_id,dataset_id,started_at,status,current_stage,input_uri,model_version) VALUES(?,?,?,?,?,?,?)', (obs['id'], dataset['id'] if dataset else None, started, 'running', 'ingestion', obs['source_url'], 'ols-trajectory-baseline-0.2.0'))
        run_id = cur.lastrowid
        _pipeline_event(con, run_id, 'ingestion', 'completed', 'Observation metadata ingested and linked to its source URL.', {'observation_id': obs['id'], 'satellite': obs['satellite'], 'sensor': obs['sensor']})
        coverage = json.loads(obs['coverage_json'] or '{}')
        qc_pass = obs['quality_status'] == 'accepted' and bool(obs['checksum']) and bool(coverage) and (obs['resolution_km'] is None or float(obs['resolution_km']) > 0)
        qc_score = 1.0 if qc_pass else 0.35
        qc_message = 'Checksum, coverage metadata, and resolution checks passed.' if qc_pass else 'Needs review: accepted checksum and non-empty coverage metadata are required.'
        _pipeline_event(con, run_id, 'qc', 'completed' if qc_pass else 'needs-review', qc_message, {'qc_score': qc_score, 'quality_status': obs['quality_status']})
        _pipeline_event(con, run_id, 'dataset', 'metadata-only', 'Dataset provenance is stored; image pixels are not claimed because no licensed pixel asset was supplied.', {'source_url': obs['source_url'], 'channel': obs['channel']})
        con.commit()
        forecast = build_forecast()
        model_available = forecast.get('status') == 'available'
        if model_available:
            _pipeline_event(con, run_id, 'model', 'completed', 'Transparent OLS trajectory baseline executed on persisted verified cyclone observations; it is not an image model.', {'model': forecast.get('model'), 'training_dataset': forecast.get('training_dataset')})
            _pipeline_event(con, run_id, 'identification', 'blocked', 'Satellite image identification was not run because no validated image model and pixels are connected.', {'reason': 'licensed imagery and image model unavailable'})
            _pipeline_event(con, run_id, 'classification', 'blocked', 'Satellite image classification was not run because no validated image model and pixels are connected.', {'reason': 'licensed imagery and image model unavailable'})
            _pipeline_event(con, run_id, 'prediction', 'completed', 'Cross-source cyclone track prediction available from the verified-observation baseline.', {'points': len(forecast.get('predicted_track', [])), 'source_type': 'verified-observations'})
            max_wind = max([float(p.get('wind_kmh') or 0) for p in forecast.get('predicted_track', [])] or [0])
            risk = 'high' if max_wind >= 118 else 'moderate' if max_wind >= 63 else 'low'
            _pipeline_event(con, run_id, 'risk_track', 'completed', 'Risk and track output generated from the cross-source baseline; not a satellite-derived landfall claim.', {'risk_level': risk, 'max_wind_kmh': max_wind})
            _pipeline_event(con, run_id, 'map', 'completed', 'Output is available to the RAVEN map and operational dashboard.', {'map_ready': True})
            status = 'partial' if not qc_pass else 'completed'
            track = {'forecast': forecast.get('predicted_track', []), 'source': 'verified-observation baseline', 'satellite_inference': False}
            con.execute('UPDATE pipeline_runs SET completed_at=?,status=?,current_stage=?,qc_score=?,detection_label=?,classification_label=?,confidence=?,risk_level=?,track_json=?,error_message=? WHERE id=?', (now(), status, 'map', qc_score, 'not-run', 'not-run', forecast.get('confidence_percent'), risk, json.dumps(track), 'Satellite identification/classification are blocked until licensed pixels and a validated image model are connected.' if not qc_pass else None, run_id))
        else:
            for key, label in PIPELINE_STAGES[3:]:
                _pipeline_event(con, run_id, key, 'blocked', 'Waiting for enough verified cyclone observations and a validated satellite image model.', {'reason': 'model output unavailable'})
            con.execute('UPDATE pipeline_runs SET completed_at=?,status=?,current_stage=?,qc_score=?,detection_label=?,classification_label=?,error_message=? WHERE id=?', (now(), 'blocked', 'model', qc_score, 'not-run', 'not-run', 'No model output is available; no satellite inference was fabricated.', run_id))
        row = con.execute('SELECT * FROM pipeline_runs WHERE id=?', (run_id,)).fetchone()
    result = dict(row)
    result['track_json'] = json.loads(result.get('track_json') or '{}')
    with db() as con:
        result['events'] = [dict(event) for event in con.execute('SELECT * FROM pipeline_events WHERE run_id=? ORDER BY id', (result['id'],))]
        for event in result['events']:
            event['metadata_json'] = json.loads(event.get('metadata_json') or '{}')
    return result


def cyclone_analysis():
    with db() as con:
        row=con.execute('SELECT * FROM satellite_observations WHERE quality_status="accepted" ORDER BY observed_at DESC LIMIT 1').fetchone()
    if not row:
        return {'status':'unavailable','label':'AI satellite detection unavailable','message':'No quality-accepted satellite observation is connected. RAVEN will not infer a cyclone from missing imagery.','sources':[x['name'] for x in satellite_catalog()]}
    return {'status':'awaiting_model','label':'Satellite observation stored; model inference unavailable','message':'A quality-accepted satellite metadata record exists, but no trained image detection/classification model is installed.','observation':dict(row),'sources':[x['name'] for x in satellite_catalog()]}


def validation_report():
    with db() as con:
        rows=[dict(r) for r in con.execute('SELECT model_name,model_version,dataset_version,split_name,metric_name,metric_value,sample_count,evaluated_at FROM validation_metrics ORDER BY evaluated_at DESC')]
    return {'status':'unavailable' if not rows else 'available','message':'No independently evaluated model metrics are stored; operational accuracy is not claimed.' if not rows else 'Metrics are from stored evaluation runs and are not fabricated.','metrics':rows}


def official_reports(tab='current'):
    current = [
        {'name':'IMD Cyclone Information and Warnings','provider':'India Meteorological Department','kind':'official warning portal','url':'https://mausam.imd.gov.in/','region':'India and North Indian Ocean','status':'live'},
        {'name':'IMD Sub-Divisionwise Weather Warning','provider':'India Meteorological Department','kind':'official weather warning portal','url':'https://mausam.imd.gov.in/responsive/all_warning.php','region':'India','status':'live'},
        {'name':'NOAA/NHC Active Tropical Cyclones','provider':'NOAA / National Hurricane Center','kind':'operational tropical cyclone feed','url':'https://www.nhc.noaa.gov/','region':'Atlantic, Eastern Pacific, Central Pacific','status':'live'},
        {'name':'JTWC Tropical Cyclone Warning Centre','provider':'Joint Typhoon Warning Center','kind':'operational tropical cyclone feed','url':'https://www.metoc.navy.mil/jtwc/jtwc.html','region':'Western Pacific, Indian Ocean','status':'live'},
    ]
    archives = [
        {'name':'NOAA IBTrACS historical best track','provider':'NOAA National Centers for Environmental Information','kind':'historical archive','url':'https://www.ncei.noaa.gov/products/international-best-track-archive','region':'Global seasons and basins','status':'archive'},
        {'name':'NOAA Historical Hurricane Tracks','provider':'NOAA Office for Coastal Management','kind':'historical track explorer','url':'https://coast.noaa.gov/hurricanes/','region':'Global historical tracks','status':'archive'},
        {'name':'Open-Meteo Historical Weather API','provider':'Open-Meteo','kind':'weather archive','url':'https://open-meteo.com/en/docs/historical-weather-api','region':'Global locations','status':'archive'},
        {'name':'INCOIS ocean information','provider':'Indian National Centre for Ocean Information Services','kind':'ocean and marine information','url':'https://www.incois.gov.in/portal/','region':'Indian Ocean region','status':'live'},
    ]
    selected = archives if str(tab).lower() == 'archives' else current
    return {'status':'available','tab':'archives' if str(tab).lower() == 'archives' else 'current','reports':selected,'generated_at':now(),'disclaimer':'Links open the authoritative provider. RAVEN does not replace official warnings.'}


IBTRACS_BASE = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv'
IBTRACS_BASINS = {'NA': 'NA', 'EP': 'EP', 'WP': 'WP', 'NI': 'NI', 'SI': 'SI', 'SP': 'SP', 'SA': 'SA'}
_history_cache = {}
_history_lock = threading.Lock()


def _ibtracs_file(basin):
    code = IBTRACS_BASINS.get((basin or 'NA').upper(), 'NA')
    return f'{IBTRACS_BASE}/ibtracs.{code}.list.v04r01.csv', code


def historical_storms(year=None, basin='ALL'):
    try:
        year = int(year or time.gmtime().tm_year - 1)
    except (TypeError, ValueError):
        raise ValueError('year must be an integer')
    if year < 1848 or year > time.gmtime().tm_year + 1:
        raise ValueError('year is outside the IBTrACS range')
    basin = (basin or 'ALL').upper()
    if basin == 'ALL':
        basin = 'NA'
    if year >= time.gmtime().tm_year - 2:
        url = f'{IBTRACS_BASE}/ibtracs.last3years.list.v04r01.csv'
        basin_code = basin
    else:
        url, basin_code = _ibtracs_file(basin)
    key = (year, basin_code, url)
    with _history_lock:
        cached = _history_cache.get(key)
    if cached is None:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'text/csv'})
        with urllib.request.urlopen(req, timeout=60) as response:
            csv_bytes = response.read()
        text = io.TextIOWrapper(io.BytesIO(csv_bytes), encoding='utf-8', errors='replace', newline='')
        reader = csv.DictReader(text)
        storms = {}
        for row in reader:
            sid = (row.get('SID') or '').strip()
            season = (row.get('SEASON') or '').strip()
            row_basin = (row.get('BASIN') or '').strip().upper()
            if not sid or not season.isdigit() or int(season) != year:
                continue
            if url.endswith('last3years.list.v04r01.csv') and basin not in ('ALL', row_basin):
                continue
            lat_raw = (row.get('LAT') or '').strip()
            lon_raw = (row.get('LON') or '').strip()
            try:
                lat, lon = float(lat_raw), float(lon_raw)
            except ValueError:
                continue
            name = (row.get('NAME') or 'UNNAMED').strip() or 'UNNAMED'
            storm = storms.setdefault(sid, {'storm_id': sid, 'name': name, 'season': year, 'basin': basin_code, 'source': 'NOAA IBTrACS', 'track': []})
            point = {'time': (row.get('ISO_TIME') or '').strip(), 'latitude': lat, 'longitude': lon}
            wind = (row.get('USA_WIND') or '').strip()
            pressure = (row.get('USA_PRES') or '').strip()
            if wind:
                try: point['wind_knots'] = float(wind)
                except ValueError: pass
            if pressure:
                try: point['pressure_hpa'] = float(pressure)
                except ValueError: pass
            storm['track'].append(point)
        cached = sorted(storms.values(), key=lambda item: item['name'])
        with _history_lock:
            _history_cache[key] = cached
    return {'status': 'available', 'year': year, 'basin': basin_code, 'storms': cached, 'storm_count': len(cached), 'source': 'NOAA IBTrACS v04r01', 'source_url': url, 'data_status': 'live'}


def scenario_projection(latitude, longitude, speed_kmh, heading_deg, wind_kmh=0, days=5):
    latitude, longitude = float(latitude), float(longitude)
    speed_kmh, heading_deg = float(speed_kmh), float(heading_deg)
    wind_kmh, days = float(wind_kmh), int(days)
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError('latitude or longitude is outside valid geographic ranges')
    if speed_kmh < 0 or speed_kmh > 200 or wind_kmh < 0 or wind_kmh > 400:
        raise ValueError('speed or wind is outside accepted ranges')
    if days < 1 or days > 5:
        raise ValueError('days must be between 1 and 5')
    heading = math.radians(heading_deg % 360)
    generated = now()
    points = []
    for hour in range(24, days * 24 + 1, 24):
        distance = speed_kmh * hour
        lat = latitude + (distance * math.cos(heading) / 111.0)
        cos_lat = max(0.2, abs(math.cos(math.radians(latitude))))
        lon = longitude + (distance * math.sin(heading) / (111.0 * cos_lat))
        points.append({'hours_ahead': hour, 'timestamp': generated + hour * 3600, 'latitude': round(max(-90, min(90, lat)), 3), 'longitude': round(((lon + 180) % 360) - 180, 3), 'wind_kmh': round(wind_kmh, 1), 'uncertainty_km': round(35 + hour * 2.5, 1), 'confidence_percent': max(20, round(86 - hour * 0.45))})
    return {'status': 'available', 'label': 'Transparent kinematic scenario projection', 'model': 'constant-speed great-circle approximation', 'generated_at': generated, 'input': {'latitude': latitude, 'longitude': longitude, 'speed_kmh': speed_kmh, 'heading_deg': heading_deg, 'wind_kmh': wind_kmh, 'days': days}, 'projected_track': points, 'sources': ['User-provided verified position and motion estimate'], 'disclaimer': 'Decision-support scenario only. This is not a prediction of formation, landfall, or intensity and is not an official warning.'}

def refresh_cycle():
    weather = refresh_weather(); observations = ingest_noaa(); forecast = build_forecast()
    if forecast.get('status') == 'available':
        sev = 'warning' if forecast.get('confidence_percent',0) >= 60 else 'watch'
        with db() as con:
            con.execute('INSERT INTO alerts(severity,title,message,source,created_at) VALUES(?,?,?,?,?)',(sev,'AI forecast refreshed','A new AI-generated cyclone track estimate is available. Confirm with official bulletins.',','.join(forecast.get('sources',[])) or 'RAVEN model',now()))
    return {'weather_status':weather.get('data_status'),'observations_ingested':observations,'forecast_status':forecast.get('status')}


def worker():
    while True:
        try: refresh_cycle()
        except Exception: pass
        time.sleep(REFRESH_SECONDS)


def json_body(handler):
    n=int(handler.headers.get('Content-Length','0'))
    if n>1_000_000: raise ValueError('Request too large')
    return json.loads(handler.rfile.read(n) or b'{}')

class Handler(BaseHTTPRequestHandler):
    server_version='RAVEN/1.0'
    def send_json(self, data, status=200):
        raw=json.dumps(data).encode(); self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def do_GET(self):
        p=urllib.parse.urlparse(self.path); path=p.path
        if path=='/api/healthz': return self.send_json({'status':'ok','service':'RAVEN','refresh_seconds':REFRESH_SECONDS})
        if path=='/cyclocane-proxy' or path.startswith('/cyclocane-proxy/'):
            target_path=path[len('/cyclocane-proxy'):].lstrip('/')
            try: return cyclocane_proxy(self, target_path, p.query)
            except Exception as exc: return self.send_json({'error':'Cyclocane proxy unavailable','detail':str(exc)},502)
        if path.startswith('/javascripts/') or path.startswith('/stylesheets/'):
            try: return cyclocane_proxy(self, path.lstrip('/'), p.query)
            except Exception as exc: return self.send_json({'error':'Cyclocane asset unavailable','detail':str(exc)},502)
        if path=='/api/dashboard':
            with db() as con:
                weather=con.execute('SELECT payload FROM weather_snapshots ORDER BY captured_at DESC LIMIT 1').fetchone()
                forecast=con.execute('SELECT payload FROM forecasts ORDER BY generated_at DESC LIMIT 1').fetchone()
                alerts=[dict(r) for r in con.execute('SELECT * FROM alerts WHERE status="active" ORDER BY created_at DESC LIMIT 10')]
                shelters=[dict(r) for r in con.execute('SELECT * FROM shelters ORDER BY district,name')]
                broadcasts=[dict(r) for r in con.execute('SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 10')]
            return self.send_json({'app':'RAVEN','weather':json.loads(weather['payload']) if weather else {'data_status':'unavailable'},'forecast':json.loads(forecast['payload']) if forecast else build_forecast(),'analysis':cyclone_analysis(),'validation':validation_report(),'satellite_catalog':satellite_catalog(),'alerts':alerts,'shelters':shelters,'broadcasts':broadcasts,'generated_at':now()})
        if path=='/api/weather/history':
            try:
                q=urllib.parse.parse_qs(p.query); return self.send_json(fetch_weather_history(q.get('lat',[DEFAULT_LAT])[0], q.get('lon',[DEFAULT_LON])[0]))
            except Exception as exc:
                return self.send_json({'status':'unavailable','error':str(exc),'source':'Open-Meteo Historical Archive'}, 502)
        if path=='/api/cyclone/risk':
            try:
                q=urllib.parse.parse_qs(p.query); return self.send_json(cyclone_risk_prediction(q.get('lat',[DEFAULT_LAT])[0], q.get('lon',[DEFAULT_LON])[0]))
            except Exception as exc:
                return self.send_json({'status':'unavailable','error':str(exc),'source':'Open-Meteo Historical Archive'}, 502)
        if path=='/api/weather':
            q=urllib.parse.parse_qs(p.query); lat=float(q.get('lat',[DEFAULT_LAT])[0]); lon=float(q.get('lon',[DEFAULT_LON])[0]); return self.send_json(open_meteo(lat,lon))
        if path=='/api/ocean':
            q=urllib.parse.parse_qs(p.query); lat=float(q.get('lat',[DEFAULT_LAT])[0]); lon=float(q.get('lon',[DEFAULT_LON])[0]); return self.send_json(marine_at(lat,lon))
        if path=='/api/ocean/grid':
            try:
                q=urllib.parse.parse_qs(p.query)
                force=q.get('refresh',['0'])[0].lower() in ('1','true','yes')
                return self.send_json(ocean_grid_report(q.get('start_date',[None])[0], q.get('end_date',[None])[0], force=force))
            except ValueError as exc:
                return self.send_json({'status':'invalid','error':str(exc),'source':'Open-Meteo Marine'},400)
            except Exception as exc:
                return self.send_json({'status':'unavailable','error':str(exc),'source':'Open-Meteo Marine'},502)
        if path=='/api/noaa/cyclones':
            try: return self.send_json(noaa_forecast_geojson())
            except Exception as exc: return self.send_json({'type':'FeatureCollection','features':[],'source':'NOAA/NHC Tropical Weather GIS Forecast Points','data_status':'unavailable','error':str(exc)},502)
        if path=='/api/cyclone/live':
            try: return self.send_json(live_cyclone_geojson())
            except Exception as exc: return self.send_json({'type':'FeatureCollection','features':[],'source':'NOAA/NHC CurrentStorms + official forecast GIS','data_status':'unavailable','error':str(exc)},502)
        if path=='/api/cyclone/scenario':
            try:
                q=urllib.parse.parse_qs(p.query)
                result=scenario_projection(q.get('lat',[DEFAULT_LAT])[0], q.get('lon',[DEFAULT_LON])[0], q.get('speed_kmh',[0])[0], q.get('heading_deg',[0])[0], q.get('wind_kmh',[0])[0], q.get('days',[5])[0])
                return self.send_json(result)
            except Exception as exc:
                return self.send_json({'status':'invalid','error':str(exc)}, 400)
        if path=='/api/cyclone/forecast':
            with db() as con: r=con.execute('SELECT payload FROM forecasts ORDER BY generated_at DESC LIMIT 1').fetchone()
            return self.send_json(json.loads(r['payload']) if r else build_forecast())
        if path=='/api/cyclone/analysis': return self.send_json(cyclone_analysis())
        if path=='/api/cyclone/history':
            try:
                q=urllib.parse.parse_qs(p.query); year=q.get('year',[None])[0]; basin=q.get('basin',['NA'])[0]
                return self.send_json(historical_storms(year, basin))
            except Exception as exc:
                return self.send_json({'status':'unavailable','error':str(exc),'source':'NOAA IBTrACS'}, 502)
        if path=='/api/official-reports':
            q=urllib.parse.parse_qs(p.query); return self.send_json(official_reports(q.get('tab',['current'])[0]))
        if path=='/api/satellite/catalog': return self.send_json(satellite_catalog())
        if path=='/api/satellite/pipeline': return self.send_json(satellite_pipeline_overview())
        if path=='/api/satellite/datasets':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM satellite_datasets ORDER BY provider, platform')])
        if path=='/api/satellite/pipeline/runs':
            with db() as con: return self.send_json(satellite_pipeline_overview()['runs'])
        if path=='/api/satellite/observations':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM satellite_observations ORDER BY observed_at DESC LIMIT 100')])
        if path=='/api/validation': return self.send_json(validation_report())
        if path=='/api/models':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM model_versions ORDER BY created_at DESC')])
        if path=='/api/alerts':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM alerts WHERE status="active" ORDER BY created_at DESC LIMIT 50')])
        if path=='/api/shelters':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM shelters ORDER BY district,name')])
        if path=='/api/broadcasts':
            with db() as con: return self.send_json([dict(r) for r in con.execute('SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 50')])
        return self.static(path)
    def do_POST(self):
        route=urllib.parse.urlparse(self.path).path
        try: data=json_body(self)
        except Exception as e: return self.send_json({'error':str(e)},400)
        if route=='/api/cyclone/observations':
            required=['storm_id','latitude','longitude']
            if any(k not in data for k in required): return self.send_json({'error':'storm_id, latitude, longitude are required'},400)
            errors=validate_observation(data)
            if errors: return self.send_json({'error':'Invalid observation','details':errors},400)
            with db() as con:
                duplicate=con.execute('SELECT id FROM cyclone_observations WHERE storm_id=? AND observed_at=?',(str(data['storm_id']),int(data.get('observed_at',now())))).fetchone()
                if duplicate: return self.send_json({'error':'Duplicate observation','id':duplicate['id']},409)
                con.execute('INSERT INTO cyclone_observations(storm_id,observed_at,latitude,longitude,wind_kmh,pressure_hpa,source,raw_json) VALUES(?,?,?,?,?,?,?,?)',(str(data['storm_id']),int(data.get('observed_at',now())),float(data['latitude']),float(data['longitude']),data.get('wind_kmh'),data.get('pressure_hpa'),str(data.get('source','verified observation')),json.dumps(data)))
            return self.send_json(build_forecast(),201)
        if route=='/api/satellite/pipeline/runs':
            try:
                result=run_satellite_pipeline(data.get('observation_id'))
                return self.send_json(result, 201)
            except Exception as exc:
                return self.send_json({'status':'error','error':str(exc)}, 400)
        if route=='/api/satellite/observations':
            required=['satellite','sensor','observed_at','source_url']
            if any(k not in data for k in required): return self.send_json({'error':'satellite, sensor, observed_at, and source_url are required'},400)
            errors=validate_observation({'latitude':data.get('latitude',0),'longitude':data.get('longitude',0),'observed_at':data.get('observed_at'),'resolution_km':data.get('resolution_km')})
            if errors: return self.send_json({'error':'Invalid satellite observation','details':errors},400)
            quality='accepted' if data.get('checksum') and data.get('coverage') else 'needs-review'
            with db() as con:
                try:
                    cur=con.execute('INSERT INTO satellite_observations(satellite,sensor,observed_at,source_url,coverage_json,resolution_km,channel,processing_status,quality_status,checksum,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(str(data['satellite']),str(data['sensor']),int(data['observed_at']),str(data['source_url']),json.dumps(data.get('coverage',{})),data.get('resolution_km'),data.get('channel'),'metadata-only',quality,data.get('checksum'),now()))
                except sqlite3.IntegrityError: return self.send_json({'error':'Duplicate satellite observation'},409)
            return self.send_json({'id':cur.lastrowid,'quality_status':quality,'message':'Metadata stored; image bytes were not fabricated or inferred.'},201)
        return self.send_json({'error':'Not found'},404)
    def static(self,path):
        rel=path.lstrip('/') or 'index.html'; f=(STATIC/rel).resolve()
        if STATIC not in f.parents or not f.is_file(): return self.send_json({'error':'Not found'},404)
        raw=f.read_bytes(); self.send_response(200); self.send_header('Content-Type', {'html':'text/html; charset=utf-8','css':'text/css','js':'application/javascript','json':'application/json'}.get(f.suffix.lstrip('.'),'application/octet-stream')); self.send_header('Cache-Control','no-store, max-age=0'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def log_message(self,*args): pass

def main():
    init_db(); threading.Thread(target=worker,daemon=True).start(); server=ThreadingHTTPServer(('0.0.0.0',PORT),Handler); print(f'RAVEN listening on 0.0.0.0:{PORT}',flush=True); server.serve_forever()
if __name__=='__main__': main()
