import pytz
import json
import redis
import pickle
import random
import logging
import psycopg2
import constants
import reverse_geocoder as rg
import psycopg2.pool
import datetime
import requests
from auth0.v3.authentication import Users
from auth0.v3.authentication import GetToken

from random import randint
from cent import Client, get_timestamp, generate_token
from functools import wraps
from flask import (Flask, request, session, render_template, redirect, url_for)
from flask_session import Session
from flask_jsonpify import jsonpify
from flask_oauthlib.client import OAuth
from werkzeug.routing import FloatConverter as BaseFloatConverter


class FloatConverter(BaseFloatConverter):
    regex = r'-?\d+(\.\d+)?'

chrts = {'pie': 'piechart', 'bar': 'barchart', 'wc': 'wordcloud', 'lc': 'linechart'}
durations = {'lasth': 'lh', 'lastd': 'ld', 'lastw': 'lw', 'lastm': 'lm', 'lasty': 'ly',
             'thish': 'th', 'thisd': 'td', 'thisw': 'tw', 'thism': 'tm', 'thisy': 'ty',
             'alltime': 'at'}
# TODO: this needs to be gotten from db
pngs = {1: 'happy', 2: 'sad', 3: 'thanks', 4: 'change', 5: 'love', 6: 'angry', 7: 'bliss'}
PING_DESCRIPTION_LENGTH = 1000
# TODO: add city, state, country columns pings table and write a backend
# service to populate these from reverse geocoding service
# NOTE: we should switch to "my pings" in the home page (even for anon users) so that we dont 'clash' with
#       'clusters' in the screen. if the user wants s/he can switch back to 'global' mode where we show 'clusters' only
# NOTE: sd and ed have to be in the form of YYYYMMDD
# NOTE: need 'count' method for date range of viewport
# NOTE: create a 'reports' endpoint that
# NOTE: types of endpoints: feed, count, top, admin, user, map, geo
# NOTE: map, user (except timeline) and geo endpoint urls will show no ping descriptions. The timeline, feed endpoints will
#       show everything about the ping
# TODO: move the logging decorator
# TODO: The count endpoints dont have to hit the DB because we can get this data from their 'ping' counterparts. We just
#       have to count the 'pings' that were already in the db and return the same. If the 'ping' counterpart key
#       does not exist then we can hit the DB (tough luck).
# TODO: check expiry intervals thoroughly
# TODO: move all the DB code to _getResultsFromDB method. For now continue with initial approach and (may be) in second phase
#       move all the db code to the above method
# TODO: show user pings in the pingmap related
# TODO: show childpings/parentpings in the pingmap related
# TODO: total counts in the home page
# TODO: show thishour, thismonth, thisday, thisweek, thisyear
# TODO: doxygen output of the code for ankit
# TODO: when a user creates pings, update the redis key for the same
# TODO: get lasth, lastd, lastw, lastm, lasty based on the user's current time zone (use PST if timezone is not available)
# TODO: use UTC millisecs for all DB operations, convert UTC to user's local time zone for internet facing operations (read/update)
# TODO: write a decorator to check the dates
# TODO: do we need to check for valid lat/lon coordinates?
# TODO: this has to be in the .env
# TODO: write decorators to check for country, state and cities
# TODO: in all the functions check the redis cache before you hit the db
# TODO: my top 5 pings
# TODO: my top pings graphs
# TODO: my top pings word cloud
# TODO: all the data needs to be UTF8'fied
# TODO: check expiry constants for redis in the settings file
prd_db = 'P9x"2->K'
prd_dv = 'U8CqsevMND'
connection_pool = psycopg2.pool.ThreadedConnectionPool(10, 100, database='pcolors', user='pingr', password=prd_db)
rediscon = redis.Redis(host='localhost', port=6379, db=0)
# conn = connection_pool.getconn()
# use conn, then:
# connection_pool.putconn(conn)
# import pdb;pdb.set_trace();
utctz = pytz.timezone('UTC')
# initialize logging
logging.basicConfig(filename='/tmp/api.log',
                    filemode='w',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info("program started")
app = Flask(__name__)
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RY'
app.config['SESSION_TYPE'] = 'filesystem'
oauth = OAuth(app)
auth0 = oauth.remote_app(
    'auth0',
    consumer_key='3xMT1o2DjtfLQ55L2OR7Iy6vjaLuLSwH',
    consumer_secret='CxaHgyWzd9dmz_vU43q5OkB8xFcSemGJvIcIxGlNSvUiec7S2qZ68iqykYOwIblS',
    request_token_params={
        'scope': 'openid profile',
        'audience': 'https://' + 'pingcolors.auth0.com' + '/userinfo'
    },
    base_url='https://%s' % 'pingcolors.auth0.com',
    access_token_method='POST',
    access_token_url='/oauth/token',
    authorize_url='/authorize',
)

# before routes are registered
app.url_map.converters['float'] = FloatConverter

sess = Session()
# TODO: We should move these to config file
HOUR_EXPIRY = 60 * 60
DAY_EXPIRY = 24 * HOUR_EXPIRY
MONTH_EXPIRY = 28 * DAY_EXPIRY
YEAR_EXPIRY = 365 * DAY_EXPIRY
WEEK_EXPIRY = 6 * DAY_EXPIRY
MINUTE_EXPIRY = 60
CENT_SECRET = "eb516ed8-4a8c-4a04-b963-5cd703c51f30"
CENT_URL = "http://localhost:8000"


#initialize centrifugo client
client = Client(CENT_URL, CENT_SECRET, timeout=1)

# Utility methods


def _checkAdmin():
    """this is a derivative of checkAuthentication"""
    try:
        if session['_admin']:
            return True
        else:
            return False
    except Exception:
        raise Exception('invalid access/not logged in')


def _checkAuthentication():
    """this checks if the user is logged in or not"""
    try:
        if session['_isloggedin']:
            True
        else:
            return False
    except Exception:
        raise Exception('invalid access/not logged in')


def _checkCity(city, state, country):
    try:
        if rediscon.exists('countries:' + country + ":states:" + state + ":cities:" + city):
            return True
        else:
            if _checkCountry(country):
                if _checkState(state):
                    qry = """SELECT * FROM cities WHERE id = %s and country = %s" and state = %s"""
                    con = connection_pool.getconn()
                    cur = con.cursor()
                    cur.execute(qry, (city, state, country))
                    pinginfo = ""
                    cur.close()
                    connection_pool.putconn(con)
                    if pinginfo:
                        # store it in the redis database, before you send this to user
                        rediscon.set('countries:' + country + ":states:" + state + ":cities:" + city)
                        return True
                    else:
                        return False
                else:
                    raise Exception('invalid state')
            else:
                raise Exception('invalid country')
    except Exception:
        raise Exception('invalid city')


def _checkState(state, country):
    try:
        if rediscon.exists('countries:' + country + ":states:" + state):
            return True
        else:
            if _checkCountry(country):
                qry = """SELECT * FROM states WHERE (name = %s or char_code = %s) and country = %s"""
                con = connection_pool.getconn()
                cur = con.cursor()
                cur.execute(qry, (state, country))
                pinginfo = ""
                cur.close()
                connection_pool.putconn(con)
                if pinginfo:
                    # store it in the redis database, before you send this to user
                    rediscon.set('countries:' + country + ":states:" + state)
                    return True
                else:
                    return False
            else:
                raise Exception('invalid state')
    except Exception:
        raise Exception('invalid state')


def _checkCountry(country):
    try:
        if rediscon.exists('countries:' + country):
            return True
        else:
            qry = """SELECT * FROM countries WHERE name = %s or char_code = %s"""
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.execute(qry, (country))
            pinginfo = ""
            cur.close()
            connection_pool.putconn(con)
            if pinginfo:
                # store it in the redis database, before you send this to user
                rediscon.set('countries:' + country)
                return True
            else:
                return False
    except Exception:
        raise Exception('invalid country')


def _checkDuration(d):
    logging.info("Got duration request with value " + d)
    if d in durations:
        logging.info("Duration is valid: " + d)
        return True
    else:
        logging.error("Invalid duration given: " + d + "=")
        return False


def _checkLatLon(swlat, swlon, nelat, nelon):
    # check the latitude & longitude
    if not (-90 <= swlat <= 90) and (-90 <= nelat <= 90):
        logging.error("Wrong latitude value given")
        raise ValueError
    if not (-180 <= swlon <= 180) and (-180 <= nelon <= 180):
        logging.error("Wrong longitude value given")
        raise ValueError
    logging.info("latitude and longitude values are valid")
    return


def _checkPingId(pid):
    if type(pid).__name__ not in ('int', 'long') or pid <= 0:
        raise ValueError("invalid ping id")


def _checkChartTypes(chrt):
    if chrt in chrts:
        return True
    else:
        return False


def _checkDates(sd, ed):
    try:
        sd = datetime.datetime.strptime(sd, '%Y%m%d')
        sd = sd.strftime('%Y-%m-%d')
        ed = datetime.datetime.strptime(ed, '%Y%m%d')
        ed = ed.strftime('%Y-%m-%d')
        if sd > ed:
            raise ValueError('start date should be less than or equal to the end date')
        else:
            return (sd, ed)
    except Exception:
        raise Exception('invalid date(s) given')


def _getResultsFromDB(qrydict):
    res = []
    con = connection_pool.getconn()
    cur = con.cursor()
    if qrydict['query']:
        cur.execute(qrydict['qry'], qrydict['args'])
    if qrydict['sproc']:
        cur.callproc(qrydict['proc'], qrydict['args'])
    # TODO: fetch the results
    cur.close()
    connection_pool.putconn(con)
    return res


def _packDataForGeoJSON(data):
    geoJsonData = {'type': 'FeatureCollection', 'features': []}
    for i in data:
        geoJsonData['features'].append({'type': 'Feature',
                                        'properties': {'title': i['pc'],
                                                       'iconUrl': i['pi'],
                                                       'id': i['id'],
                                                       'iconSize': [10, 10]},
                                        'geometry': {'type': 'Point',
                                                     'coordinates': [i['ln'], i['lt']]}})
    return geoJsonData


def _sessinfo():
    # ##################################
    # REMOVE THE LOCALHOST IN PRODUCTION
    # ##################################
    if request.remote_addr == '127.0.0.1':
        session['city'] = 'Hyderabad'
        session['state'] = 'Telangana'
        session['country'] = 'india'
        session['country_code'] = 'IN'
        session['lat'] = 17.3871
        session['lon'] = 78.4916
        session['loc'] = str(session['lat']) + ", " + str(session['lon'])
        session['tz'] = 'Asia/Calcutta'
        session['ipaddress'] = '127.0.0.1'
        session['uid'] = 1
        session['utype'] = 'USER'
    else:
        response = requests.get("https://api.ipdata.co/" + request.remote_addr)
        logging.debug("Got request from " + str(response.content))
        session['ipaddress'] = request.remote_addr
        session['city'] = response.json()['city']
        session['state'] = response.json()['region']
        session['country'] = response.json()['country_name']
        session['country_code'] = response.json()['country_code']
        session['lat'] = response.json()['latitude']
        session['lon'] = response.json()['longitude']
        session['loc'] = str(session['lat']) + ", " + str(session['lon'])
        session['tz'] = response.json()['time_zone']
        # REMOVE THIS IN PRODUCTION
        session['uid'] = 1
        session['utype'] = 'USER'
    return session


# Decorators

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'profile' not in session:
            # Redirect to Login page here
            return redirect('/')
        return f(*args, **kwargs)
    return decorated


def requires_correct_duration(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            logging.info(str(kwargs))
            if _checkDuration(kwargs['d']):
                return f(*args, **kwargs)
            else:
                raise Exception
        except Exception as e:
            logging.error("IInvalid duration given " + e)
    return wrapped


def requires_correct_charttypes(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkChartTypes(kwargs['chrt'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.error("couldn't create the session " + str(ve))
    return wrapped


def requires_sess(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _sessinfo()
            return f(*args, **kwargs)
        except Exception as ve:
            logging.error("couldn't create the session " + str(ve))
    return wrapped


def requires_correct_latlon(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkLatLon(kwargs['swlat'], kwargs['swlon'], kwargs['nelat'], kwargs['nelon'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.error("invalid gps coordinates given " + str(ve))
    return wrapped


def requires_valid_ping(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkPingId(kwargs['pid'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.error("invalid ping given " + str(ve))
    return wrapped


def requires_admin(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkAdmin()
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid credentials given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid credentials given"})
    return wrapped


def requires_login(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkAuthentication()
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid credentials given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid credentials given"})
    return wrapped


def requires_correct_city(f):
    """this decorator checks the correctness of the city with the help of _checkCity function"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkCity(kwargs['city'], kwargs['state'], kwargs['country'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid city given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid city given"})
    return wrapped


def requires_correct_state(f):
    """this decorator checks the correctness of the state with the help of _checkState function"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkState(kwargs['state'], kwargs['country'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid state given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid state given"})
    return wrapped


def requires_correct_country(f):
    """this decorator checks the correctness of the country with the help of _checkCountry function"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            _checkCountry(kwargs['country'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid country given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid country given"})
    return wrapped


def requires_correct_dates(f):
    """this decorator checks the correctness of the dates with the help of _checkDates function"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            (sd, ed) = _checkDates(kwargs['sd'], kwargs['ed'])
            return f(*args, **kwargs)
        except Exception as ve:
            logging.fatal("invalid date(s) given " + str(ve))
            return jsonpify({"status": 0, "msg": "invalid date(s) given"})
    return wrapped

# Authentication & authorization


def _update_redis_keys(d, swlat, swlon, nelat, nelon):
    # you gotta send data only for the selected/default time filter but you gotta update all the other
    # time filters including the counts
    qry = """lastx_thisx_bounding_box"""
    con = connection_pool.getconn()
    cur = con.cursor()
    for i in ['th', 'td', 'tw', 'tm', 'ty', 'at']:
        bounding_box = []
        results = []
        geoJSONData = {}
        ky = ":".join((i, '{:.2f}'.format(swlat), '{:.2f}'.format(swlon), '{:.2f}'.format(nelat), '{:.2f}'.format(nelon)))
        logging.error("Cache-MISS!! " + ky)
        logging.info("Calling crsr with duration " + i)
        cur.callproc(qry, ['crsr', swlat, swlon, nelat, nelon, "2017-12-01", "2017-12-01", 0, session['tz'], i])
        cur.close()
        cur2 = con.cursor('crsr')
        results = cur2.fetchall()
        if len(results) > 0:
            for i in results:
                bounding_box.append(i[0])
            geoJSONData = _packDataForGeoJSON(bounding_box)
        else:
            logging.error("No results for " + i + " " + ky)
            geoJSONData = {'type': 'FeatureCollection', 'features': []}
        # store this info in redis before we send it to user
        rediscon.setex(ky, pickle.dumps(bounding_box), MINUTE_EXPIRY)
        # update the counts also
        rediscon.setex(ky, count_bndbox, HOUR_EXPIRY)
    cur2.close()
    connection_pool.putconn(con)
    return geoJSONData


@app.route('/callback')
def callback_handling():
    code = request.args.get('code')
    get_token = GetToken('pingcolors.auth0.com')
    auth0_users = Users('pingcolors.auth0.com')
    token = get_token.authorization_code('3xMT1o2DjtfLQ55L2OR7Iy6vjaLuLSwH',
                                         'CxaHgyWzd9dmz_vU43q5OkB8xFcSemGJvIcIxGlNSvUiec7S2qZ68iqykYOwIblS',
                                         code,
                                         'http://www.pingcolors.com/callback')
    user_info = auth0_users.userinfo(token['access_token'])

    ert = requests.post('https://pingcolors.auth0.com/tokeninfo',
                        headers={'content-type': 'application/json'},
                        json={'id_token':token['id_token']})
    jsn = ert.json()

    return redirect('/dashboard')


@app.route('/dashboard', methods=['GET', 'POST'])
@requires_auth
def dashboard():
    return render_template('dashboard.html',
                           userinfo=session[constants.PROFILE_KEY],
                           userinfo_pretty=json.dumps(session[constants.JWT_PAYLOAD], indent=4))


@app.route('/login', methods=['GET'])
def login():
    return auth0.authorize(callback='http://www.pingcolors.com/callback')


@app.route('/logout', methods=['GET'])
def logout():
    # Clear session stored data
    session.clear()
    # Redirect user to logout endpoint
    params = {'returnTo': url_for('home', _external=True), 'client_id': '3xMT1o2DjtfLQ55L2OR7Iy6vjaLuLSwH'}
    return redirect(auth0.base_url + '/v2/logout?' + urlencode(params))


@app.route("/api/v1/user/geoinfo", methods=['GET'])
@requires_sess
def user_geo():
    if 'lat' in session and session['lat'] is not None:
        return jsonpify({'data': {'lat': session['lat'], 'lng': session['lon']}})
    else:
        # TODO: move this to _sessinfo method
        session['lat'] = 37.77
        session['lon'] = -122.43
        session['city'] = 'san francisco'
        session['state'] = 'ca'
        session['country'] = 'usa'
        session['country_code'] = 'us'
        session['loc'] = session['lat'] + ", " + session['lon']
        session['tz'] = 'America/Los_Angeles'
        return jsonpify({'data': {'lat': session['lat'], 'lng': session['lon']}})


@app.route("/api/v1/user/isloggedin", methods=['GET'])
@requires_sess
@requires_login
def user_loggedin():
    pass


@app.route("/api/v1/testcent", methods=['GET'])
@requires_sess
def test_cent():
    channel = "news"
    data = {'input': str(random.randint(100, 1000))}
    client.publish(channel, data)
    return('', 204)


@app.route("/api/v1/pingtypes", methods=['GET'])
@requires_sess
def pingtypes():
    '''get the existing ping types'''
    # check the redis for the pingtypes
    ky = 'pingtypes'
    ptypes = []
    if redis.exists(ky):
        ptypes = redis.get(ky)
    else:
        qry = """get_pingtypes"""
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.execute(qry)
        cur.close()
        connection_pool.putconn(con)
        # store it in the redis database, before you send this to user
        rediscon.set('pingtypes', ptypes)
    pass


# Ping create/update/read

@app.route("/api/v1/ping/<int:pid>", methods=['GET'])
@requires_sess
@requires_valid_ping
def get_ping_info(pid):
    # THIS IS FOR TESTING REMOVE IN PROD
    pid = 659177
    # check the pinginfo in the redis..there needs to be no
    # expiry date set for this btw as the info will never change
    pinginfo = {}
    geoinfo = {}
    ky = 'ping:' + str(pid)
    if rediscon.exists(ky):
        temp = rediscon.get(ky)
        pinginfo = pickle.loads(temp)
    else:
        logging.error("Cache-MISS!! " + ky)
        qry = """pinginfo"""
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', pid, session['tz']])
        cur.close()
        cur2 = con.cursor('crsr')
        pinginfo = cur2.fetchone()[0]
        cur2.close()
        connection_pool.putconn(con)
        if pinginfo:
            try:
                geoinfo = rg.search((pinginfo['lt'], pinginfo['ln']))[0]
            except Exception:
                if geoinfo['name'] == '':
                    geoinfo['name'] = 'NA'
                if geoinfo['admin1'] == '':
                    geoinfo['admin1'] = 'NA'
                if geoinfo['cc'] == '':
                    geoinfo['cc'] = 'NA'
            # get the location information
            pinginfo['city'] = geoinfo['name']
            pinginfo['state'] = geoinfo['admin1']
            pinginfo['country'] = geoinfo['cc']
        else:
            logging.error("No results for " + " " + ky)
        # store this info in redis before we send it to user
        rediscon.setex(ky, pickle.dumps(pinginfo), YEAR_EXPIRY)
    return jsonpify(pinginfo)


@app.route("/api/v1/ping/create", methods=['POST'])
@requires_sess
def ping_create():
    # get the current location from session, userid (if any) and pingid
    pingid = int(request.json['id'])
    # check if the last_pingid for this user matches with the passed in pingid
    qry = "ping_insert"
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry, [session['uid'], pingid, session['lat'], session['lon'], session['utype']])
    lastid = cur.fetchone()[0]
    session['lastpingid'] = lastid
    cur.close()
    connection_pool.putconn(con)
    # insert this ping in the redis too
    rediscon.set('lastpinginfo:', "_".join([str(session['uid']), str(pingid), session['utype'], str(lastid)]))
    # this should go to a messaging system
    #geojsondata = _update_redis_keys(request.json['d'],
    #                                 request.json['swlat'],
    #                                 request.json['swlon'],
    #                                 request.json['nelat'],
    #                                 request.json['nelon'])
    return jsonpify({'status': 1,
                     'id': lastid,
                     'lat': session['lat'],
                     'lng': session['lon'],
                     'iurl': pngs[pingid]})


@app.route("/api/v1/ping/update", methods=['POST'])
@requires_sess
@requires_valid_ping
def ping_update():
    '''ping update optionally happens if the user wants to write the ping description'''
    ping_id = request.get_json()['id']
    # check if the ping_id exists in the session
    if session.get['latest_pingid'] == ping_id:
        # check if the ping description is of valid length
        ping_description = request.get_json()['pingd'].strip()
        # TODO: 1000, this has to come from config
        if 0 > len(ping_description) <= PING_DESCRIPTION_LENGTH:
            qry = "ping_update"
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.execute(qry, (session.get['uid'], ping_id, ping_description))
            updlastid = cur.fetchone()
            sess.put['updatepingid'] = updlastid
            cur.close()
            connection_pool.putconn(con)
            return jsonpify({'status': 1, 'msg': 'success'})
        else:
            return jsonpify({'status': 0, 'msg': 'too small or too large of a ping description'})
    else:
        return jsonpify({'status': 0, 'msg': 'illegal ping id'})


# Reporting


@app.route("/api/v1/pings/map/<string:d>/<float:swlat>/<float:swlon>/<float:nelat>/<float:nelon>", methods=['POST', 'GET'])
@requires_sess
@requires_correct_latlon
@requires_correct_duration
def get_pings_viewport_duration(d, swlat, swlon, nelat, nelon):
    """get pings in a given bounding box in the last hour"""
    bounding_box = []
    results = []
    geoJSONData = {}
    ky = ":".join((d, '{:.2f}'.format(swlat), '{:.2f}'.format(swlon), '{:.2f}'.format(nelat), '{:.2f}'.format(nelon)))
    # import pdb;pdb.set_trace();
    logging.info("Got " + d + " Viewport Request with following values: " + ky + " From: " + session['ipaddress'])
    # TODO: make the swlat, swlon, nelat, nelon strings and use 'lasth' to prefix as the key for redis
    # check the pings in the redis database
    if rediscon.exists(ky):
        logging.info("Cache-hit!! " + ky)
        temp = rediscon.get(ky)
        try:
            bounding_box = pickle.loads(temp)
            geoJSONData = _packDataForGeoJSON(bounding_box)
        except Exception:
            geoJSONData = {'type': 'FeatureCollection', 'features': []}
    else:
        logging.error("Cache-MISS!! " + ky)
        qry = """lastx_thisx_bounding_box"""
        con = connection_pool.getconn()
        cur = con.cursor()
        # the dates are just a place holder they just need to be legal/valid dates but we dont do anything with
        # them. Since this is not a logged-in user data, the userid field is 0
        logging.info("Calling crsr with duration " + durations[d])
        cur.callproc(qry, ['crsr', swlat, swlon, nelat, nelon, "2017-12-01", "2017-12-01", 0, session['tz'], durations[d]])
        cur.close()
        cur2 = con.cursor('crsr')
        results = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        if len(results) > 0:
            for i in results:
                bounding_box.append(i[0])
            geoJSONData = _packDataForGeoJSON(bounding_box)
        else:
            logging.error("No results for " + d + " " + ky)
            geoJSONData = {'type': 'FeatureCollection', 'features': []}
        # store this info in redis before we send it to user
        rediscon.setex(ky, pickle.dumps(bounding_box), MINUTE_EXPIRY)
    return jsonpify(geoJSONData)


@app.route("/api/v1/pings/count/<string:d>/<float:swlat>/<float:swlon>/<float:nelat>/<float:nelon>", methods=['POST', 'GET'])
@requires_sess
@requires_correct_latlon
@requires_correct_duration
def get_pings_viewport_duration_count(d, swlat, swlon, nelat, nelon):
    """get pings count in a given bounding box in the last hour"""
    # check for the value in the redis
    if d in durations:
        count_bndbox = 0
        bnds = ':'.join((d, '{:.2f}'.format(swlat), '{:.2f}'.format(swlon), '{:.2f}'.format(nelat), '{:.2f}'.format(nelon)))
        ky = ':'.join((d, "count", bnds))
        ky2 = ':'.join((d, bnds))
        if rediscon.exists(ky):
            logging.error("Cache-hit!! " + ky)
            lasth_count_bndbox = rediscon.get(ky)
        elif rediscon.exists(ky2):
            logging.error("Cache-hit From Pings!! " + ky2)
            count_bndbox = len(pickle.loads(rediscon.get(ky2)))
            rediscon.setex(ky, lasth_count_bndbox, HOUR_EXPIRY)
        else:
            logging.error("Cache-MISS!! " + ky)
            qry = """lastx_thisx_bounding_box_count"""
            con = connection_pool.getconn()
            cur = con.cursor()
            # the dates are just a place holder they just need to be legal/valid dates but we dont do anything with
            # them. Since this is not a logged-in user data, the userid field is 0
            cur.callproc(qry, (swlat, swlon, nelat, nelon, "2017-12-01", "2017-12-01", 0, session['tz'], durations[d]))
            count_bndbox = cur.fetchone()[0]
            cur.close()
            connection_pool.putconn(con)
            # store this info in redis before we send it to user
            rediscon.setex(ky, count_bndbox, HOUR_EXPIRY)
        return jsonpify({'status': 1, 'count_bndbox': count_bndbox})
    else:
        return "no view"


@app.route("/api/v1/pings/map/<int:sd>/<int:ed>/<float:swlat>/<float:swlon>/<float:nelat>/<float:nelon>", methods=['POST', 'GET'])
@requires_sess
@requires_correct_dates
@requires_correct_latlon
def get_pings(sd, ed, swlat, swlon, nelat, nelon):
    '''Depending on the page, we show pings with ids or
    entire ping information (description, location etc for logged in user)'''
    # load pings for the home page
    pings_daterange_bounding_box = []
    results = []
    ky = ':'.join(("pings", "daterange", str(sd), str(ed), str(swlat), str(swlon), str(nelat), str(nelon)))
    logging.info("""
                 Got Pings Date Range Viewport Request with following values: """ + ky + " From: " + session['ipaddress'])
    if rediscon.exists(ky):
        logging.error("Cache-hit!! " + ky)
        temp = rediscon.get(ky)
        pings_daterange_bounding_box = pickle.loads(temp)
    else:
        logging.error("Cache-MISS!! " + ky)
        qry = """lastx_thisx_bounding_box"""
        con = connection_pool.getconn()
        cur = con.cursor()
        # the dates are just a place holder they just need to be legal/valid dates but we dont do anything with
        # them. Since this is not a logged-in user data, the userid field is 0
        cur.callproc(qry, ['crsr', swlat, swlon, nelat, nelon, sd, ed, 0, session['tz'], ""])
        cur.close()
        cur2 = con.cursor('crsr')
        results = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        if len(results) > 0:
            for i in results:
                pings_daterange_bounding_box.append(i[0])
        else:
            logging.error("No results for pings daterange bounding box " + ky)
        # store this info in redis before we send it to user
        rediscon.setex(ky, pickle.dumps(pings_daterange_bounding_box), HOUR_EXPIRY)
    return jsonpify({'status': 1, 'pings_daterange_bounding_box': pings_daterange_bounding_box})


@app.route("/api/v1/pings/geo/<string:d>/<string:country>")
@requires_sess
@requires_correct_country
@requires_correct_duration
def country_pings(d, country):
    """Get the country's ping count in last hour"""
    if d in durations:
        country = []
        ky = d + ':' + country
        if rediscon.exists(ky):
            country = rediscon.get(ky)
        else:
            qry = """lastx_thisx_geo"""
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.callproc(qry, ["", "", "", country, "", "", "", durations[d], session['tz']])
            country = cur.fetchall()
            cur.close()
            connection_pool.putconn(con)
            rediscon.setex(ky, country, HOUR_EXPIRY)
        return jsonpify({'country': country})
    else:
        return "no view"


@app.route("/api/v1/pings/geo/<string:d>/<string:state>/<string:country>")
@requires_sess
@requires_correct_state
@requires_correct_duration
def state_pings(d, state, country):
    """Get the country's ping count in last hour"""
    if d in durations:
        state = []
        ky = d + ':' + state
        if rediscon.exists(ky):
            state = rediscon.get(ky)
        else:
            qry = """ lastx_thisx_geo"""
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.callproc(qry, ("", state, country, "", "", "", "lh", session['tz']))
            state = cur.fetchall()
            cur.close()
            connection_pool.putconn(con)
            rediscon.setex(ky, state, HOUR_EXPIRY)
        return jsonpify({'state': state})
    else:
        return "no view"


@app.route("/api/v1/pings/geo/<string:d>/<string:city>/<string:state>/<string:country>")
@requires_sess
@requires_correct_city
@requires_correct_duration
def lasthour_city_pings(d, city, state, country):
    """Get the country's ping count in last hour"""
    if d in durations:
        city = []
        ky = d + ':' + city
        if rediscon.exists(ky):
            city = rediscon.get(ky)
        else:
            qry = """ lastx_thisx_geo """
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.callproc(qry, (city, state, country, "", "", "", durations[d], session['tz']))
            city = cur.fetchall()
            cur.close()
            connection_pool.putconn(con)
            rediscon.setex(ky, city, HOUR_EXPIRY)
        return jsonpify({'city': city})
    else:
        return "no view"


@app.route("/api/v1/pings/geo/<int:sd>/<int:ed>/<string:country>")
@requires_sess
@requires_correct_dates
@requires_correct_country
def geo_country_pings(sd, ed, country):
    """gets the ping count in the country between two dates"""
    ky = "country_pings:" + ":".join((country, sd, ed))
    country_pings_daterange = []
    if rediscon.exists(ky):
        country_pings_daterange = rediscon.get(ky)
    else:
        qry = """ geo_pings_daterange"""
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ("", "", country, sd, ed, session['tz']))
        country_pings_daterange = cur.fetchall()
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, country_pings_daterange, YEAR_EXPIRY)
    return jsonpify({'countrypingsdaterange': country_pings_daterange})


@app.route("/api/v1/pings/geo/<int:sd>/<int:ed>/<string:state>/<string:country>")
@requires_sess
@requires_correct_dates
@requires_correct_state
def geo_state_pings(sd, ed, state, country):
    """gets the ping count in the state between two dates"""
    ky = "state_pings:" + ":".join((state, country, sd, ed))
    state_pings_daterange = []
    if rediscon.exists(ky):
        state_pings_daterange = rediscon.get(ky)
    else:
        qry = """ geo_pings_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ("", state, country, sd, ed, session['tz']))
        state_pings_daterange = cur.fetchall()
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, state_pings_daterange, DAY_EXPIRY)
    return jsonpify({'state_pings_daterange': state_pings_daterange})


@app.route("/api/v1/pings/geo/<int:sd>/<int:ed>/<string:city>/<string:state>/<string:country>")
@requires_sess
@requires_correct_city
@requires_correct_dates
def geo_city_pings(sd, ed, city, state, country):
    """gets the ping count in the city between two dates"""
    ky = "state_pings:" + ":".join((state, country, sd, ed))
    city_pings_daterange = []
    if rediscon.exists(ky):
        city_pings_daterange = rediscon.get(ky)
    else:
        qry = """ geo_pings_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, (city, state, country, sd, ed, session['tz']))
        city_pings_daterange = cur.fetchall()
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, city_pings_daterange, HOUR_EXPIRY)
    return jsonpify({'city_pings_daterange': city_pings_daterange})


@app.route("/api/v1/pings/user/timeline/feed/<limit>/<offset>")
@requires_sess
@requires_login
@requires_correct_dates
def timeline_pings_limit_offset(limit, offset, userid):
    """ get all the pings(along with descriptions) for the logged in user """
    ky = "timeline_pings_limit_offset:" + ":".join((userid, limit, offset))
    timeline_pings_daterange = []
    if rediscon.exists(ky):
        timeline_pings_daterange = rediscon.get(ky)
    else:
        qry = """ timeline_pings_limit_offset """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, (userid, limit, offset))
        timeline_pings_daterange = cur.fetchall()
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, timeline_pings_limit_offset, HOUR_EXPIRY)
    return jsonpify({'timeline_pings_limit_offset': timeline_pings_daterange})


@app.route("/api/v1/pings/user/timeline/<int:sd>/<int:ed>")
@requires_sess
@requires_login
@requires_correct_dates
def timeline_pings(sd, ed, userid):
    """ get all the pings(along with descriptions) for the logged in user """
    ky = "timeline_pings:" + ":".join((userid, sd, ed))
    timeline_pings_daterange = []
    if rediscon.exists(ky):
        timeline_pings_daterange = rediscon.get(ky)
    else:
        qry = """ timeline_pings_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, [userid, sd, ed])
        timeline_pings_daterange = cur.fetchall()
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, timeline_pings_daterange, HOUR_EXPIRY)
    return jsonpify({'timeline_pings_daterange': timeline_pings_daterange})


@app.route("/api/v1/pings/count/global/<int:sd>/<int:ed>")
@requires_correct_dates
@requires_sess
def global_pings_count_dates(sd, ed):
    qry = """ global_ping_count_between_two_dates """
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry, [sd, ed])
    globalPingCount = cur.fetchone()[0]
    cur.close()
    connection_pool.putconn(con)
    return jsonpify({'globalpingcount': globalPingCount[0]})


@app.route("/api/v1/pings/count/global")
@requires_sess
def global_pings_count():
    globalPingCount = 0
    ky = "globalpingcount"
    if rediscon.exists(ky):
        globalPingCount = rediscon.get(ky)
    else:
        qry = """ global_ping_count """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry)
        globalPingCount = cur.fetchone()[0]
        cur.close()
        connection_pool.putconn(con)
        rediscon.setex(ky, globalPingCount, 60)
    return jsonpify({'globalpingcount': globalPingCount[0]})


@app.route("/api/v1/pings/top/parents/<int:sd>/<int:ed>")
@requires_sess
@requires_correct_dates
def top_parents(sd, ed):
    '''rank all the parents with the ping counts from <int:sd> to <int:ed>'''
    # check the redis server for existing info
    plist = []
    ky = 'top_parents:' + ':'.join((sd, ed))
    if rediscon.exists(ky):
        plist = rediscon.get(ky)
    else:
        qry = """ top_parents_between_two_dates """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        parentPings = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i in parentPings:
            plist.append(i)
        # put this info in redis with one day as expiry date, before sending it to the user
        rediscon.setex(ky, plist, DAY_EXPIRY)
    return jsonpify(plist)


@app.route("/api/v1/pings/feed/<string:d>/<int:start>/<int:count>")
@requires_sess
#@requires_correct_duration
def lastx_thisx_feed(d, start, count):
    """this gives the pings in this hour (so far)"""
    thish_pings = {}
    ky = ":".join(("user_feed", str(session['uid']), d, str(start), str(count)))
    if rediscon.exists(ky):
        t_pings = rediscon.get(ky)
        this_pings = pickle.loads(t_pings)
    else:
        qry = """ lastx_thisx_feed """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', 2, '2017-01-01', '2017-12-31', 'Asia/Calcutta', 'ly', count, start])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i in topusrs:
            thish_pings[i[0]['id']] = i[0]
        rediscon.setex(ky, pickle.dumps(thish_pings), 60)
    return jsonpify(thish_pings)


@app.route("/api/v1/pings/count/<string:d>")
@requires_sess
@requires_correct_duration
def lastx_thisx_count():
    """this gives the pings count in this hour (so far)"""
    thish_pings_count = 0
    ky = ':'.join(("thish", "pings", "count"))
    ky2 = ':'.join(("thish", "pings"))
    if rediscon.exists(ky):
        logging.error("Cache-hit!! " + ky)
        thish_pings_count = rediscon.get(ky)
    elif rediscon.exists(ky2):
        logging.error("Cache-hit From Pings!! " + ky2)
        thish_pings_count = len(pickle.loads(rediscon.get(ky2)))
        rediscon.setex(ky, thish_pings_count, 60)
    else:
        logging.error("Cache-MISS!! " + ky)
        qry = """lastx_thisx_pings_count"""
        con = connection_pool.getconn()
        cur = con.cursor()
        # the dates are just a place holder they just need to be legal/valid dates but we dont do anything with
        # them. Since this is not a logged-in user data, the userid field is 0
        cur.callproc(qry, ("2017-12-01", "2017-12-01", 0, session['tz'], "th"))
        thish_pings_count = cur.fetchone()[0]
        cur.close()
        connection_pool.putconn(con)
        # store this info in redis before we send it to user
        rediscon.setex(ky, thish_pings_count, 60)
    return jsonpify({'status': 1, 'thish_pings_count': thish_pings_count})


@app.route("/api/v1/pings/map/my/<string:d>")
@requires_sess
@requires_correct_duration
def thisy_mypings():
    """this gives the pings in this year (so far) for the logged in user"""
    thisy_mypings = 0
    ky = "thisy:mypings:" + session['uid']
    if rediscon.exists(ky):
        thisy_mypings = rediscon.get(ky)
    else:
        qry = """ thisy_mypings """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', session['uid']])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i in topusrs:
            thisy_mypings = i
        rediscon.setex(ky, thisy_mypings, DAY_EXPIRY)
    return jsonpify(thisy_mypings)


@app.route("/api/v1/pings/count/my/<string:d>")
@requires_login
@requires_correct_duration
def my_pings_duration_count(d):
    """return the pings count of the loggedin user in the last month"""
    mypings_lastm_count = 0
    ky = "mypings:lastm:count:" + session['uid']
    if rediscon.exists(ky):
        mypings_lastm_count = rediscon.get(ky)
    else:
        qry = """ mypings_lastm_count """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', session['uid']])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i in topusrs:
            mypings_lastm_count = i
        rediscon.setex(ky, mypings_lastm_count, DAY_EXPIRY)
    return jsonpify(mypings_lastm_count)


@app.route("/api/v1/admin/users/top/<string:country>")
@requires_admin
@requires_correct_country
def topusers_country(country):
    """get the top users of ping in a country for all time"""
    topusers_alltime_country = []
    ky = 'topusers:' + country
    if rediscon.exists(ky):
        topusers_alltime_country = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_alltime_country[i] = j
        rediscon.setex(ky, topusers_country, DAY_EXPIRY)
    return jsonpify(topusers_alltime_country)


@app.route("/api/v1/admin/users/top/<string:state>/<string:country>")
@requires_admin
@requires_correct_state
def topusers_state(state, country):
    """get the top users of ping in a state for all time"""
    topusers_state_country = {}
    ky = "topusers:" + ":".join((state, country))
    if rediscon.exists(ky):
        topusers_state_country = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_state_country[i] = j
        rediscon.setex(ky, topusers_state_country, DAY_EXPIRY)
    return jsonpify(topusers_state_country)


@app.route("/api/v1/admin/users/top/<string:city>/<string:state>/<string:country>")
@requires_admin
@requires_correct_city
def topusers_city(city, state, country):
    """get the top users of ping in a city for all time"""
    topusers_city_state_country = {}
    ky = "topusers:" + ":".join((city, state, country))
    if rediscon.exists(ky):
        topusers_city_state_country = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_city_state_country[i] = j
        rediscon.setex(ky, topusers_city_state_country, DAY_EXPIRY)
    return jsonpify(topusers_country)


@app.route("/api/v1/admin/users/top")
@requires_admin
def topusers_of_ping():
    """get the top users of ping all time"""
    topusers = {}
    ky = "topusers"
    if rediscon.exists(ky):
        topusers = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr'])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers[i] = j
        rediscon.setex(ky, topusers, DAY_EXPIRY)
    return jsonpify(topusers_country)


@app.route("/api/v1/admin/users/top/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_dates
def topusers_of_ping_daterange(sd, ed):
    """get the top users of ping in the daterange"""
    topusers_daterange = {}
    ky = "topusers:" + ":".join((sd, ed))
    if rediscon.exists(ky):
        topusers_daterange = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_daterange[i] = j
        rediscon.setex(ky, topusers_daterange, DAY_EXPIRY)
    return jsonpify(topusers_country)


@app.route("/api/v1/admin/users/top/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_country
@requires_correct_dates
def topusers_country_daterange(country, sd, ed):
    """get the top users in a country for a given date range"""
    topusers_country = {}
    ky = "topusers:" + ":".join((str(country), sd, ed))
    if rediscon.exists(ky):
        topusers_country = rediscon.get(ky)
    else:
        qry = """ topusers_country_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_country[i] = j
        rediscon.setex(ky, topusers_country, DAY_EXPIRY)
    return jsonpify(topusers_country)


@app.route("/api/v1/admin/users/top/<string:state>/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_state
@requires_correct_dates
def topusers_state_daterange(state, country, sd, ed):
    """get the top users in a state for a given date range"""
    topusers_state_country = {}
    ky = "topusers:" + ":".join((str(state), str(country), sd, ed))
    if rediscon.exists(ky):
        topusers_state_country = rediscon.get(ky)
    else:
        qry = """ topusers_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', state, country, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_state_country[i] = j
        rediscon.setex(ky, topusers_state_country, DAY_EXPIRY)
    return jsonpify(topusers_state_country)


@app.route("/api/v1/admin/users/top/<string:city>/<string:state>/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_city
@requires_correct_dates
def topusers_city_daterange(city, state, country, sd, ed):
    """get the top users in a city for a given date range"""
    topusers_city_state_country = {}
    ky = "topusers:" + ":".join((str(city), str(state), str(country), sd, ed))
    if rediscon.exists(ky):
        topusers_city_state_country = rediscon.get(ky)
    else:
        qry = """ topusers_city_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', city, state, country, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            topusers_city_state_country[i] = j
        rediscon.setex(ky, topusers_city_state_country, DAY_EXPIRY)
    return jsonpify(topusers_city_state_country)


@app.route("/api/v1/admin/pings/top/childpings/<parent>")
@requires_admin
def top_child_pings_of_parent_global(parent):
    """Get all ranked child pings of a given parent globally all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparentalltime:' + parent
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', parent])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:country>")
@requires_admin
@requires_correct_country
def top_child_pings_of_parent_country_alltime(parent, country):
    """Get all ranked child pings of a given parent in a country all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparentalltime:' + ':'.join((parent, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', parent, country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:state>/<string:country>")
@requires_admin
@requires_correct_state
def top_child_pings_of_parent_state_alltime(parent, state, country):
    """Get all ranked child pings of a given parent in a state all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparentalltime:' + ':'.join((parent, state, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', parent, state, country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:city>/<string:state>/<string:country>")
@requires_admin
@requires_correct_city
def top_child_pings_of_parent_city_alltime(parent, city, state, country):
    """Get all ranked child pings of a given parent in a city all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparentalltime:' + ':'.join((parent, city, state, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent_city_state_country """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_dates
@requires_correct_country
def top_child_pings_of_parent_country_daterange(sd, ed, parent, country):
    """Get all ranked child pings of a given parent in a country all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparent:' + ':'.join((sd, ed, parent, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent_country_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', country, parent, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:state>/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_dates
@requires_correct_state
def top_child_pings_of_parent_state_daterange(sd, ed, parent, state, country):
    """Get all ranked child pings of a given parent in a state all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparent:' + ':'.join((sd, ed, parent, state, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_parent_state_country_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', parent, state, country, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/admin/pings/top/childpings/<parent>/<string:city>/<string:state>/<string:country>/<int:sd>/<int:ed>")
@requires_admin
@requires_correct_dates
@requires_correct_city
def top_child_pings_of_parent_city_daterange(sd, ed, parent, city, state, country):
    """Get all ranked child pings of a given parent in a city all time"""
    top_child_pings_parent_daterange = []
    ky = 'topchildpingsofparent:' + ':'.join((sd, ed, parent, city, state, country))
    if rediscon.exists(ky):
        top_child_pings_parent_daterange = rediscon.get(ky)
    else:
        qry = """ topchildpings_city_state_country_daterange """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr', parent, state, country, sd, ed])
        cur.close()
        cur2 = con.cursor('crsr')
        topusrs = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i, j in topusrs.items():
            top_child_pings_parent_daterange[i] = j
        rediscon.setex(ky, top_child_pings_parent_daterange, DAY_EXPIRY)
    return jsonpify(top_child_pings_parent_daterange)


@app.route("/api/v1/pings/top/childpings/alltime")
def top_child_pings():
    '''Get all child pings, parent pings in the db in the descending order'''
    top_child_pings_alltime = []
    if rediscon.exists('topchildpingsalltime'):
        top_child_pings_alltime = rediscon.get('topchildpingsalltime')
    else:
        qry = """ topchildpings_alltime """
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.callproc(qry, ['crsr'])
        cur.close()
        cur2 = con.cursor('crsr')
        childPings = cur2.fetchall()
        cur2.close()
        connection_pool.putconn(con)
        for i in childPings:
            top_child_pings_alltime.append(i)
        rediscon.setex('topchildpingsalltime', top_child_pings_alltime, DAY_EXPIRY)
    return jsonpify(top_child_pings_alltime)


@app.route("/api/v1/pings/parents")
def parents():
    '''Show all the existing parent pings in the datab'''
    # check the redis before you query db
    plist = []
    if rediscon.exists('parentpings'):
        plist = rediscon.get('parentpings')
    else:
        qry = """SELECT UNNEST(enum_range(NULL::ping_master_types))"""
        con = connection_pool.getconn()
        cur = con.cursor()
        cur.execute(qry)
        parents = cur.fetchall()
        cur.close()
        plist = []
        connection_pool.putconn(con)
        for i in parents:
            plist.append(i)
        # put this in redis (with an expiry time of 30 days) before sending it to user
        rediscon.setex('parentpings', plist, YEAR_EXPIRY)
    return jsonpify(plist)


@app.route("/api/v1/pings/map/<string:parent>")
@requires_sess
def children_of_parent(parent):
    '''given a parent ping, rank all the child pings along with counts'''
    ky = 'childpings:' + parent
    if rediscon.exists(ky):
        plist = rediscon.get(ky)
    else:
        # check if the parent exists in the ping_master_types
        try:
            # parent pings are ALWAYS in upper case
            parent = parent.upper()
            # MOVE THIS TO STOREDPROC AND REDIS
            qry = """SELECT 1
                     FROM (SELECT
                             UNNEST(
                                    ENUM_RANGE(
                                               NULL::ping_master_types))) c
                     WHERE c.unnest = %s"""
            logging.debug("Sent query " + qry + " with parent: " + parent)
            con = connection_pool.getconn()
            cur = con.cursor()
            cur.execute(qry, [parent])
            # stored proc
            qry = """ child_pings_of_parent """
            logging.info("Sent query " + qry + " with parent " + parent)
            cur.callproc(qry, ['cur2', parent])
            cur2 = con.cursor('cur2')
            child_pings = cur2.fetchall()
            cur2.close()
            plist = []
            connection_pool.putconn(con)
        except Exception as e:
            logging.fatal("Error during children_of_parent call " + str(e))
            return jsonpify({"status": 0, "msg": "unknown error occurred"})
        for i in child_pings:
            plist.append(i)
        rediscon.setex(ky, plist, YEAR_EXPIRY)
    return jsonpify(plist)


@app.route("/api/v1/a/pings/graphs/<string:chrt>/<int:sd>/<int:ed>", methods=['GET'])
@requires_sess
#@requires_correct_dates
@requires_correct_charttypes
def a_pings(sd, ed, chrt):
    '''This single method prepares the data for charts/analytics
       for logged in user or anon users/stats page'''
    chart = {}
    # TODO: get the user id here
    ky = chrt + ':global:' + ':'.join((str(sd), str(ed)))
    if rediscon.exists(ky):
        chart_temp = rediscon.get(ky)
        chart = pickle.loads(chart_temp)
    else:
        qry = """global_ping_word_cloud """
        con = connection_pool.getconn()
        cur = con.cursor()

        # TODO: get the user id here
        cur.callproc(qry, [0, 'Asia/Calcutta', 'ly'])
        childPings = cur.fetchall()[0][0]
        cur.close()
        connection_pool.putconn(con)
        chart['wc'] = childPings
        rediscon.setex(ky, pickle.dumps(chart), DAY_EXPIRY)
    return jsonpify({'status': 1, 'data': chart})


@app.route("/api/v1/logind")
@requires_sess
def logind():
    # the login and password
    login = request.getJson()['login']
    password = request.getJson()['password']
    userinfo = {}
    qry = """check_user"""
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry, (login, password))
    # return with user info viz., login_name, isadmin, lastlogin, most recent ping etc
    userinfo = cur.fetchone()
    cur.close()
    connection_pool.putconn(con)
    if userinfo:
        session['_login'] = userinfo['login']
        session['_email'] = userinfo['email']
        session['_isloggedin'] = True
        session['_isadmin'] = userinfo['isadmin']
        session['_lastlogin'] = userinfo['lastlogin']
        session['_mostrecentpingid'] = userinfo['pingid']
        session['_mostrecentpingname'] = userinfo['pingname']
        # TODO: return to the homepage with loggedin user name
        return render_template()
    else:
        # TODO: return to error page
        return render_template()


@app.route("/api/v1/logoutd")
@requires_sess
def logoutd():
    session.pop('_login')
    session.pop('_email')
    session.pop('_isloggedin')
    session.pop('_isadmin')
    session.pop('_lastlogin')
    session.pop('_mostrecentpingid')
    session.pop('_mostrecentpingname')
    # TODO: return to the home page
    return render_template()


# admin create/activate/deactivate of pings have a direct bearing on the UI as the UI need to dynamically adapt
# TODO: for admin we dont have to use the cache/redis


@app.route("/api/v1/admin/create/ping/parent")
@requires_admin
def create_parent_ping():
    """create a parent ping"""
    parent_title = request.getJson()['ptitle']
    qry = """ create_parent_ping """
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry)
    lastid = 0
    cur.close()
    connection_pool.putconn(con)
    # store it in the redis (with expiry time of 28 days) before sending it to the user
    return jsonpify({'success': 1, 'lastid': lastid})


@app.route("/api/v1/admin/create/ping/child")
@requires_admin
def create_child_ping():
    """create a child ping"""
    parentid = request.getJson()['pid']
    child_title = request.getJson()['child_title']
    child_color = request.getJson()['child_color']
    qry = """ create_child_ping """
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry, (parentid, child_title, child_color))
    lastid = 0
    cur.close()
    connection_pool.putconn(con)
    return jsonpify({'success': 1, 'lastid': lastid})


@app.route("/api/v1/admin/activate/<uid>")
@requires_admin
def activate_user(uid):
    """activate a user"""
    pass


@app.route("/api/v1/admin/users/list")
@requires_admin
def list_users():
    """list all users"""
    pass


@app.route("/api/v1/admin/user/<uid>")
@requires_admin
def user_info(uid):
    """get user info"""
    pass


@app.route("/api/v1/admin/deactivate/<uid>")
@requires_admin
def deactivate_user(uid):
    """deactivate a user"""
    pass


@app.route("/api/v1/admin/makeadmin/<uid>")
@requires_admin
def makedamin_user(uid):
    """make user admin """
    pass


@app.route("/api/v1/admin/removeadmin/<uid>")
@requires_admin
def removeadmin_user(uid):
    """remove user as admin"""
    pass


@app.route("/api/v1/admin/activate/ping")
@requires_admin
def activate_ping():
    """activate a child ping"""
    pass


@app.route("/api/v1/admin/deactivate/ping")
@requires_admin
def deactivate_ping():
    """deactivate a child ping"""
    pass


@app.route("/api/v1/admin/update/ping")
@requires_admin
def update_ping():
    """update ping title, parent and any other pertinent information"""
    parentid = request.getJson()['pid']
    child_title = request.getJson()['child_title']
    child_color = request.getJson()['child_color']
    qry = """ update_child_ping """
    con = connection_pool.getconn()
    cur = con.cursor()
    cur.callproc(qry, (parentid, child_title, child_color))
    lastid = 0
    cur.close()
    connection_pool.putconn(con)
    return jsonpify({'success': 1, 'lastid': lastid})


@app.route("/", methods=['GET'])
def yindex():
    return render_template("map.html")


@app.route("/map", methods=['GET'])
def shw_map():
    return render_template("map.html")


@app.route("/admin", methods=['GET'])
def shw_adm():
    return render_template("admin.html")


@app.route("/about", methods=['GET'])
def shw_abt():
    return render_template("about.html")


@app.route("/persona", methods=['GET'])
def shw_psna():
    return render_template("persona.html")


@app.route("/stats", methods=['GET'])
def shw_sts():
    return render_template("stats.html")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5002')
