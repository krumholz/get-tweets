import os
import base64
import pymysql
from pymysql.err import OperationalError
import twitter

CONNECTION_NAME = os.environ.get('INSTANCE_CONNECTION_NAME')
DB_USER = os.environ.get('MYSQL_USERNAME')
DB_PASSWORD = os.environ.get('MYSQL_PASSWORD')
DB_NAME = os.environ.get('MYSQL_DATABASE')

api = twitter.Api(
    consumer_key = os.environ.get('TWITTER_KEY'),
    consumer_secret = os.environ.get('TWITTER_SECRET'),
    access_token_key = os.environ.get('TWITTER_ACCESS_KEY'),
    access_token_secret = os.environ.get('TWITTER_TOKEN_SECRET'),
    tweet_mode='extended'
)

mysql_config = {
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'cursorclass': pymysql.cursors.DictCursor,
  'autocommit': True
}

# Create SQL connection globally to enable reuse
# PyMySQL does not include support for connection pooling
mysql_conn = None


def __get_cursor():
    """
    Helper function to get a cursor
      PyMySQL does NOT automatically reconnect,
      so we must reconnect explicitly using ping()
    """
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()


def mysql_demo(event, context):
    global mysql_conn

    # Initialize connections lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not mysql_conn:
        try:
            mysql_conn = pymysql.connect(**mysql_config)
        except OperationalError:
            # If production settings fail, use local development ones
            mysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
            mysql_conn = pymysql.connect(**mysql_config)

    # Remember to close SQL resources declared while running this function.
    # Keep any declared in global scope (e.g. mysql_conn) for later reuse.
    with __get_cursor() as cursor:
        cursor.execute("SELECT * FROM `tweets` ORDER BY ID DESC LIMIT 1")
        last_row = cursor.fetchone()
        timeline = api.GetHomeTimeline(since_id=last_row['id'], count=200)
        val = []
        for tweet in timeline:
            val.append(tuple((tweet._json['id'], tweet._json['created_at'], tweet._json['full_text'], tweet._json['user']['screen_name'], tweet._json['source'])))
        sql = "INSERT INTO `tweets`(`id`, `created_at`, `full_text`, `screen_name`, `source`) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(sql, val)
        cursor.close()
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        return print(pubsub_message)
