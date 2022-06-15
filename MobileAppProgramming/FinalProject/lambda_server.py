import boto3, io, json
import eyed3
from flask import Flask, Response
from flask import request
from flask import jsonify
from werkzeug.serving import WSGIRequestHandler
from sqlalchemy import create_engine, PrimaryKeyConstraint
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
import setuptools

WSGIRequestHandler.protocol_version = "HTTP/1.1"

app = Flask(__name__)

USER = "postgres"
PW = "que!tSTR1NG"
URL = "database-1.c0kj3fru42xd.ap-northeast-2.rds.amazonaws.com"
PORT = "5432"
DB = "postgres"
engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(USER, PW, URL, PORT, DB))
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


class Song(Base):
    __tablename__ = 'songs'
    title = Column(String(120), unique=False)
    artist = Column(String(50), unique=False)
    file_name = Column(String(200), unique=True)
    __table_args__ = (
        PrimaryKeyConstraint(title, artist),
        {},
    )

    def __init__(self, title=None, artist=None, file_name=None):
        self.title = title
        self.artist = artist
        self.file_name = file_name

    def __repr__(self):
        return f'<User {self.name!r}>’'


#Base.metadata.drop_all(bind=engine)
#Base.metadata.create_all(bind=engine)

#file_name = "gorillaz-05-clint_eastwood-rev.mp3"  # Download from Icampus
bucket_name = "zappa-31brqjrdf"  # Your bucket name
file_key = "08 Despite the Weather.mp3"  # Same with file name
song_file = "08 Despite the Weather.mp3"
file_list = ["05 Know You.mp3", "08 Despite the Weather.mp3",
             "Daft Punk-09-Something About Us.mp3",
             "Kendrick Lamar - 11 - How Much a Dollar Cost (feat. James Fauntleroy & Ronald Isley).mp3",
             "08 Despite the Weather.mp3",
             "gorillaz-05-clint_eastwood-rev.mp3"]

s3 = boto3.client("s3")


def generate_url(file):
    global s3
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': "zappa-31brqjrdf",'Key': file}, ExpiresIn=360)
    return url

def get_metadata(file):
    audio = eyed3.load(file)
    title = audio.tag.title
    artist = audio.tag.artist
    print(title, '   ', artist)
    return title, artist


def add_song_info(file):
    title, artist = get_metadata(file)
    if db_session.query(Song).filter_by(title=title, artist=artist).first() is None:
        song = Song(title=title, artist=artist, file_name=file)
        db_session.add(song)
        db_session.commit()
    else:
        print("add song info failed")


@app.route("/upload", methods=['GET'])  #upload to S3
def upload():
    global s3, file_name, bucket_name, file_key

    # case 1 : upload your local file
    s3.upload_file(
        Filename=file_name,
        Bucket=bucket_name,
        Key=file_key,
    )

    # case 2 : upload new file by stringIO body
    contents = "My string to save to s3 object"
    fake_handle = io.StringIO(contents)
    s3.put_object(Bucket=bucket_name, Key="stringIO_test.txt", Body=fake_handle.read())

    print("upload finish")
    return jsonify("upload finish")


def get_total_bytes(file_name):
    global s3, bucket_name
    result = s3.list_objects(Bucket=bucket_name)
    for item in result['Contents']:
        if item['Key'] == file_name:
            return item['Size']


def get_object(file_name, total_bytes):
    global s3, bucket_name
    if total_bytes > 1000000:
        return get_object_range(file_name, total_bytes)
    return s3.get_object(Bucket=bucket_name, Key=file_name)['Body'].read()


def get_object_range(file_name, total_bytes):
    global s3, bucket_name
    offset = 0
    while total_bytes > 0:
        end = offset + 999999 if total_bytes > 1000000 else ""
        total_bytes -= 1000000
        byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=end)
        offset = end + 1 if not isinstance(end, str) else None
        yield s3.get_object(Bucket=bucket_name, Key=file_name, Range=byte_range)['Body'].read()


@app.route("/search_song", methods=['GET'])     # to RDS
def search_song():
    title = request.args.get('title')
    artist = request.args.get('artist')
    file_name=''
    check = False
    result = db_session.query(Song).all()
    for s in result:
        if s.title == title and s.artist == artist:
            file_name = s.file_name
            check = True
    return jsonify(success=check, file_name=file_name), 200


@app.route("/download", methods=['GET'])
def download():
    file = request.args.get('file_name')
    total_bytes = get_total_bytes(file)
    obj = get_object(file, total_bytes)
    return Response(obj, mimetype='mp3',headers={"Content-Disposition": "attachment;filename=test.mp3"})
    #return jsonify(file_data=obj), 200

@app.route("/get_url", methods=['GET'])
def get_url():
    file = request.args.get('file_name')
    url = generate_url(file)

    return url


if __name__ == "__main__":
    #app.run(host='localhost', port=8888)
    app.run(host='localhost', port=8888)
