import functools
import json
import urllib.parse
import inspect
import socket
import traceback
import pprint
import sys
sys.path.append(".")
import argparse
import glob
import os
import flask
import flask.views
import datetime
import uuid
import config

def createApp():
    """
    Initialize flask application
    """
    sys.path.append(os.getcwd())

    app = flask.Flask(__name__)
    app.secret_key = "mysrc"
    app.session_cookie_name = "mycookie"

    app.add_url_rule(config.baseUrl + "/<table>", view_func=RequestHandler.as_view("table"),
                     methods=["HEAD", "GET", "POST", "OPTIONS"])
    app.add_url_rule(config.baseUrl + "/<table>/<id>", view_func=RequestHandler.as_view("instance"),
                     methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
    app.add_url_rule(config.baseUrl + "/<table>/<id>/<action>", view_func=RequestHandler.as_view("action"),
                     methods=["POST", "GET", "OPTIONS"])

    return app


app = createApp()
def main():
    """
    Run the application
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="Listen on port.", default=8000, type=int)
    parser.add_argument('-d', "--debug", help='Run in debug mode', default=False, action='store_true', required=False)
    args = parser.parse_args()

    http_server = gevent.pywsgi.WSGIServer(('', args.port), app, log=None)
    http_server.serve_forever()


if __name__ == "__main__":
    main()