from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import pandas as pd
import os
from flask_cors import CORS, cross_origin
from apps.core.config import Config

app = Flask(__name__)
CORS(app)

@app.route('/training', methods = ['POST'])
@cross_origin()
def training_route_client():
    """
    * Method: training_route_client
    * Description: method to call training route
    :return: none
    *
    * who       when        version     change (include bug = if apply)
    * -----     -------     -------     -------------------------------
    * Ray       03/29/2023  1.0         Initial Creation
    *
    * Parameters:
    *   none
    """
    try:
        config = Config()
        # Get Run ID
        run_id = config.get_run_id()
        data_path = config.training_data_path
        # trainmodel object initialization
        return Response("Training successful! and it's RunID is: " + str(run_id))
    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)

if __name__ == "__main__":
    host = '0.0.0.0'
    port = 5000
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()
