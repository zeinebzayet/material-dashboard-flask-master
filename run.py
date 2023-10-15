# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

import os
from   flask_migrate import Migrate
from   flask_minify  import Minify
from   sys import exit

from apps.config import config_dict
from apps import create_app, db
from flask import render_template
from apps.OLAP.Promotion_decision_algorithm import collect, prepare_data, analyser, agir
# WARNING: Don't run with debug turned on in production!
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# The configuration
get_config_mode = 'Debug' if DEBUG else 'Production'

try:

    # Load the configuration using the default values
    app_config = config_dict[get_config_mode.capitalize()]

except KeyError:
    exit('Error: Invalid <config_mode>. Expected values [Debug, Production] ')

app = create_app(app_config)
Migrate(app, db)

if not DEBUG:
    Minify(app=app, html=True, js=False, cssless=False)
    
if DEBUG:
    app.logger.info('DEBUG            = ' + str(DEBUG)             )
    app.logger.info('Page Compression = ' + 'FALSE' if DEBUG else 'TRUE' )
    app.logger.info('DBMS             = ' + app_config.SQLALCHEMY_DATABASE_URI)
    app.logger.info('ASSETS_ROOT      = ' + app_config.ASSETS_ROOT )

@app.route('/show_customers')
def show_customers():
    data = collect()
    data, label_encoder = prepare_data(data)
    model, X_test, y_test = analyser(data)
    results, accuracy, report = agir(model, X_test, y_test, label_encoder, data)
    results_list = results.to_dict(orient='records')

    print(results_list)
    segment = 'index'
    return render_template('home/tables.html', results=results_list ,segment=segment)


if __name__ == "__main__":
    app.run()
