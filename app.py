import json
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func , and_
from flask import Flask, jsonify, render_template
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Welcome to Hawaii Weather API List. Available Routes are:<br/>"
        f"/api/v1.0/precipitation : Returns total precipitation by date<br/>"
        f"/api/v1.0/stations : Returns list of stations<br/>"
        f"/api/v1.0/tobs : Returns dates and temperature observations of the most active station in Hawaii<br/>"
        f"/api/v1.0/startdate : Returns minimum , average and maximum tempreatures for a given date parameter in YYYY-MM-DD format <br/>"
        f"/api/v1.0/startdate/enddate : Returns minimum , average and maximum tempreatures for the date range parameter in YYYY-MM-DD format <br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return sum of precipitation grouped by Date as a key value pair"""
    # Query as commented above
    #results = session.query(Measurement.date,func.sum(Measurement.prcp).label('total precipitation').group_by(Measurement.date).all())
    results = session.query(Measurement.date,func.sum(Measurement.prcp).label('total precipitation')).group_by(Measurement.date).all()
    session.close()

    # Convert list of tuples into dictionary before you jsonify it
    dict_prcpbydate = dict(results)
    return jsonify(dict_prcpbydate)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """:return the list of stations within the DB"""
    results = session.query(Station.name).all()
    session.close()
    return jsonify(results)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """Query the dates and temperature observations of the most active station for the last year of data"""
    """Find the most active station first"""
    active_station = session.query(Measurement.station).group_by(
        Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    """Now get active station name"""
    mostactivestation = str(list(active_station)[0])
    """Find the two dates , current and current-1 year"""
    # Design a query to retrieve the last 12 months of precipitation data
    current_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
    # Calculate the date 1 year ago from the last data point in the database
    current_date_less_one = current_date - relativedelta(years=1)
    # Perform a query to retrieve the date and precipitation entry for most active station
    result = session.query(Measurement.date, Measurement.prcp).filter(
        Measurement.date > current_date_less_one).filter(Measurement.station==mostactivestation).all()
    session.close()
    # Convert list of tuples into list before you jsonify it
    lst_prcpObservations = list(np.ravel(result))
    return jsonify(lst_prcpObservations)

@app.route("/api/v1.0/<start>")
def tobsbydate(start):
    #Check if the input date is a valid date
    date_format = '%Y-%m-%d'
    try:
        date_obj = datetime.strptime(str(start), date_format)
    except ValueError:
        #("Incorrect data format, should be YYYY-MM-DD")
        return 'Incorrect date format, should be YYYY-MM-DD', 400
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """ When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date """
    result = session.query(func.min(Measurement.tobs).label("Minimum_Tempreature"),
                           func.avg(Measurement.tobs).label("Average_Tempreature"),
                           func.max(Measurement.tobs).label("Maximum_Tempreature"),).filter( Measurement.date >= start)

    session.close()
    for row in result:
        return jsonify(row._asdict())



@app.route("/api/v1.0/<start>/<end>")
def tobsbetween(start,end):
    #Check if the input date is a valid date
    date_format = '%Y-%m-%d'
    try:
        date_start = datetime.strptime(str(start), date_format)
        date_start = datetime.strptime(str(end), date_format)
    except ValueError:
        #("Incorrect data format, should be YYYY-MM-DD")
        return 'Incorrect date format, should be YYYY-MM-DD', 400
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """ When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date """
    result = session.query(func.min(Measurement.tobs).label("Minimum_Tempreature"),
                           func.avg(Measurement.tobs).label("Average_Tempreature"),
                           func.max(Measurement.tobs).label("Maximum_Tempreature"),).filter(and_(Measurement.date>=start,Measurement.date<=end))

    session.close()
    for row in result:
        return jsonify(row._asdict())


@app.errorhandler(404)
def page_not_found(error):
   return render_template('404.html', title = '404'), 404
if __name__ == '__main__':
    app.run(debug=True)
