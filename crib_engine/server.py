import  os
from flask import Flask, request,jsonify
from sqlalchemy import create_engine,text
from sqlalchemy.orm import scoped_session, sessionmaker
import urllib
from dotenv import load_dotenv
load_dotenv()

from src.components.TenthCriteria import TenthCriteria
from src.components.NinethCriteria import NinethCriteria
from src.components.EighthCriteria import EighthCriteria
from src.components.SeventhCriteria import SeventhCriteria
from src.components.SixthCriteria import SixthCriteria
from src.components.FifthCriteria import FifthCriteria
from src.components.FouthCriteria import FouthCriteria
from src.components.ThirdCriteria import ThirdCriteria
from src.components.SecondCriteria import SecondCriteria
from src.components.FirstCriteria import FirstCriteria
from src.components.sql.execute_sql import update_crib_status


server = Flask(__name__)

# Config
server.config["SQL_SERVER"] = os.environ.get("SQL_SERVER")
server.config["SQL_DATABASE"] = os.environ.get("SQL_DATABASE")
server.config["SQL_USER"] = os.environ.get("SQL_USER")
server.config["SQL_PASSWORD"] = os.environ.get("SQL_PASSWORD")
server.config["SQL_DRIVER"] = os.environ.get("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

connection_string = (
    f"DRIVER={server.config['SQL_DRIVER']};"
    f"SERVER={server.config['SQL_SERVER']};"
    f"DATABASE={server.config['SQL_DATABASE']};"
    f"UID={server.config['SQL_USER']};"
    f"PWD={server.config['SQL_PASSWORD']}"
)
connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
engine = create_engine(connection_url)
db_session = scoped_session(sessionmaker(bind=engine))

try:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    print("connect successfully") 
except Exception as e:
    print(f"Connection failed: {str(e)}")


@server.route("/crib_calculate", methods=["POST"])
def crib_calculate():
    try:
        data = request.get_json()
        # record = data['after']
        help_id = data['HELP_ID']
        print(help_id)

        tenth_score = TenthCriteria(help_id, db_session).calculate_tenth_score()
        nineth_score = NinethCriteria(help_id, db_session).calculate_nineth_score()
        eight_score = EighthCriteria(help_id, db_session).calculate_eighth_score()
        seventh_score = SeventhCriteria(help_id, db_session).calculate_seventh_score()
        six_score = SixthCriteria(help_id, db_session).calculate_sixth_score()
        fifth_score = FifthCriteria(help_id, db_session).calculate_fifth_score()
        fouth_score = FouthCriteria(help_id, db_session).calculate_fouth_score()
        third_score = ThirdCriteria(help_id, db_session).calculate_third_score()
        second_score = SecondCriteria(help_id, db_session).calculate_second_score()
        first_score = FirstCriteria(help_id, db_session).calculate_first_score()

        # total_score = float(tenth_score) + float(nineth_score) + float(eight_score) + float(seventh_score) + float(six_score)  + float(fouth_score) + float(third_score) + float(second_score) + float(first_score)
        total_score = float(tenth_score) + float(nineth_score) +float(eight_score) + float(seventh_score) + float(six_score) + float(fifth_score) + float(fouth_score)  + float(third_score)+ float(second_score)+ float(first_score)
        
        if total_score is not None:
            # response, status_code = update_crib_status(help_id, db_session)
            response, status_code = "success",200
            if status_code == 200:
                return jsonify({"total_score": total_score}), 200
            else:
                return jsonify({"error": response}), status_code
        else:
            return jsonify({"error": "Total score is None"}), 400

        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    server.run(host="0.0.0.0",debug=True, port=5000)
