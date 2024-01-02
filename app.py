import os
from flask import Flask, render_template, request, jsonify
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from dotenv import load_dotenv
# from datetime import datetime

load_dotenv()  # Load environment variables from .env file


mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_host = os.getenv("MYSQL_HOST")
mysql_database = os.getenv("MYSQL_DATABASE")


app = Flask(__name__)

# MySQL database configuration
db_config = {
    "host": mysql_host,
    "user": mysql_user,
    "password": mysql_password,
    "database": mysql_database,
}

# Establish a connection to the MySQL database using SQLAlchemy
db_uri = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}"
engine = create_engine(db_uri)

# Mapping of CSV file names to headers
csv_headers_mapping = {
    'jobs': ['id', 'job'],
    'hired_employees': ['id', 'name', 'hire_datetime', 'department_id', 'job_id'],
    'departments': ['id', 'department']
    # Add more mappings as needed
}


# Function to convert timestamp to date string
def convert_timestamp(timestamp):
    if len(str(timestamp)) <= 3:
        return None
    elif len(str(timestamp)) >= 10:
        return str(timestamp)[:10]


# Endpoint to render the HTML form
@app.route('/')
def index():
    return render_template('upload_v2.html')


# Endpoint to receive historical data from CSV files
@app.route("/upload", methods=["POST"])
def upload_data():
    try:
        # Assuming the CSV file is sent as form-data with the key 'file'
        csv_file = request.files["file"]

        # Read the CSV file into a pandas DataFrame
        if csv_file and allowed_file(csv_file.filename):

            # Establish a connection to the MySQL database
            connection = mysql.connector.connect(**db_config)

            # Extract the table name from the CSV file name (excluding extension)
            table_name = os.path.splitext(csv_file.filename)[0]

            # Check if the table already exists

            table_exists_query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{mysql_database}';"
            with connection.cursor() as cursor:
                cursor.execute(table_exists_query)
                table_exists = cursor.fetchone()

            # Determine headers based on the table name
            headers = csv_headers_mapping.get(table_name, None)

            if headers is None:
                connection.close()
                return jsonify({'error': f'Headers not defined for table {table_name}.'}), 400

            if not table_exists:
                # Table does not exist, throw an error
                connection.close()
                return (
                    jsonify(
                        {
                            "error": "Table does not exist. Please create the table first."
                        }
                    ),
                    400,
                )

            df = pd.read_csv(csv_file, header=None, names=headers)

            # Convert timestamp column to date string
            if 'hire_datetime' in df.columns:
                df['hire_datetime'] = df['hire_datetime'].apply(convert_timestamp)

            chunk = 1000

            # Use the to_sql function to insert data into the MySQL table
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=chunk,
            )

            # Close the connection
            connection.close()

            return jsonify({"message": "Data uploaded successfully"})
        else:
            return jsonify({"error": "Invalid file format"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint to display results for requeriment_1
@app.route('/requeriment_1')
def show_requeriment_1_results():

    df = get_requeriment_results('requeriment_1')

    return render_template('query_results.html', title='Requeriment 1 Results', data=df.to_html())


# Endpoint to display results for requeriment_2
@app.route('/requeriment_2')
def show_requeriment_2_results():

    df = get_requeriment_results('requeriment_2')

    return render_template('query_results.html', title='Requeriment 2 Results', data=df.to_html())


# Get SQL file path function
def get_sql_path(sql_file_name):
    script_directory = os.path.dirname(os.path.abspath(__file__))
    sql_folder_path = os.path.join(script_directory, 'sql')

    # Construct the full path to the SQL file
    sql_file_path = os.path.join(sql_folder_path, sql_file_name)

    return sql_file_path


# Show requeriments result function
def get_requeriment_results(requeriment):

    sql_path = get_sql_path(f"{requeriment}.sql")

    with open(sql_path, "r") as file:
        query = file.read()

    # Establish a connection to the MySQL database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute(query)

    column_names = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(cursor.fetchall(), columns=column_names)

    cursor.close()
    connection.close()

    return df

# Helper function to check if the file extension is allowed
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"


if __name__ == "__main__":
    app.run(debug=True)
