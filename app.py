from datetime import datetime, time, timezone
from flask import Flask, request, jsonify
import os
from database import get_db
# import psycopg2
# from dotenv import load_dotenv

# load_dotenv()
app = Flask(__name__)
# def connect():
#     return psycopg2.connect(
#                 host = os.getenv("DBHOST"),
#                 dbname = os.getenv("DBNAME"),
#                 user = os.getenv("DBUSER"),
#                 password = os.getenv("DBPASSWORD"),
#                 port = os.getenv("DBPORT"))

@app.route("/")
def fuck():
    return "FUCK !!!"

@app.post("/execute_query")
def executeQuery():
    try:
        conn = get_db()
        cursor = conn.cursor()
        data = request.get_json()
        # print(data)
        query = data["query"]
        cursor.execute(query)
        return jsonify([dict(row) for row in cursor.fetchall()])
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        
@app.post("/complex")
def addComplex():
    try:
        conn = get_db()
        cursor = conn.cursor()
        data = request.get_json()
        # print(data)
        name = data["name"]
        lat = data["lat"]
        long = data["long"]
        cursor.execute('INSERT INTO complexes (name, lat, long) VALUES (?, ?, ?);', (name, lat, long))
        return {"message": "Complex added successfully"}, 201
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.get("/complexes")
def getComplexes():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM complexes")
        return jsonify([dict(row) for row in cursor.fetchall()])
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.post("/driver")
def addDriver():
    try:
        conn = get_db()
        cursor = conn.cursor()
        data = request.get_json()
        # print(data)
        driverName = data["driverName"]
        driverPhone = data["driverPhone"]
        passengerCount = data["passengerCount"]
        permit = data["permit"]
        complexA = data["complexA"]
        complexB = data["complexB"]
        cursor.execute("select * FROM drivers where permit = ?", (permit,))
        result = cursor.fetchall()
        if result is not None and len(result) > 0:
            return jsonify({"message":"مجرى الخط موجود مسبقا"}), 200
        cursor.execute('INSERT INTO drivers (name, phone, passengers_number, permit) VALUES (?, ?, ?, ?);', (driverName, driverPhone, passengerCount, permit))
        driver_id = cursor.lastrowid
        cursor.execute('INSERT INTO driver_complexes (driver_id, complex_id) VALUES (?, ?);', (driver_id, complexA))
        cursor.execute('INSERT INTO driver_complexes (driver_id, complex_id) VALUES (?, ?);', (driver_id, complexB))
        return jsonify({
            "message": "Driver added successfully",
            "driver_id": driver_id,
            "driverName": driverName,
            "driverPhone": driverPhone,
            "passengerCount": passengerCount,
            "permit": permit,
            "complexA": complexA,
            "complexB": complexB
                }), 201
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.get("/driver-complexes/<string:permit>")
def getDriverComplexes(permit):
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("select * from complexes where id in (select complex_id from driver_complexes where driver_id in (select id FROM drivers where permit = ?))", (permit,))
        return jsonify([dict(row) for row in cursor.fetchall()]), 200
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.get("/cars/")
def getCars():
    try:
        conn = get_db()
        cursor = conn.cursor()
        # data = request.get_json()
        fromComplex = request.args.get("from")
        toComplex = request.args.get("to")
        print(fromComplex, toComplex)
        cursor.execute("select * from drivers where (status = 'inQueue' and from_complex like ? and to_complex like ?) or (status = 'onRoad' and from_complex like ? and to_complex like ?) order by status ASC, timestamp ASC", (fromComplex+'%', toComplex+'%', toComplex+'%', fromComplex+'%',))
        result = cursor.fetchall()
        drivers = []
        for item in result:
            driver = {
                "id": item[0],
                "driverName": item[1],
                "phone": item[2],
                "passengers": item[3],
                "permit": item[4],
                "status": item[5],
                "ts": item[8],
            }
            drivers.append(driver)
        return jsonify({"drivers": drivers}), 200
    except Exception as e :
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.post("/user-action")
def addUserAction():
    try:
        conn = get_db()
        cursor = conn.cursor()
        data = request.get_json()
        # print(data)
        passengerName = data["passengerName"]
        passengerPhone = data["passengerPhone"]
        driverPhone = data["driverPhone"]
        time = data["time"]
        cursor.execute('INSERT INTO users_actions (passenger_name, passenger_phone, driver_phone, "time") VALUES (?, ?, ?, ?);', (passengerName, passengerPhone, driverPhone, time))
        return {
            "message": "Passenger call added successfully",
            "passengerName": passengerName,
            "passengerPhone": passengerPhone,
            "driverPhone": driverPhone,
            "time": time
                }, 201
    except Exception as e :
        print(e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.put("/driver-status")
def changeDriverStatus():
    try:
        conn = get_db()
        cursor = conn.cursor()
        data = request.get_json()
        permit = data["permit"]
        status = data["status"]
        timestamp = data["timestamp"]
        if(status == "inQueue"):
            fromComplex = data["fromComplex"]
            toComplex = data["toComplex"]
            cursor.execute('update drivers set status = ?, timestamp = ?, from_complex = ?, to_complex = ? where permit = ?;', (status, timestamp, fromComplex, toComplex, permit,))
        else:
            cursor.execute('update drivers set status = ?, timestamp = ? where permit = ?;', (status, timestamp, permit))
        return {
            "message": "Driver status updated successfully",
            "permit": permit,
            "status": status,
            "timestamp": timestamp
                }, 200
    except Exception as e :
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=50000, debug=True)