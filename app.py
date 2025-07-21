import psycopg2
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

def connect():
    return psycopg2.connect(
                host  = os.environ.get('dbhost'),
                # host = "dpg-cm0rbcmn7f5s73cavqo0-a", 
                dbname = os.environ.get('dbname'), 
                user = os.environ.get('dbuser'), 
                password = os.environ.get('dbpassword'), 
                port = os.environ.get('dbport'))

@app.route("/")
def fuck():
    return "FUCK !!!"


@app.get("/complexes")
def getComplexes():
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("select * from complexes")
            result = cur.fetchall()
            complexes = []
            for item in result:
                complex = {
                    "id": item[0],
                    "name": item[1],
                    "lat": item[2],
                    "long": item[3],
                }
                complexes.append(complex)
            response = jsonify({"complexes": complexes})
            response.status_code = 200
            return response
        except Exception as e :
            print(e)
        finally:
            conn.commit()
            cur.close()
            conn.close()

@app.get("/driver-complexes/<string:permit>")
def getDriverComplexes(permit):
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("select * from complexes where id in (select complex_id from public.driver_complexes where driver_id in (select id FROM public.drivers where permit = %s))", (permit,))
            result = cur.fetchall()
            complexes = []
            # print(result)
            for item in result:
                complex = {
                    "id": item[0],
                    "name": item[1],
                    "lat": item[2],
                    "log": item[3],
                }
                complexes.append(complex)
            response = jsonify({"complexes": complexes})
            response.status_code = 200
            return response
        except Exception as e :
            print(e)
        finally:
            conn.commit()
            cur.close()
            conn.close()

@app.get("/driver/<string:permit>")
def getDriver(permit):
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("select * FROM public.drivers where permit = %s", (permit,))
            result = cur.fetchall()
            if result is not None and len(result) > 0:
                response = jsonify({"message":"permit already exist"})
                response.status_code = 200
                return response
            else:
                response = jsonify({"message":"permit doesn't exist"})
                response.status_code= 200
                return response
            # complexes = []
            # # print(result)
            # for item in result:
            #     complex = {
            #         "id": item[0],
            #         "name": item[1],
            #         "lat": item[2],
            #         "log": item[3],
            #     }
            #     complexes.append(complex)
            # response = jsonify({"complexes": complexes})
            # response.status_code = 200
            # return response
        except Exception as e :
            print(e)
        finally:
            conn.commit()
            cur.close()
            conn.close()

@app.get("/cars/")
def getCars():
        try:
            fromComplex = request.args.get('from').replace(" ", "")
            toComplex = request.args.get('to').replace(" ", "")
            conn = connect()
            cur = conn.cursor()
            cur.execute("select * from drivers where (status = 'inQueue' and from_complex like %s and to_complex like %s) or (status = 'onRoad' and from_complex like %s and to_complex like %s) order by status ASC, timestamp ASC", (fromComplex+'%', toComplex+'%', toComplex+'%', fromComplex+'%',))
            result = cur.fetchall()
            drivers = []
            for item in result:
                driver = {
                    "id": item[0],
                    "driverName": item[1],
                    "phone": item[2],
                    "passengers": item[3],
                    "permit": item[4],
                    "status": item[5],
                    "ts": item[6],
                }
                drivers.append(driver)
            response = jsonify({"drivers": drivers})
            response.status_code = 200
            return response
        except Exception as e :
            print(e)
        finally:
            conn.commit()
            cur.close()
            conn.close()

@app.post("/user-action")
def addUserAction():
    try:
        conn = connect()
        cur = conn.cursor()
        data = request.get_json()
        passengerName = data["passengerName"]
        passengerPhone = data["passengerPhone"]
        driverPhone = data["driverPhone"]
        time = data["time"]
        cur.execute('INSERT INTO users_actions (passenger_name, passenger_phone, driver_phone, "time") VALUES (%s, %s, %s, %s);', (passengerName, passengerPhone, driverPhone, time))
        return {
            "message": "Passenger call added",
            "passengerName": passengerName,
            "passengerPhone": passengerPhone,
            "driverPhone": driverPhone,
            "time": time
                }, 201
    except Exception as e :
        print(e)
    finally:
        conn.commit()
        cur.close()
        conn.close()

@app.put("/driver-status")
def changeDriverStatus():
    try:
        conn = connect()
        cur = conn.cursor()
        data = request.get_json()
        permit = data["permit"]
        status = data["status"]
        timestamp = data["timestamp"]
        if(status == "inQueue"):
            fromComplex = data["fromComplex"]
            toComplex = data["toComplex"]
            cur.execute('update drivers set status = %s, timestamp = %s, from_complex = %s, to_complex = %s where permit = %s;', (status, timestamp, fromComplex, toComplex, permit,))
            return {
                "message": "Driver status updated successfully",
                "permit": permit,
                "status": status,
                "timestamp": timestamp
                    }, 200
        cur.execute('update drivers set status = %s, timestamp = %s where permit = %s;', (status, timestamp, permit))
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
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=50000, debug=True)