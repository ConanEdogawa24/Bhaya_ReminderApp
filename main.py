from fastapi import FastAPI, HTTPException
from datetime import datetime
import sqlite3

app = FastAPI()

def get_connection():
    connection = sqlite3.connect("events.db")
    connection.row_factory = sqlite3.Row
    return connection

def create_tables():
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_name TEXT NOT NULL UNIQUE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS birthdays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        birthday_date DATE NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS holidays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        holiday_name TEXT NOT NULL,
        holiday_date DATE NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fiestas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location TEXT NOT NULL,
        fiesta_date DATE NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        activity_date DATE NOT NULL
    );
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO event_types (type_name)
    VALUES ('birthday'), ('holiday'), ('fiesta'), ('activity');
    """)

    connection.commit()
    cursor.close()
    connection.close()

@app.on_event("startup")
def startup_event():
    create_tables()

def str_to_date(date_str: str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

### BIRTHDAYS ###
@app.post("/birthdays/add", tags=["Birthdays"])
async def add_birthday(name: str, birthday: str):
    birthday_date = str_to_date(birthday)
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("INSERT INTO birthdays (name, birthday_date) VALUES (?, ?)", (name, birthday_date))
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()
    return {"message": "Birthday added successfully", "data": {"name": name, "birthday": birthday}}

@app.get("/birthdays", tags=["Birthdays"])
async def view_birthdays():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM birthdays")
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"data": [dict(result) for result in results]}

@app.get("/birthdays/{id}", tags=["Birthdays"])
async def get_birthday(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM birthdays WHERE id = ?", (id,))
    result = cursor.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Birthday not found")
    cursor.close()
    connection.close()
    return {"data": dict(result)}

@app.put("/birthdays/update/{id}", tags=["Birthdays"])
async def update_birthday(id: int, new_name: str, new_birthday: str):
    new_birthday_date = str_to_date(new_birthday)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE birthdays SET name = ?, birthday_date = ? WHERE id = ?", (new_name, new_birthday_date, id))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Birthday not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Birthday updated successfully"}

@app.delete("/birthdays/delete/{id}", tags=["Birthdays"])
async def delete_birthday(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM birthdays WHERE id = ?", (id,))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Birthday not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Birthday deleted successfully"}

### HOLIDAYS ###
@app.post("/holidays/add", tags=["Holidays"])
async def add_holiday(holiday_name: str, holiday_date: str):
    holiday_date_obj = str_to_date(holiday_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO holidays (holiday_name, holiday_date) VALUES (?, ?)", (holiday_name, holiday_date_obj))
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Holiday added successfully"}

@app.get("/holidays", tags=["Holidays"])
async def view_holidays():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM holidays")
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"data": [dict(result) for result in results]}

@app.get("/holidays/{id}", tags=["Holidays"])
async def get_holiday(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM holidays WHERE id = ?", (id,))
    result = cursor.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Holiday not found")
    cursor.close()
    connection.close()
    return {"data": dict(result)}

@app.put("/holidays/update/{id}", tags=["Holidays"])
async def update_holiday(id: int, new_holiday_name: str, new_holiday_date: str):
    new_holiday_date_obj = str_to_date(new_holiday_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE holidays SET holiday_name = ?, holiday_date = ? WHERE id = ?", (new_holiday_name, new_holiday_date_obj, id))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Holiday not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Holiday updated successfully"}

@app.delete("/holidays/delete/{id}", tags=["Holidays"])
async def delete_holiday(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM holidays WHERE id = ?", (id,))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Holiday not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Holiday deleted successfully"}

### FIESTAS ###
@app.post("/fiestas/add", tags=["Fiestas"])
async def add_fiesta(location: str, fiesta_date: str):
    fiesta_date_obj = str_to_date(fiesta_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO fiestas (location, fiesta_date) VALUES (?, ?)", (location, fiesta_date_obj))
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Fiesta added successfully"}

@app.get("/fiestas", tags=["Fiestas"])
async def view_fiestas():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM fiestas")
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"data": [dict(result) for result in results]}

@app.get("/fiestas/{id}", tags=["Fiestas"])
async def get_fiesta(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM fiestas WHERE id = ?", (id,))
    result = cursor.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Fiesta not found")
    cursor.close()
    connection.close()
    return {"data": dict(result)}

@app.put("/fiestas/update/{id}", tags=["Fiestas"])
async def update_fiesta(id: int, new_location: str, new_fiesta_date: str):
    new_fiesta_date_obj = str_to_date(new_fiesta_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE fiestas SET location = ?, fiesta_date = ? WHERE id = ?", (new_location, new_fiesta_date_obj, id))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Fiesta not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Fiesta updated successfully"}

@app.delete("/fiestas/delete/{id}", tags=["Fiestas"])
async def delete_fiesta(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM fiestas WHERE id = ?", (id,))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Fiesta not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Fiesta deleted successfully"}

### ACTIVITIES ###
@app.post("/activities/add", tags=["Activities"])
async def add_activity(title: str, activity_date: str):
    activity_date_obj = str_to_date(activity_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO activities (title, activity_date) VALUES (?, ?)", (title, activity_date_obj))
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Activity added successfully"}

@app.get("/activities", tags=["Activities"])
async def view_activities():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM activities")
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"data": [dict(result) for result in results]}

@app.get("/activities/{id}", tags=["Activities"])
async def get_activity(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM activities WHERE id = ?", (id,))
    result = cursor.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Activity not found")
    cursor.close()
    connection.close()
    return {"data": dict(result)}

@app.put("/activities/update/{id}", tags=["Activities"])
async def update_activity(id: int, new_title: str, new_activity_date: str):
    new_activity_date_obj = str_to_date(new_activity_date)
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE activities SET title = ?, activity_date = ? WHERE id = ?", (new_title, new_activity_date_obj, id))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Activity not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Activity updated successfully"}

@app.delete("/activities/delete/{id}", tags=["Activities"])
async def delete_activity(id: int):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM activities WHERE id = ?", (id,))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Activity not found")
    connection.commit()
    cursor.close()
    connection.close()
    return {"message": "Activity deleted successfully"}

### UPCOMING EVENTS ###
@app.get("/events/upcoming", tags=["Events"])
async def get_upcoming_events():
    connection = get_connection()
    cursor = connection.cursor()

    query = """
    SELECT 'Birthday' as event_type, name as title, birthday_date as event_date FROM birthdays
    WHERE birthday_date >= date('now')
    UNION ALL
    SELECT 'Holiday' as event_type, holiday_name as title, holiday_date as event_date FROM holidays
    WHERE holiday_date >= date('now')
    UNION ALL
    SELECT 'Fiesta' as event_type, location as title, fiesta_date as event_date FROM fiestas
    WHERE fiesta_date >= date('now')
    UNION ALL
    SELECT 'Activity' as event_type, title, activity_date as event_date FROM activities
    WHERE activity_date >= date('now')
    ORDER BY event_date;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return {"data": [dict(result) for result in results]}



    cursor = connection.cursor()
