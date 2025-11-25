from fastapi import FastAPI, HTTPException, UploadFile, File, Response
from pydantic import BaseModel
import sqlite3
from datetime import datetime
import uvicorn
import csv
import io

app = FastAPI(title="Car Owner Management API", version="1.0.0")

@app.middleware("http")
async def print_middleware(request, call_next):
    print(f"{request.method} {request.url.path}")
    response = await call_next(request)
    return response

DB_FILE = "car_owners_db.sqlite"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS car_owners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            email TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            year INTEGER NOT NULL,
            color TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            created_at TEXT,
            FOREIGN KEY(owner_id) REFERENCES car_owners(id)
        )
    """)
    conn.commit()
    conn.close()

init_db()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def row_to_dict(row, table="car_owners"):
    if table == "car_owners":
        return dict(row)
    else:
        return dict(row)

# --- Pydantic Models ---
class CarOwner(BaseModel):
    id: int | None = None
    name: str
    age: int
    email: str
    created_at: str | None = None

class CarOwnerUpdate(BaseModel):
    name: str | None = None
    age: int | None = None
    email: str | None = None

class Car(BaseModel):
    id: int | None = None
    brand: str
    model: str
    year: int
    color: str
    owner_id: int
    created_at: str | None = None

class CarUpdate(BaseModel):
    brand: str | None = None
    model: str | None = None
    year: int | None = None
    color: str | None = None
    owner_id: int | None = None

# --- Helper Functions ---
def read_car_owners():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM car_owners")
    rows = cursor.fetchall()
    conn.close()
    return [row_to_dict(row, "car_owners") for row in rows]

def get_car_owner_by_id(owner_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM car_owners WHERE id=?", (owner_id,))
    row = cursor.fetchone()
    conn.close()
    return row_to_dict(row, "car_owners") if row else None

def create_car_owner_in_db(owner: CarOwner):
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    cursor.execute(
        "INSERT INTO car_owners (name, age, email, created_at) VALUES (?, ?, ?, ?)",
        (owner.name, owner.age, owner.email, now)
    )
    conn.commit()
    owner_id = cursor.lastrowid
    conn.close()
    return get_car_owner_by_id(owner_id)

def update_car_owner_in_db(owner_id: int, owner_update: CarOwnerUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()
    fields = []
    values = []
    for key, value in owner_update.dict(exclude_unset=True).items():
        fields.append(f"{key}=?")
        values.append(value)
    if not fields:
        conn.close()
        return get_car_owner_by_id(owner_id)
    values.append(owner_id)
    cursor.execute(f"UPDATE car_owners SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    conn.close()
    return get_car_owner_by_id(owner_id)

def delete_car_owner_from_db(owner_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM cars WHERE owner_id=?", (owner_id,))
    if cursor.fetchone()[0] > 0:
        conn.close()
        return False
    cursor.execute("DELETE FROM car_owners WHERE id=?", (owner_id,))
    conn.commit()
    conn.close()
    return True

def read_cars(owner_id: int | None = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    if owner_id:
        cursor.execute("SELECT * FROM cars WHERE owner_id=?", (owner_id,))
    else:
        cursor.execute("SELECT * FROM cars")
    rows = cursor.fetchall()
    conn.close()
    return [row_to_dict(row, "cars") for row in rows]

def get_car_by_id(car_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cars WHERE id=?", (car_id,))
    row = cursor.fetchone()
    conn.close()
    return row_to_dict(row, "cars") if row else None

def validate_owner_exists(owner_id: int):
    return get_car_owner_by_id(owner_id) is not None

def create_car_in_db(car: Car):
    if not validate_owner_exists(car.owner_id):
        raise HTTPException(status_code=400, detail="Owner not found")
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    cursor.execute(
        "INSERT INTO cars (brand, model, year, color, owner_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (car.brand, car.model, car.year, car.color, car.owner_id, now)
    )
    conn.commit()
    car_id = cursor.lastrowid
    conn.close()
    return get_car_by_id(car_id)

def update_car_in_db(car_id: int, car_update: CarUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()
    fields = []
    values = []
    update_dict = car_update.dict(exclude_unset=True)
    if "owner_id" in update_dict and not validate_owner_exists(update_dict["owner_id"]):
        raise HTTPException(status_code=400, detail="Owner not found")
    for key, value in update_dict.items():
        fields.append(f"{key}=?")
        values.append(value)
    if not fields:
        conn.close()
        return get_car_by_id(car_id)
    values.append(car_id)
    cursor.execute(f"UPDATE cars SET {', '.join(fields)} WHERE id=?", values)
    conn.commit()
    conn.close()
    return get_car_by_id(car_id)

def delete_car_from_db(car_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cars WHERE id=?", (car_id,))
    conn.commit()
    conn.close()
    return True

# --- CSV Functions ---
def export_car_owners_to_csv():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM car_owners")
    rows = cursor.fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['id','name','age','email','created_at'])
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))
    return output.getvalue()

def export_cars_to_csv(owner_id: int | None = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    if owner_id:
        cursor.execute("SELECT * FROM cars WHERE owner_id=?", (owner_id,))
    else:
        cursor.execute("SELECT * FROM cars")
    rows = cursor.fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['id','brand','model','year','color','owner_id','created_at'])
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))
    return output.getvalue()

def import_car_owners_from_csv(csv_content: bytes):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        csv_text = csv_content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(csv_text))
        count = 0
        now = datetime.now().isoformat()
        for row in reader:
            if not row.get("name") or not row.get("email"):
                continue
            cursor.execute(
                "INSERT OR IGNORE INTO car_owners (name, age, email, created_at) VALUES (?, ?, ?, ?)",
                (row["name"], int(row.get("age",0)), row["email"], now)
            )
            count += 1
        conn.commit()
        return {"message": f"Imported {count} car owners", "imported": count, "uploaded_at": now}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

def import_cars_from_csv(csv_content: bytes):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        csv_text = csv_content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(csv_text))
        count = 0
        now = datetime.now().isoformat()
        for row in reader:
            owner_id = int(row.get("owner_id",0))
            if not validate_owner_exists(owner_id):
                continue
            cursor.execute(
                "INSERT INTO cars (brand, model, year, color, owner_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (row["brand"], row["model"], int(row["year"]), row["color"], owner_id, now)
            )
            count +=1
        conn.commit()
        return {"message": f"Imported {count} cars", "imported": count, "uploaded_at": now}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# --- Endpoints ---

@app.get("/")
def root():
    return {"message": "Welcome to Car Owner API", "version":"1.0.0"}

# -------------------
# Car Owners
# -------------------

# קבועים קודם
@app.get("/car-owners")
def get_all_car_owners():
    return read_car_owners()

@app.get("/car-owners/export-csv")
def export_owners_csv():
    csv_data = export_car_owners_to_csv()
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="car_owners.csv"'}
    )

@app.post("/car-owners", status_code=201)
def create_owner(owner: CarOwner):
    return create_car_owner_in_db(owner)

@app.post("/car-owners/upload-csv")
async def upload_owners_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type")
    content = await file.read()
    return import_car_owners_from_csv(content)

# דינמיים בסוף
@app.get("/car-owners/{owner_id}")
def get_owner(owner_id: int):
    owner = get_car_owner_by_id(owner_id)
    if not owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return owner

@app.put("/car-owners/{owner_id}")
def update_owner(owner_id: int, owner_update: CarOwnerUpdate):
    owner = update_car_owner_in_db(owner_id, owner_update)
    if not owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return owner

@app.delete("/car-owners/{owner_id}", status_code=204)
def delete_owner(owner_id: int):
    if not delete_car_owner_from_db(owner_id):
        raise HTTPException(status_code=400, detail="Owner has cars or not found")
    return Response(status_code=204)

# -------------------
# Cars
# -------------------

# קבועים קודם
@app.get("/cars")
def get_cars(owner_id: int | None = None):
    return read_cars(owner_id)

@app.get("/cars/export-csv")
def export_cars_csv_endpoint(owner_id: int | None = None):
    csv_data = export_cars_to_csv(owner_id)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="cars.csv"'}
    )

@app.post("/cars", status_code=201)
def create_new_car(car: Car):
    return create_car_in_db(car)

@app.post("/cars/upload-csv")
async def upload_cars_csv_endpoint(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type")
    content = await file.read()
    return import_cars_from_csv(content)

# דינמיים בסוף
@app.get("/cars/{car_id}")
def get_single_car(car_id: int):
    car = get_car_by_id(car_id)
    if not car:
        raise HTTPException(status_code=404, detail="Car not found")
    return car

@app.put("/cars/{car_id}")
def update_car_endpoint(car_id: int, car_update: CarUpdate):
    car = update_car_in_db(car_id, car_update)
    if not car:
        raise HTTPException(status_code=404, detail="Car not found")
    return car

@app.delete("/cars/{car_id}", status_code=204)
def delete_car_endpoint(car_id: int):
    delete_car_from_db(car_id)
    return Response(status_code=204)

# -------------------
# Run
# -------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
