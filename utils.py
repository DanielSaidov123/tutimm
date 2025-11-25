from fastapi import UploadFile, FastAPI
import io
import csv
app=FastAPI()

@app.post("/upload-csv")
def upload_csv(file: UploadFile):
    """
    Endpoint that extracts and processes a CSV file from the request.
    Uses Python's csv library to read and parse the CSV data.
    """
    # Validate that the uploaded file is a CSV
    if file.content_type != "text/csv":
         return {"error": "File must be a CSV"}


    # Read file bytes
    content = file.file.read().decode("utf-8")

    # Parse CSV
    reader = csv.reader(io.StringIO(content))
    header = next(reader)
    rows = list(reader)

    for line in rows:
        print(line)

    return {
        "filename": file.filename,
        "content_type": file.content_type,
        "total_rows": len(rows),
        "columns": header,
        "data": rows[0:5],
        "message": f"Successfully processed CSV with {len(rows)} rows"
    }


# pip install python-multipart