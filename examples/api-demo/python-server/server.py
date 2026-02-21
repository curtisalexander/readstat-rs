"""FastAPI server wrapping readstat via PyO3 bindings."""

import json

from fastapi import FastAPI, File, Query, UploadFile
from fastapi.responses import JSONResponse, Response

import readstat_py

app = FastAPI(title="readstat API server (Python)")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/metadata")
async def metadata(file: UploadFile = File(...)):
    data = await file.read()
    md_json = readstat_py.read_metadata(data)
    return JSONResponse(content=json.loads(md_json))


@app.post("/preview")
async def preview(file: UploadFile = File(...), rows: int = Query(default=10)):
    data = await file.read()
    csv_bytes = readstat_py.read_to_csv(data, rows)
    return Response(content=csv_bytes, media_type="text/csv")


@app.post("/data")
async def convert(
    file: UploadFile = File(...),
    format: str = Query(default="csv"),
):
    data = await file.read()

    if format == "csv":
        content = readstat_py.read_to_csv(data)
        return Response(content=content, media_type="text/csv")
    elif format == "ndjson":
        content = readstat_py.read_to_ndjson(data)
        return Response(content=content, media_type="application/x-ndjson")
    elif format == "parquet":
        content = readstat_py.read_to_parquet(data)
        return Response(
            content=content,
            media_type="application/octet-stream",
            headers={"Content-Disposition": 'attachment; filename="data.parquet"'},
        )
    elif format == "feather":
        content = readstat_py.read_to_feather(data)
        return Response(
            content=content,
            media_type="application/octet-stream",
            headers={"Content-Disposition": 'attachment; filename="data.feather"'},
        )
    else:
        return JSONResponse(
            status_code=400,
            content={
                "error": f"Unknown format '{format}'. Use csv, ndjson, parquet, or feather."
            },
        )
