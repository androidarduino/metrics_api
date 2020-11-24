import json
import time
from datetime import datetime, timedelta
from fastapi import FastAPI, File, Header
from typing import Optional

from thumbnail import default_granularity, get_dates_in_range, get_file_list, get_thumbnail_list, retrieve_and_merge

app = FastAPI()


# Upload LSY data blob every 300 seconds
@app.post("/files/{child_id}/{starts}/{ends}")
async def create_file(child_id, starts, ends, file: bytes = File(...), status_code=201,
                      authentication: Optional[str] = Header(None)):
    # starts and ends: epoch time stamps for data block starts and ends

    # Store the file into GCS, trigger thumbnail function if needed
    print("uploading file... for child: ", child_id)
    # Validate parameters
    params = {child_id: "guid", }
    s1 = time.localtime(starts)
    date1, time1 = time.strftime('%Y%m%d', s1), time.strftime('%H%M%S')
    s2 = time.localtime(ends)
    date2, time2 = time.strftime('%Y%m%d', s1), time.strftime('%H%M%S')

    _, compressed_length = gcs.put(f"{child_id}/{date1}/5m_{time1}_{time2}.gzip", file)
    # If the data pack cross two days, put it in both days' buckets
    if date1 != date2:
        _, compressed_length = gcs.put(f"{child_id}/{date2}/5m_{time1}_{time2}.gzip", file)

    # @todo: generate pyramid thumbnails for the data

    return {"file_size": len(file), "compressed_size": compressed_length}


# Query data range
@app.post("/get/{child_id}/{starts}/{ends}/{granularity}")
async def query(child_id, starts, ends, granularity):
    # If granularity not specified, define it by range
    if not granularity:
        granularity = default_granularity(int(ends) - int(starts))
    if granularity == '5m' or granularity == '1h':
        # Extract dates, look into the directories between the date range and retrieve a file list that matches
        dates = get_dates_in_range(starts, ends)
        files = get_file_list(child_id, dates, granularity, starts, ends)
        # Read all files, label all data items, assemble them together and return
        return retrieve_and_merge(files)
    else:
        # Look into the child root directory for thumbnail files
        files = get_thumbnail_list(child_id, granularity, starts, ends)
        return retrieve_and_merge(files)


