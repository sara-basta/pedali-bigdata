"""Envoie un fichier local vers MinIO (bucket lake) sans passer par la console.
 
Lancement (dans le conteneur) :
    docker exec -it jupyter python producer/upload_to_minio.py
    docker exec -it jupyter python producer/upload_to_minio.py data/stations.json lake/bronze/ref/stations.json
"""
import os
import shutil
import sys
 
from pyarrow import fs
 
src = sys.argv[1] if len(sys.argv) > 1 else "data/trips_history.jsonl"
dst = sys.argv[2] if len(sys.argv) > 2 else "lake/bronze/history/trips_history.jsonl"
 
s3 = fs.S3FileSystem(
    access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    endpoint_override="minio:9000",
    scheme="http",
)
 
with open(src, "rb") as f_in, s3.open_output_stream(dst) as f_out:
    shutil.copyfileobj(f_in, f_out)
 
info = s3.get_file_info(dst)
print(f"OK : {src} -> s3a://{dst} ({info.size / 1e6:.1f} Mo)")