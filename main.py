
import threading
from pymongo.mongo_client import MongoClient
import csv
import os
import multiprocessing

uri = os.getenv("MONGO_URI")

client = MongoClient(uri)
db = client['sample_mflix']
collection = db['comments']

cursor = collection.find()
lock = threading.Lock()

CHUNK_SIZE = 1_000

os.makedirs('raw', exist_ok=True)

def get_num_threads(num_threads=None):
    if num_threads is None:
        return multiprocessing.cpu_count()
    return num_threads


def procesar_chunk(thread_id):
    chunk_count = 0
    while True:
        chunk = []
        with lock:
            try:
                for _ in range(CHUNK_SIZE):
                    chunk.append(next(cursor))
            except StopIteration:
                break

        if chunk:
            chunk_count += 1
            csv_filename = f'raw/chunk_{thread_id}_{chunk_count}.csv'
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=chunk[0].keys())
                writer.writeheader()
                writer.writerows(chunk)
            print(f'{threading.current_thread().name} procesó un chunk de {len(chunk)} documentos y lo guardó en {csv_filename}')


num_threads = get_num_threads()

threads = []
for i in range(num_threads):
    thread = threading.Thread(target=procesar_chunk, args=(i+1,), name=f'Hilo-{i+1}')
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

client.close()
