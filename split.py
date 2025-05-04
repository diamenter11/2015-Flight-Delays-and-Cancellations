# import os
# import pandas as pd

# # Config
# input_file = "Stream/flights.csv"
# output_dir = "stream"
# rows_per_file = 10000

# # Créer le dossier s'il n'existe pas
# os.makedirs(output_dir, exist_ok=True)

# # Lecture en chunks
# for i, chunk in enumerate(pd.read_csv(input_file, chunksize=rows_per_file)):
#     output_path = os.path.join(output_dir, f"chunk_{i+1}.csv")
#     chunk.to_csv(output_path, index=False)
#     print(f"{output_path} créé.")



import os
import pandas as pd
import time

# Config
source_file = "source/flights.csv"
stream_dir = "stream"
rows_per_chunk = 10000

# Créer le dossier stream s'il n'existe pas
os.makedirs(stream_dir, exist_ok=True)

# Lecture et envoi progressif
for i, chunk in enumerate(pd.read_csv(source_file, chunksize=rows_per_chunk)):
    output_path = os.path.join(stream_dir, f"chunk_{i+1}.csv")
    chunk.to_csv(output_path, index=False)
    print(f"{output_path} ajouté.")
