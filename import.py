import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
load_dotenv()

engine = create_engine(os.getenv("DB_CONNECT"))
chunksize = 100_000  # je nach RAM evtl. 50_000 oder 500_000

def filling(table:str,csv_path:str):
	first_chunk = True  # nur beim ersten Chunk die Tabelle anlegen

	for i, chunk in enumerate(pd.read_csv(csv_path, sep=",", chunksize=chunksize, dtype=str)):
		# Transaktion manuell starten
		with engine.connect() as connection:
			transaction = connection.begin()
			try:
				chunk.to_sql(
					name=table,
					con=connection,
					schema="projekt_katinka",
					if_exists="replace" if first_chunk else "append",
					index=False
				)
				transaction.commit()
				print(f"Chunk {i} verarbeitet ({len(chunk)} Zeilen)")
				first_chunk = False  # ab jetzt nur noch anhängen
			except SQLAlchemyError as e:
				print("Fehler:", e)
				transaction.rollback()
# csv_path=r"C:\Users\eve\Downloads\mystuff\Data+\mysql\projekt_katinka\wordtype.csv" 
# filling("wordtype",csv_path) 
csv_path=r"C:\Users\eve\Downloads\mystuff\Data+\mysql\projekt_katinka\sprachen.csv" 
filling("languages",csv_path)
# csv_path=r"C:\Users\eve\Downloads\mystuff\Data+\mysql\projekt_katinka\learning_traces.13m.csv" 
# filling("vocable_learning",csv_path)
