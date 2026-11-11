import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(message)s",
	handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=dotenv_path)
if not os.getenv("S3_BUCKET_NAME"):
	print(f"DEBUG: Tentou ler de: {dotenv_path}")
	print(f"DEBUG: Arquivo existe? {dotenv_path.exists()}")
	
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DATASET_PATH = os.getenv("DATASET_PATH", "data/raw/ifood_df.csv")

def get_s3_client():
	"""Inicializa e retorna o cliente S3 com base nas credenciais configuradas."""
	try:
		return boto3.client("s3", region_name=AWS_REGION)
	except Exception as error:
		logging.error("Erro ao inicializar o cliente S3: %s", error)
		raise
def upload_file_to_s3(file_path: Path, bucket: str, s3_key: str):
	"""Realiza o upload de um arquivo local para o S3 com tratamento de erros."""
	s3_client = get_s3_client()
	try:
		logging.info(
			"Iniciando upload de '%s' para 's3://%s/%s'...",
			file_path,
			bucket,
			s3_key,
		)
		s3_client.upload_file(str(file_path), bucket, s3_key)
		logging.info("Upload concluído com sucesso!")
	except FileNotFoundError:
		logging.error("Arquivo local não encontrado: %s", file_path)
		raise
	except NoCredentialsError:
		logging.error("Credenciais AWS não encontradas para o upload do arquivo: %s", file_path)
		raise
	except ClientError as error:
		logging.error("Erro no cliente AWS S3: %s", error)
		raise
	except Exception as error:
		logging.error("Erro inesperado ao fazer upload para S3: %s", error)
		raise

def run_pipeline():
	"""Executa o fluxo de ingestão Batch para a Camada Bronze."""
	if not S3_BUCKET_NAME:
		raise ValueError("A variável S3_BUCKET_NAME não foi definida no arquivo .env!")
	raw_file = Path(DATASET_PATH)
	if not raw_file.exists():
		logging.warning(
			"Arquivo %s não encontrado. Criando arquivo de teste simulado...", raw_file
		)
		raw_file.parent.mkdir(parents=True, exist_ok=True)
		# Cria um arquivo mock caso ainda não tenha baixado o CSV
		with raw_file.open("w", encoding="utf-8") as file:
			file.write(
				"id,Income,Kidhome,Teenhome,Recency,MntWines,MntFruits,Response\n"
			)
			file.write("1,58138,0,0,58,635,88,1\n")
			file.write("2,46344,1,1,38,11,1,0\n")
		logging.info("Arquivo de teste criado em: %s", raw_file)

	# Particionamento lógico por data de ingestão (Ano/Mês/Dia)
	now = datetime.now()
	partition = (
		f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"
	)
	s3_destination_key = f"raw/ifood_crm/{partition}/{raw_file.name}"
	upload_file_to_s3(
		file_path=raw_file,
		bucket=S3_BUCKET_NAME,
		s3_key=s3_destination_key,
	)

if __name__ == "__main__":
	logging.info("Iniciando pipeline de ingestão Batch (EL)...")
	run_pipeline()
	logging.info("Pipeline finalizado.")
	