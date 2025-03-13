import requests
import paramiko
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import os
import json
import psutil
import time
from google.cloud import storage
import datetime
import pytz



# Configurações do SFTP
SFTP_HOST = 'sftp.marchon.com.br'
SFTP_PORT = 2221
SFTP_USERNAME = 'CompreOculos'
SFTP_PASSWORD = '@CMPCLS$2023'
REMOTE_DIR = 'COMPREOCULOS/ESTOQUE'
FILE_TO_CHECK = 'estoque_disponivel.csv'

# Configuração da API
API_URL = 'https://api.bling.com.br/Api/v3/estoques'
LOG_FILE = "log_envio_api.log"
# Definição do ID do depósito
DEPOSITO_ID = 14888163276  # Substitua pelo ID do depósito desejado


BUCKET_NAME = "apibling"  # Nome do bucket no Google Cloud

def log_envio(mensagem):
    """Salva logs localmente, imprime na tela e envia para o bucket."""
    brt_tz = pytz.timezone('America/Sao_Paulo')
    data_hora = datetime.datetime.now(brt_tz).strftime("%Y-%m-%d %H:%M:%S")

    log_mensagem = f"[{data_hora}] {mensagem}"

    # Salvar no arquivo local
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(log_mensagem + "\n")

    # Exibir no terminal (para ver no Cloud Shell)
    print(log_mensagem)

    # Enviar para o bucket
    enviar_log_para_bucket()
def enviar_log_para_bucket():
    """Envia o arquivo de log para o Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"logs/{LOG_FILE}")  # Salva dentro da pasta "logs"

        blob.upload_from_filename(LOG_FILE)
        print(f"✅ Log enviado para {BUCKET_NAME}/logs/{LOG_FILE}")
    except Exception as e:
        print(f"⚠ Erro ao enviar log para o bucket: {e}")

def conectar_sftp():
    """Conecta ao servidor SFTP e retorna uma sessão."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Conectando ao servidor SFTP...")
        client.connect(SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD)
        return client.open_sftp()
    except Exception as e:
        print(f"Erro ao conectar ao servidor SFTP: {e}")
        return None
def baixar_arquivo_sftp(sftp, remote_file_path, local_file_path):
    """Baixa um arquivo do SFTP para a máquina local."""
    try:
        print(f"Baixando o arquivo {remote_file_path}...")
        start_time = time.time()
        sftp.get(remote_file_path, local_file_path)
        end_time = time.time()
        download_time = end_time - start_time
        print(f"Arquivo baixado para {local_file_path} em {download_time:.2f} segundos.")
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")

def ler_planilha_sftp(caminho_arquivo):
    """Lê e processa o arquivo CSV baixado do SFTP."""
    try:
        sftp_df = pd.read_csv(caminho_arquivo)
        print(f"Arquivo do SFTP carregado com {sftp_df.shape[0]} linhas.")
        sftp_df[['codigo_produto', 'balanco']] = sftp_df.iloc[:, 0].str.split(';', expand=True)
        sftp_df['balanco'] = sftp_df['balanco'].astype(float)
        return sftp_df[['codigo_produto', 'balanco']]
    except Exception as e:
        print(f"Erro ao ler a planilha do SFTP: {e}")
        return None
def ler_planilha_usuario():
    """Solicita ao usuário a seleção de uma planilha e lê seus dados."""
    root = Tk()
    root.withdraw()
    caminho_planilha = askopenfilename(title="Selecione a planilha Excel", filetypes=[("Arquivos Excel", "*.xlsx *.xls")])
    if not caminho_planilha:
        print("Nenhuma planilha foi selecionada.")
        return None

    try:
        df = pd.read_excel(caminho_planilha)
        if df.shape[1] < 3:
            raise ValueError("A planilha deve conter pelo menos 3 colunas.")
        return pd.DataFrame({
            "id_usuario": df.iloc[:, 1].astype(str).str.strip(),
            "codigo_produto": df.iloc[:, 2].astype(str).str.strip()
        })
    except Exception as e:
        print(f"Erro ao ler a planilha: {e}")
        return None
def buscar_correspondencias(sftp_df, usuario_df):
    """Faz a correspondência entre os produtos do usuário e os do SFTP."""
    if sftp_df is None or usuario_df is None:
        print("Erro: Arquivos de entrada não carregados corretamente.")
        return pd.DataFrame()

    resultado = usuario_df.merge(sftp_df, on="codigo_produto", how="left")
    return resultado



def solicitar_bearer_token():
    """Solicita o Bearer Token ao usuário antes do envio à API."""
    token = input("\n🔑 Insira o Bearer Token para autenticação na API: ").strip()
    if not token:
        print("⚠ Erro: O token não pode estar vazio.")
        return None
    return token

def ajustar_estoque(valor):
    """Ajusta o valor do estoque, subtraindo 10, garantindo que não fique negativo."""
    return max(0, valor - 10)
def enviar_dados_api(resultado_df, deposito_id):
    """Envia os dados processados para a API do Bling."""
    if resultado_df.empty:
        print("Nenhum dado para enviar à API.")
        return

    # Ajustar o estoque antes de enviar
    resultado_df['balanco'] = resultado_df['balanco'].apply(ajustar_estoque)

    token = solicitar_bearer_token()
    if not token:
        print("⚠ Token inválido. Cancelando envio de dados.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    session = requests.Session()
    session.headers.update(headers)

    log_envio("\n🔍 Iniciando envio de dados para a API...\n")
    # Contador de envios bem-sucedidos
    contador_envios = 0
    total_bytes_enviados = 0
    start_time = time.time()

    for _, row in resultado_df.iterrows():
        if pd.notna(row["balanco"]) and pd.notna(row["id_usuario"]):
            payload = {
                "produto": {
                    "id": int(row["id_usuario"]),
                    "codigo": row["codigo_produto"]
                },
                "deposito": {
                    "id": deposito_id
                },
                "operacao": "B",
                "preco": 100,
                "custo": 10,
                "quantidade": row["balanco"],
                "observacoes": "Atualização de estoque via script"
            }
            try:
                # Verifica se o balanço é maior que zero antes de enviar
                if row["balanco"] > 0:
                    send_start_time = time.time()  # Início do envio
                    response = session.post(API_URL, json=payload)
                    send_end_time = time.time()  # Fim do envio
                    total_bytes_enviados += len(json.dumps(payload).encode('utf-8'))
                    
                    log_msg = f"\n📦 Enviado para API:\n{json.dumps(payload, indent=2)}"
                    
                    if response.status_code in [200, 201]:
                        log_envio(f"✔ Sucesso [{response.status_code}]: Produto {row['codigo_produto']} atualizado na API.{log_msg}")
                        contador_envios += 1  # Incrementa o contador de envios
                    else:
                        log_envio(f"❌ Erro [{response.status_code}]: {response.text}{log_msg}")
                    # Calcular o tempo de resposta do servidor
                    response_time = send_end_time - send_start_time
                    log_envio(f"⏱ Tempo de resposta do servidor para {row['codigo_produto']}: {response_time:.2f} segundos")
                else:
                    log_envio(f"⚠ Produto {row['codigo_produto']} não enviado, balanço igual a zero.")

            except Exception as e:
                log_envio(f"❌ Erro ao enviar {row['codigo_produto']}: {e}")

    end_time = time.time()
    total_time = end_time - start_time
    upload_speed = total_bytes_enviados / total_time if total_time > 0 else 0
    cpu_usage = psutil.cpu_percent(interval=1)

    # Log do total de envios
    log_envio(f"\n✅ Envio finalizado! Total de IDs enviados: {contador_envios}")
    log_envio(f"⏱ Tempo total de envio: {total_time:.2f} segundos")
    log_envio(f"📊 Velocidade de upload: {upload_speed / 1024:.2f} KB/s")
    log_envio(f"🖥 Uso de CPU: {cpu_usage}%")
def salvar_planilha_resultado(resultado_df, nome_arquivo="resultado_correspondencias.xlsx"):
    """Salva os resultados da correspondência localmente e no bucket do Google Cloud."""
    try:
        resultado_df.to_excel(nome_arquivo, index=False)
        print(f"Resultados salvos em {os.path.abspath(nome_arquivo)}")

        # Enviar para o bucket
        salvar_no_bucket("apibling", nome_arquivo, f"resultados/{nome_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar os resultados: {e}")


def salvar_no_bucket(bucket_name, source_file_name, destination_blob_name):
    """Salva um arquivo no bucket do Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f"📂 Arquivo {source_file_name} enviado para o bucket {bucket_name} como {destination_blob_name}.")
    except Exception as e:
        print(f"❌ Erro ao salvar {source_file_name} no bucket: {e}")
def main():
    sftp = conectar_sftp()
    if not sftp:
        print("Conexão com o SFTP falhou. Finalizando o script.")
        return

    local_file_path = FILE_TO_CHECK
    remote_file_path = f"{REMOTE_DIR}/{FILE_TO_CHECK}"
    baixar_arquivo_sftp(sftp, remote_file_path, local_file_path)
    sftp.close()

    sftp_df = ler_planilha_sftp(local_file_path)
    usuario_df = ler_planilha_usuario()

    if sftp_df is None or usuario_df is None:
        return

    resultados = buscar_correspondencias(sftp_df, usuario_df)
    salvar_planilha_resultado(resultados)

    # Usar o DEPOSITO_ID definido no início
    enviar_dados_api(resultados, DEPOSITO_ID)

if __name__ == "__main__":
    main()
