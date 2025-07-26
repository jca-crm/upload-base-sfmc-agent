import pyodbc
import pandas as pd
from pathlib import Path, PurePosixPath
import paramiko
from datetime import datetime
from time import perf_counter, sleep

# ------------------ util ------------------
DELAY_SECONDS = 0


def ts():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


CHUNK_SIZE = 500_000  # 500k por arquivo
# RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")  # OPCIONAL para identificar a execução

# ------------------ configs ------------------
# DB
server = '10.61.72.63'
database = 'StageBI'
username = 'marketingUser'
password = 'iUfeztI>e]eD'

# SFTP
sftp_host = 'mclv2q0655xn6m9pcrqst392lqp4.ftp.marketingcloudops.com'
sftp_port = 22
sftp_user = '514031163'
sftp_pass = 'Vj07VWu!SLy5hp$!'
sftp_destino = '/Import/'

# paths
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

# base do nome (cada parte receberá _partXXX)
base_name = f"CrmClientes_WMB"  # Insere RUN_ID e deixa entre parentesês assim: CrmClientes_WMB_{RUN_ID}

# conexão
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"Encrypt=no;"
)

# ------------------ SQL ------------------
sql = """
/* CTE para tratar dados de fonte */
WITH FonteTratada AS (
    SELECT DISTINCT
        CASE
            WHEN FonteDados IN ('Zoox', 'Zoox-Graal', 'Zoox-JCA', 'Zoox-RodoviariaNR', 'Zoox-TerminalRSN') THEN 'Zoox'
            WHEN FonteDados IN ('Outlet', 'Outlet de Passagens') THEN 'Outlet'
            WHEN FonteDados IN ('ClubeGiro', 'Clube Giro') THEN 'ClubeGiro'
            WHEN FonteDados IN ('WeMobi') THEN 'Wemobi'
            WHEN FonteDados IN ('Web', 'TotalBus', 'Site (UTP)', 'Catarinense', 'Expresso do Sul', 'Rápido Ribeirão', 'Viação Cometa', 'Cometa', '1001', 'SalesForce') THEN 'UTP'
            WHEN FonteDados IN ('Opção', 'MobiGo', 'Grupo JCA', 'Globus', 'Buslog', ' ') THEN 'UL'
            ELSE FonteDados
        END AS FonteDados,
        Nome,
        
        -- Melhor lógica para extrair o primeiro nome
        CASE
            WHEN CHARINDEX(' ', TRIM(Nome)) > 0 THEN LEFT(TRIM(Nome), CHARINDEX(' ', TRIM(Nome)) - 1)
            ELSE TRIM(Nome)
        END AS PrimeiroNome,
        
        Email,
        DataCadastro,
        
        -- Correção do CPF com preenchimento de zeros à esquerda
        REPLICATE('0', (11 - LEN(CPF))) + CAST(CPF AS VARCHAR(11)) AS CPF,

		DataNascimento,
        
        -- Normalização do número de celular
        '55' + REPLACE(REPLACE(REPLACE(REPLACE(Celular, '(', ''), '-', ''), ')', ''), ' ', '') AS Celular,
        
        Sexo AS Genero,
        Cidade,
        Estado
    FROM marketing.CrmClientes
    WHERE Email IS NOT NULL
          AND CPF IS NOT NULL
          AND FonteDados NOT IN ('Zoox', 'ClubeGiro', 'Outlet', 'UTP', 'UL')  -- Filtragem precoce excluindo outras fontes de dados
          --AND DataCadastro BETWEEN '2000-01-01' AND '2025-12-31'
          AND DataCadastro BETWEEN DATEADD(DAY, -7, GETDATE()) AND DATEADD(DAY, 7, GETDATE())
          AND CPF NOT IN ('00000000000', '11111111111', '22222222222', '33333333333', '44444444444',
                          '55555555555', '66666666666', '77777777777', '88888888888', '99999999999')
          AND (Email NOT LIKE '%1001%' AND Email NOT LIKE '%COMETA%' AND Email NOT LIKE '%CATARINENSE%' AND
               Email NOT LIKE '%EXPRESSO%' AND Email NOT LIKE '%RAPIDO%' AND Email NOT LIKE '%BUSLOG%' AND
               Email NOT LIKE '%INTEGRA%' AND Email NOT LIKE '%JCA%' AND Email NOT LIKE '%MACAENSE%' AND 
               Email NOT LIKE '%METAR%' AND Email NOT LIKE '%OPCAO%' AND Email NOT LIKE '%SIT%' AND 
               Email NOT LIKE '%WEMOBI%' AND Email NOT LIKE '%JCATLM%')
),

-- CTE para seleção de clientes Wemobi
ClientesWMB AS (
    SELECT
        FonteDados,
        Nome,
        PrimeiroNome,
        Email,
        DataCadastro,
        CPF,
		DataNascimento,
        Celular,
        Genero,
        Cidade,
        Estado,
        'BR' AS Locale
    FROM FonteTratada
    WHERE FonteDados = 'Wemobi'
)

/* Seleciona os clientes tratados */
SELECT * 
FROM ClientesWMB;
"""


def run() -> None:
    t0 = perf_counter()
    conn = None
    transport = None
    sftp = None
    total_rows = 0
    n_parts = 0

    print(f"[{ts()}] ▶ Iniciando o script... (chunk={CHUNK_SIZE:,})")

    try:
        # DB
        print(f"[{ts()}] 🔌 Conectando ao banco...")
        conn = pyodbc.connect(conn_str, timeout=5)
        print(f"[{ts()}] ✅ Conectado.")

        # SFTP
        print(f"[{ts()}] 🌐 Conectando ao SFTP...")
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_pass)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(f"[{ts()}] ✅ Sessão SFTP iniciada.")

        print(f"[{ts()}] 💍 Executando/streaming da query em chunks...")
        t_query = perf_counter()

        total_rows = 0

        for n_parts, chunk in enumerate(pd.read_sql(sql, conn, chunksize=CHUNK_SIZE), start=1):
            t_part = perf_counter()

            # formatações
            chunk['CPF'] = chunk['CPF'].astype(str).str.zfill(11)

            # salva local
            part_fname = OUTPUT_DIR / f"{base_name}.csv"  # Caso eu queira particionar o arquivo em n partes, usar: f"{base_name}_part{n_parts:03d}.csv"
            chunk.to_csv(part_fname, sep=';', encoding='utf-8-sig', index=False)

            # envia
            destino = str(PurePosixPath(sftp_destino) / part_fname.name)
            sftp.put(str(part_fname), destino)

            rows = len(chunk)
            total_rows += rows
            print(f"[{ts()}] 📤 part{n_parts:03d}: {rows:,} (acumulado {total_rows:,})")

            # espera 5 minutos antes do próximo chunk
            print(f"[{ts()}] ⏳ Aguardando {DELAY_SECONDS/60:.0f} minutos para o MC processar...")
            sleep(DELAY_SECONDS)

            # começando novo processo
            print(f"[{ts()}] 🔄️ Carregando próxima parte...")

        print(f"[{ts()}] ✅ Streaming finalizado em {perf_counter()-t_query:.2f}s.")
        print(f"[{ts()}] ✔ Total enviado: {total_rows:,} linhas em {n_parts} arquivo(s).")

    except KeyboardInterrupt:
        print(f"[{ts()}] ⛔ Execução cancelada pelo usuário (Ctrl+C).")
    except Exception as e:
        print(f"[{ts()}] ❌ Erro:")
        print(e)
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        if conn:
            conn.close()

        elapsed = perf_counter() - t0
        print(f"[{ts()}] ⏱ Fim. Tempo total: {elapsed:.2f}s ({elapsed/60:.2f} min)")
