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
sftp_user = '514031167'
sftp_pass = 'Vj05VWu!SLy5hp$!'
sftp_destino = '/Import/'

# paths
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

# base do nome (cada parte receberá _partXXX)
base_name = f"CrmTransacional_UTP"  # Insere RUN_ID e deixa entre parentesês assim: CrmTransacional_UTP_{RUN_ID}

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
SELECT DISTINCT
    TransFonteId,
    Bilhete,
    DataCompra,
    DataViagem,
    CASE WHEN TokenVoucher IS NULL THEN '' ELSE TokenVoucher END AS Cupom,
    CASE WHEN TaxaEmbarque IS NULL THEN '0.00' ELSE CAST(TaxaEmbarque AS VARCHAR) END AS TaxaEmbarque,
    CASE WHEN Pedagio IS NULL THEN '0.00' ELSE CAST(Pedagio AS VARCHAR) END AS Pedagio,
    CASE WHEN Tarifa IS NULL THEN '0.00' ELSE CAST(Tarifa AS VARCHAR) END AS Tarifa,
    CASE WHEN TarifaBase IS NULL THEN '0.00' ELSE CAST(TarifaBase AS VARCHAR) END AS TarifaBase,
    CASE WHEN ValorCobrado IS NULL THEN '0.00' ELSE CAST(REPLACE(ValorCobrado, ',', '.') AS VARCHAR) END AS ValorCobrado,
    CASE WHEN ValorSeguro IS NULL THEN '0.00' ELSE CAST(REPLACE(ValorSeguro, ',', '.') AS VARCHAR) END AS ValorSeguro,
    CASE WHEN ValorVoucher IS NULL THEN '0.00' ELSE CAST(REPLACE(ValorVoucher, ',', '.') AS VARCHAR) END AS ValorCupom,
    CASE WHEN TaxaComissao IS NULL THEN '0.00' ELSE CAST(REPLACE(TaxaComissao, ',', '.') AS VARCHAR) END AS TaxaComissao,
    CASE WHEN TaxaConveniencia IS NULL THEN '0.00' ELSE CAST(REPLACE(TaxaConveniencia, ',', '.') AS VARCHAR) END AS TaxaConveniencia,
    CASE WHEN KM IS NULL THEN '0.00' ELSE CAST(REPLACE(KM, ',', '.') AS VARCHAR) END AS KM,
    CASE WHEN PlataformaEmbarque IS NULL THEN '' ELSE PlataformaEmbarque END AS PlataformaEmbarque,
    FormaPagamento,
    CASE WHEN BandeiraCartao IS NULL THEN '' ELSE BandeiraCartao END AS BandeiraCartao,
    CASE WHEN QtdParcelas IS NULL THEN '0' ELSE CAST(QtdParcelas AS VARCHAR) END AS QtdParcelas,
    Moeda,
    CASE
        WHEN EmpresaVenda LIKE 'AUTO VIACAO 1001 LTDA' THEN '1001'
        WHEN EmpresaVenda LIKE 'AUTO VIACAO CATARINENSE LTDA' THEN 'CATARINENSE'
        WHEN EmpresaVenda LIKE 'EXPRESSO DO SUL S A' THEN 'EXPRESSO DO SUL'
        WHEN EmpresaVenda LIKE 'RAPIDO RIBEIRAO PRETO LTDA' THEN 'RÁPIDO RIBEIRÃO'
        WHEN EmpresaVenda LIKE 'VIACAO COMETA S A' THEN 'COMETA'
        ELSE EmpresaVenda
    END AS EmpresaVenda,
    TipoPassageiro,
    ServicoId,
    TipoCarro,
    ChegadaProgramada,
    TempoPercurso,
    SentidoLinha,
    CASE WHEN IdaVolta IS NULL THEN '' ELSE IdaVolta END AS IdaVolta,
    OrigemId,
    Origem,
    EstadoOrigemId,
    EstadoOrigem,
    DestinoId,
    Destino,
    EstadoDestinoId,
    EstadoDestino,
    Classe,
    CanalVenda,
    Poltrona,
    CASE WHEN Localizador IS NULL THEN '' ELSE Localizador END AS Localizador,
    CASE WHEN NomePassageiro IS NULL THEN '' ELSE NomePassageiro END AS NomePassageiro,
    REPLICATE('0', (11 - LEN(CPF))) + CAST(CPF AS VARCHAR(11)) AS CPF,
    CASE WHEN Documento IS NULL THEN '' ELSE Documento END AS Documento,
    CASE WHEN Documento2 IS NULL THEN '' ELSE Documento2 END AS Documento2,
    CASE WHEN DocumentoComprador IS NULL THEN '' ELSE DocumentoComprador END AS DocumentoComprador,
    CASE WHEN NomeComprador IS NULL THEN '' ELSE NomeComprador END AS NomeComprador,
    CASE WHEN NomeCartao IS NULL THEN '' ELSE NomeCartao END AS NomeCartao,
	StatusPassagem
FROM (
    SELECT 
        TransFonteId,
        Bilhete,
        DataCompra,
        DataViagem,
        TokenVoucher,
        TaxaEmbarque,
        Pedagio,
        Tarifa,
        TarifaBase,
        ValorCobrado,
        ValorSeguro,
        ValorVoucher,
        TaxaComissao,
        TaxaConveniencia,
        KM,
        PlataformaEmbarque,
        FormaPagamento,
        BandeiraCartao,
        QtdParcelas,
        Moeda,
        EmpresaVenda,
        TipoPassageiro,
        ServicoId,
        TipoCarro,
        ChegadaProgramada,
        TempoPercurso,
        SentidoLinha,
        IdaVolta,
        OrigemId,
        Origem,
        EstadoOrigemId,
        EstadoOrigem,
        DestinoId,
        Destino,
        EstadoDestinoId,
        EstadoDestino,
        Classe,
        CASE
			WHEN CanalVenda LIKE 'BUSES' OR CanalVenda LIKE 'OUTLET' THEN 'OUTLET'
            WHEN CanalVenda LIKE 'PROPRIAS' OR CanalVenda LIKE 'CLICK BUS' OR CanalVenda LIKE 'OUTLLET' OR CanalVenda LIKE 'FRANQUEADAS' THEN 'WEMOBI'
            ELSE CanalVenda
        END AS CanalVenda,
        Poltrona,
        Localizador,
        NomePassageiro,
        CPF,
        Documento,
        Documento2,
        DocumentoComprador,
        NomeComprador,
        NomeCartao,
		StatusPassagem
    FROM marketing.CrmTransacional
    WHERE DataViagem IS NOT NULL
          AND CPF IS NOT NULL
          AND Classe NOT LIKE '%URBANO%'
		  --AND DataViagem BETWEEN '2020-01-01' AND '2020-03-31'
          AND DataViagem BETWEEN DATEADD(DAY, -7, GETDATE()) AND DATEADD(DAY, 7, GETDATE())
          AND CPF NOT IN ('00000000000', '11111111111', '22222222222', '33333333333', '44444444444',
                          '55555555555', '66666666666', '77777777777', '88888888888', '99999999999')
) AS subquery
WHERE CanalVenda NOT IN ('CLUBE GIRO', 'OUTLET', 'UTP WEMOBI', 'WEMOBI')
ORDER BY TransFonteId ASC;
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
        print(f"[{ts()}] 🗑 Conectando ao banco...")
        conn = pyodbc.connect(conn_str, timeout=5)
        print(f"[{ts()}] ✅ Conectado.")

        # SFTP
        print(f"[{ts()}] 🌐 Conectando ao SFTP...")
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_pass)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(f"[{ts()}] ✅ Sessão SFTP iniciada.")

        print(f"[{ts()}] 👅 Executando/streaming da query em chunks...")
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
