import os
import requests
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Se você já tem teste_email.py com essa função, mantenha:
from teste_email import enviar_email_com_anexo

# ==============================
# SESSION COM RETRY (RESILIENTE)
# ==============================
def make_session() -> requests.Session:
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=2,  # 2s, 4s, 8s, 16s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "DELETE"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

def http_get(url: str, headers=None, params=None) -> requests.Response:
    # timeout=(conectar, ler)
    return SESSION.get(url, headers=headers, params=params, timeout=(10, 90))


# ==============================
# CACHE DE CLIENTES (para não chamar API repetido)
# ==============================
CLIENTES_CACHE: dict[str, str] = {}

# ==============================
# DIAGNÓSTICO (pode deixar)
# ==============================
print("RODANDO:", __file__)
print("CWD:", os.getcwd())

# ==============================
# CONFIGURAÇÃO
# ==============================
API_KEY = os.environ.get("ASAAS_API_KEY")

# ✅ Padrão corrigido (API do Asaas)
BASE_URL = os.environ.get("ASAAS_BASE_URL", "https://api.asaas.com/v3").rstrip("/")

# Limite para NÃO incluir cobranças altas
LIMITE_VALOR = float(os.environ.get("MAX_PAYMENT_VALUE", "1000"))

if not API_KEY:
    raise Exception("API Key não configurada. Configure ASAAS_API_KEY nas variáveis de ambiente/Secrets.")

HEADERS = {
    "access_token": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "mecanicaautomation/1.0",
}

DEFAULT_EXPORT_PATH = os.path.join(os.getcwd(), "export")
DEFAULT_LOG_PATH = os.path.join(os.getcwd(), "logs")

EXPORT_PATH = os.environ.get("EXPORT_PATH", DEFAULT_EXPORT_PATH)
LOG_PATH = os.environ.get("LOG_PATH", DEFAULT_LOG_PATH)

# ==============================
# LOG
# ==============================
def log(message: str) -> None:
    os.makedirs(LOG_PATH, exist_ok=True)
    with open(os.path.join(LOG_PATH, "job.log"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} - {message}\n")

# ==============================
# BUSCAR NOME DO CLIENTE (via /customers/{id}) + CACHE
# ==============================
def buscar_nome_cliente(customer_id: str) -> str:
    if not customer_id:
        return ""

    if customer_id in CLIENTES_CACHE:
        return CLIENTES_CACHE[customer_id]

    url = f"{BASE_URL}/customers/{customer_id}"

    try:
        resp = http_get(url, headers=HEADERS)
        if resp.status_code == 200:
            nome = (resp.json().get("name") or "").strip()
        else:
            nome = ""
            log(f"Falha ao buscar cliente {customer_id}: HTTP {resp.status_code} - {resp.text[:200]}")
    except Exception as e:
        nome = ""
        log(f"Exceção ao buscar cliente {customer_id}: {repr(e)}")

    CLIENTES_CACHE[customer_id] = nome
    return nome

# ==============================
# BUSCAR COBRANÇAS VENCIDAS (com paginação)
# ==============================
def buscar_vencidos(limit: int = 100) -> list[dict]:
    url = f"{BASE_URL}/payments"

    offset = 0
    todos: list[dict] = []

    while True:
        params = {
            "status": "OVERDUE",
            "limit": limit,
            "offset": offset,
        }

        try:
            resp = http_get(url, headers=HEADERS, params=params)
        except Exception as e:
            log(f"Timeout/erro de rede ao consultar payments offset={offset}: {repr(e)}")
            raise

        if resp.status_code != 200:
            print("ERRO API:", resp.status_code, resp.text)
            log(f"Erro API payments: {resp.status_code} - {resp.text}")
            raise Exception(f"Erro ao consultar Asaas: HTTP {resp.status_code}")

        payload = resp.json()
        items = payload.get("data", []) or []

        if not items:
            break

        todos.extend(items)
        offset += limit

    return todos

# ==============================
# EXPORTAR PARA EXCEL (com filtro de valor + nome do cliente)
# ==============================
def recomendar_acao(dias_atraso: int, valor_total: float) -> str:
        if dias_atraso >= 90:
            return "Oferecer versão Cloud e SUSPENDER SUPORTE até regularização."
        elif dias_atraso >= 60:
            return "Oferecer versão Cloud e SUSPENDER SUPORTE até regularização."
        elif dias_atraso >= 30:
            return "Sugerido reforçar cobrança e acompanhar de perto."
        else:
            return "Sugerido lembrete amigável e acompanhamento."


def exportar_excel(dados: list[dict]):
    os.makedirs(EXPORT_PATH, exist_ok=True)

    if not dados:
        msg = "Nenhuma cobrança vencida encontrada (status=OVERDUE)."
        log(msg)
        print(msg)
        return None

    dados_filtrados: list[dict] = []
    pulados_acima = 0

    for item in dados:
        try:
            valor = float(item.get("value") or 0)
        except (TypeError, ValueError):
            valor = 0.0

        if valor < LIMITE_VALOR:
            dados_filtrados.append(item)
        else:
            pulados_acima += 1

    log(f"Filtro: mantendo < R$ {LIMITE_VALOR:.2f}. Pulados (>= limite): {pulados_acima}")
    print(f"Filtro: mantendo < R$ {LIMITE_VALOR:.2f}. Pulados (>= limite): {pulados_acima}")

    if not dados_filtrados:
        msg = f"Nenhuma cobrança vencida abaixo de R$ {LIMITE_VALOR:.2f}."
        log(msg)
        print(msg)
        return None

    linhas = []
    for item in dados_filtrados:
        customer_id = item.get("customer") or ""
        nome_cliente = buscar_nome_cliente(customer_id)

        linhas.append({
            "CustomerID": customer_id,
            "Cliente": nome_cliente,
            "ID_Pagamento": item.get("id"),
            "Valor": item.get("value"),
            "Vencimento": item.get("dueDate"),
            "Tipo": item.get("billingType"),
            "Status": item.get("status"),
            "Descricao": item.get("description"),
            "ExternalReference": item.get("externalReference"),
            "InvoiceURL": item.get("invoiceUrl"),
            "BankSlipURL": item.get("bankSlipUrl"),
        })

    df = pd.DataFrame(linhas)
    
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
        df = df[df["Valor"] < LIMITE_VALOR].copy()
        df["Vencimento_dt"] = pd.to_datetime(df["Vencimento"], errors="coerce")
        hoje = pd.Timestamp.today().normalize()

        df_resumo = df.groupby("Cliente", as_index=False).agg(
            Valor=("Valor", "sum"),
            VencimentoMaisAntigo=("Vencimento_dt", "min")
        )

        df_resumo["DiasAtraso"] = (hoje - df_resumo["VencimentoMaisAntigo"]).dt.days.fillna(0).astype(int)
        df_resumo = df_resumo.sort_values(by="Valor", ascending=False)

        df_resumo["Recomendacao"] = df_resumo.apply(
            lambda row: recomendar_acao(row["DiasAtraso"], row["Valor"]),
            axis=1
        )
                


    if df.empty:
        msg = f"Após filtro, não restou nenhuma cobrança abaixo de R$ {LIMITE_VALOR:.2f}."
        log(msg)
        print(msg)
        return None

    total_valor = df["Valor"].sum()

    if "Vencimento" in df.columns:
        df["Vencimento_dt"] = pd.to_datetime(df["Vencimento"], errors="coerce")
        df = df.sort_values(by=["Vencimento_dt", "Valor"], ascending=[True, False])
        df = df.drop(columns=["Vencimento_dt"])
    else:
        df = df.sort_values(by=["Valor"], ascending=[False])

    nome_arquivo = f"vencidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho = os.path.join(EXPORT_PATH, nome_arquivo)
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Detalhado", index=False)
        df_resumo.to_excel(writer, sheet_name="Resumo Cliente", index=False)
        from openpyxl.chart import BarChart, Reference

        ws = writer.book["Resumo Cliente"]

        chart = BarChart()
        chart.title = "Valores em Aberto por Cliente"
        chart.y_axis.title = "Valor"
        chart.x_axis.title = "Cliente"

        data = Reference(ws, min_col=2, min_row=1, max_row=len(df_resumo)+1)
        cats = Reference(ws, min_col=1, min_row=2, max_row=len(df_resumo)+1)

        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)

        ws.add_chart(chart, "E2")

    log(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)} | Limite: < R$ {LIMITE_VALOR:.2f}")
    print(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)} | Limite: < R$ {LIMITE_VALOR:.2f}")
    return caminho, total_valor, df_resumo


    

# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================
def main() -> None:
    log("===== INICIO JOB ASAAS VENCIDOS =====")
    print("Iniciando job...")

    log(f"Config: BASE_URL={BASE_URL} | LIMITE_VALOR={LIMITE_VALOR:.2f} | EXPORT_PATH={EXPORT_PATH}")
    print(f"Config: BASE_URL={BASE_URL} | LIMITE_VALOR={LIMITE_VALOR:.2f} | EXPORT_PATH={EXPORT_PATH}")

    dados = buscar_vencidos()
    print("Qtd vencidos encontrados (total):", len(dados))
    log(f"Qtd vencidos encontrados (total): {len(dados)}")

    resultado = exportar_excel(dados)

    if resultado:
        arquivo, total_valor, df_resumo = resultado

        total_formatado = f'R$ {total_valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")

        top_clientes = df_resumo.head(20)

        linhas_recomendacao = []
        for _, row in top_clientes.iterrows():
            valor_fmt = f'R$ {row["Valor"]:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
            linhas_recomendacao.append(
                f'- {row["Cliente"]}: {valor_fmt} | {row["DiasAtraso"]} dias em atraso | {row["Recomendacao"]}'
            )

        texto_recomendacoes = "\n".join(linhas_recomendacao)

        corpo_email = (
            f"Segue planilha em anexo.\n\n"
            f"Total de valores vencidos: {total_formatado}\n\n"
            f"Recomendações:\n{texto_recomendacoes}"
        )
        if arquivo:
            print("DEBUG_ASSUNTO:", f"Asaas - Clientes Vencidos Mecanicaweb | Total: {total_formatado}")
            print("DEBUG_CORPO:", f"Segue planilha em anexo.\n\nTotal de valores vencidos: {total_formatado}")
            print("DEBUG_ARQUIVO:", arquivo)
            enviar_email_com_anexo(
                assunto=f"Asaas - Clientes Vencidos Mecanicaweb | Total: {total_formatado}",
                corpo=corpo_email,
                destinatarios=[
                    "vendas@mecanicaweb.com.br",
                    "marcelino@istweb.com.br",
                    "suporte@istweb.com.br",
                    "tanigawaobk@gmail.com",
                ],
                arquivo=arquivo,
        )
            log("E-mail enviado com anexo.")
            print("E-mail enviado com anexo.")
    else:
        log("Sem arquivo para enviar por e-mail (sem vencidos no critério).")
        print("Sem arquivo para enviar por e-mail (sem vencidos no critério).")

    log("===== FIM JOB ASAAS VENCIDOS =====")
    print("Job finalizado.")

if __name__ == "__main__":
    main()