import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import traceback

# ======================================== #
# Função: Carrega DataFrame no Google Sheets
# ======================================== #
def carregar_dataframe(
    df: pd.DataFrame,
    spreadsheet_id: str,
    aba: str,
    credentials_path: str
) -> None:
    """
    Carrega um DataFrame para uma aba específica de uma planilha do Google Sheets.
    Cria a aba se ela não existir, limpa antes de escrever.

    Parâmetros:
    - df: DataFrame
    - spreadsheet_id: ID da planilha no Google Sheets
    - aba: nome da aba
    - credentials_path: caminho para JSON de credenciais
    """

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(credentials)

        planilha = client.open_by_key(spreadsheet_id)

        try:
            worksheet = planilha.worksheet(aba)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = planilha.add_worksheet(title=aba, rows="1000", cols="20")

        worksheet.clear()

        df_convertido = df.fillna("").astype(str)
        dados = [df_convertido.columns.tolist()] + df_convertido.values.tolist()
        worksheet.update("A1", dados)

        print(f"Dados carregados com sucesso na Planilha='{planilha.title}', Aba='{aba}'")

    except Exception as e:
        print(f"Erro ao carregar dados no Google Sheets: {e}")
        traceback.print_exc()

def carregar_dataframes_sheets(
    planilha_id: str,
    dataframes: dict,
    credentials_path: str
):
    """
    Recebe um dicionário com {nome_aba: dataframe} e carrega todos para a planilha indicada.
    """
    for aba, df in dataframes.items():
        carregar_dataframe(df, planilha_id, aba, credentials_path)

# Adicione isso ao seu arquivo load_to_sheets.py

def ler_dataframe_sheets(spreadsheet_id: str, aba: str, credentials_path: str) -> pd.DataFrame:
    """
    Lê os dados existentes na planilha para memória.
    Retorna um DataFrame vazio se a aba não existir ou estiver vazia.
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(credentials)
        
        planilha = client.open_by_key(spreadsheet_id)
        worksheet = planilha.worksheet(aba)
        
        # get_all_records retorna uma lista de dicionários
        dados = worksheet.get_all_records()
        
        if not dados:
            return pd.DataFrame()
            
        # Converte para DataFrame
        df = pd.DataFrame(dados)
        
        # IMPORTANTE: O gspread pode trazer números como strings dependendo da formatação
        # Tente converter colunas numéricas automaticamente
        df = df.apply(pd.to_numeric)
        
        return df

    except (gspread.exceptions.WorksheetNotFound, Exception):
        # Se der erro ou não achar a aba, assume que não tem histórico
        return pd.DataFrame()