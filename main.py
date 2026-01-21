import yfinance as yf
import pandas as pd
from datetime import datetime
from load_to_sheets import carregar_dataframes_sheets
import os
from dotenv import load_dotenv

load_dotenv()

br_stocks = [
    # Ações (Stocks)
    'ITUB4', 'BBAS3', 'BPAC11', 'ROXO34', 'BBSE3', 'CXSE3', 'ITSA4', 'SOJA3', 
    'MDIA3', 'LREN3', 'AZZA3', 'VIVA3', 'GRND3', 'VULC3', 'EZTC3', 'JHSF3', 
    'LAVV3', 'RENT3', 'COGN3', 'RADL3', 'ODPV3', 'WEGE3', 'MILS3', 'GOAU4', 
    'KLBN4', 'VIVT3', 'EGIE3', 'EQTL3', 'ISAE4', 'AURE3', 'SBSP3', 'SAPR11', 
    'BMOB3',
    
    # FIIs
    'HGLG11', 'BTLG11', 'XPML11', 'ALZR11', 'VISC11', 'LVBI11', 'KNCR11', 
    'KNRI11', 'TRXF11', 'HFOF11', 'HGBS11', 'PVBI11', 'BRCR11', 'GARE11', 
    'MXRF11', 'GGRC11', 'BRCO11', 'MCCI11', 'HSML11', 'RBRR11', 'KNHF11'
]

tickers_sa = [f"{t}.SA" for t in br_stocks]

def update_sheet_bulk():
    dados = yf.download(tickers_sa, period="1d", group_by='ticker', auto_adjust=True)
    df_close = dados.xs('Close', level=1, axis=1)
    df_final = df_close.T

    df_final.columns = ['Preço'] 
    df_final.index.name = 'Ativo' 
    df_final = df_final.reset_index()

    df_final['Ativo'] = df_final['Ativo'].str.replace('.SA', '')
    df_final['Atualizado em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_final = df_final.fillna(0)

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {
            os.getenv("GS_ABA"): df_final,
        },
        os.getenv("GCP_CREDENTIALS_JSON", "credentials.json")
    )

if __name__ == "__main__":
        update_sheet_bulk()