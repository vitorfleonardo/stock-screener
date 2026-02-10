import yfinance as yf
import pandas as pd
import time
import random
from src.load_to_sheets import carregar_dataframes_sheets
from src.tickers import ALL_TICKERS_YAHOO
from src.info import organize_data
from src.tesouro import get_tesouro_direto
from bcb import currency
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

import numpy as np

load_dotenv()

MAX_RETRIES = 3 
CHUNK_SIZE = 50

def update_cotas():
    total_ativos = len(ALL_TICKERS_YAHOO)
    print(f"--- INICIANDO ROTINA PARA {total_ativos} ATIVOS DO YAHOO FINANCE ---")
    
    dados_mercado = yf.download(
        tickers=ALL_TICKERS_YAHOO, 
        period="1d",
        group_by='ticker', 
        auto_adjust=True,
        threads=True 
    )

    dataframe_stocks = []
    
    for ticker in ALL_TICKERS_YAHOO:
    
        tempo_espera = random.uniform(2.1, 5.0)
        time.sleep(tempo_espera)

        info = {}
        sucesso = False
        tentativa = 0

        while tentativa < MAX_RETRIES and not sucesso:
            try:
                ativo = yf.Ticker(ticker)
                info = ativo.info
                sucesso = True
            except Exception as e:
                tentativa += 1
                tempo_backoff = 2 * tentativa
                print(f"\n   -> Erro ao pegar info de {ticker} (Tentativa {tentativa}/{MAX_RETRIES}). Erro: {e}")
                print(f"   -> Tentando novamente em {tempo_backoff}s...")
                time.sleep(tempo_backoff)
        
        dados_ativo = organize_data(ticker, info, dados_mercado)

        if ( dados_ativo is None ): 
            print(f"ERRO DE PARSE em {ticker}")
            dataframe_stocks.append({'Ativo': ticker.replace('.SA', ''), 'Preço Atual': 0, 'Recomendação': 'ERRO_SCRIPT'})
        else:
            dataframe_stocks.append(dados_ativo)

    df_yahoo_updated= pd.DataFrame(dataframe_stocks)
    df_yahoo_updated['Atualizado em'] = pd.Timestamp.now(tz='America/Sao_Paulo').strftime("%Y-%m-%d %H:%M")

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {"cotacoes_yahoo_raw": df_yahoo_updated},
        "credentials.json"
    )

def update_dloar_cotacao():
    data_fim = datetime.now().strftime('%Y-%m-%d')
    data_inicio = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    df_dol = currency.get(['USD'], start=data_inicio, end=data_fim)
    cotacao_dolar = pd.DataFrame([{
        "Modeda": "USD",
        "ultima_cotacao": df_dol['USD'].iloc[-1],
        "data_cotacao": df_dol.index[-1].strftime('%d/%m/%Y')
    }])

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {"cotacoes_bcb_raw": cotacao_dolar},
        "credentials.json"
    )

def update_renda_fixa():
    df_tesouro = get_tesouro_direto()
    titulos_ipca = df_tesouro[df_tesouro['Tipo Titulo'].str.contains("IPCA")]

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {"cotacoes_tesouro_raw": titulos_ipca},
        "credentials.json"
    )

if __name__ == "__main__":
        # update_cotas()
        update_dloar_cotacao()
        # update_renda_fixa()