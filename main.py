from matplotlib import pyplot as plt
import yfinance as yf
import pandas as pd
import time
import random
from load_to_sheets import carregar_dataframes_sheets
import os
from dotenv import load_dotenv

load_dotenv()

MAX_RETRIES = 3 
CHUNK_SIZE = 50

stocks_list = [
    # Ações
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
tickers_sa = [f"{t}.SA" for t in stocks_list]
    

def update_sheet_bulk():
    total_ativos = len(tickers_sa)
    print(f"--- INICIANDO ROTINA PARA {total_ativos} ATIVOS ---")
    
    try:
        dados_mercado = yf.download(
            tickers=tickers_sa, 
            period="1d",
            group_by='ticker', 
            auto_adjust=True,
            threads=True 
        )
    except Exception as e:
        print(f"Erro crítico no download em lote: {e}")
        return

    dataframe_stocks = []
    
    for i, ticker in enumerate(tickers_sa):
        progresso = f"[{i+1:02d}/{total_ativos}]"

        tempo_espera = random.uniform(2.1, 5.0)
        print(f"{progresso} Aguardando {tempo_espera:.1f}s...", end=" ")
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

        try:

            preco_close = 0.0
            preco_open = 0.0
            preco_high = 0.0
            preco_low = 0.0
            vol = 0

            if ticker in dados_mercado.columns.levels[0]:
                if not dados_mercado[ticker].empty:
                    try:
                        preco_close = dados_mercado[ticker]['Close'].iloc[0]
                        preco_open = dados_mercado[ticker]['Open'].iloc[0]
                        preco_high = dados_mercado[ticker]['High'].iloc[0]
                        preco_low = dados_mercado[ticker]['Low'].iloc[0]
                        vol = dados_mercado[ticker]['Volume'].iloc[0]
                    except:
                        pass

            dados_ativo = {
                'Ativo': ticker.replace('.SA', ''),

                # --- DADOS DE MERCADO ---
                'Preço Atual': preco_close,
                'Abertura': preco_open,
                'Máxima': preco_high,
                'Mínima': preco_low,
                'Volume': vol,
                'Média Volume (10d)': info.get('averageVolume10days', 0),
                'Beta': info.get('beta', 0),

                # --- TENDÊNCIA (MÉDIAS MÓVEIS) --- 
                'Média Móvel 50d': info.get('fiftyDayAverage', 0),  
                'Média Móvel 200d': info.get('twoHundredDayAverage', 0), 

                # --- VALUATION (Múltiplos) ---
                'P/L (Atual)': info.get('trailingPE', 0),
                'P/L (Projetado)': info.get('forwardPE', 0),
                'P/VP': info.get('priceToBook', 0),
                'PEG Ratio': info.get('pegRatio', 0),
                'P/S (Preço/Venda)': info.get('priceToSalesTrailing12Months', 0),
                'EV/EBITDA': info.get('enterpriseToEbitda', 0),
                'EV/Receita': info.get('enterpriseToRevenue', 0),
                'Valor de Mercado': info.get('marketCap', 0),
                'Enterprise Value': info.get('enterpriseValue', 0),

                # --- DADOS POR AÇÃO (Fundamental) ---
                'LPA (Lucro p/ Ação)': info.get('trailingEps', 0),      
                'VPA (Valor Patrimonial p/ Ação)': info.get('bookValue', 0),

                # --- EFICIÊNCIA & RENTABILIDADE ---
                'ROE': (info.get('returnOnEquity', 0) or 0) * 100,
                'ROA': (info.get('returnOnAssets', 0) or 0) * 100,
                'Margem Líquida': (info.get('profitMargins', 0) or 0) * 100,
                'Margem Operacional': (info.get('operatingMargins', 0) or 0) * 100,
                'Margem Bruta': (info.get('grossMargins', 0) or 0) * 100,
                'Cresc. Receita (yoy)': (info.get('revenueGrowth', 0) or 0) * 100,
                'Cresc. Lucro (yoy)': (info.get('earningsGrowth', 0) or 0) * 100, # NOVO

                # --- SAÚDE FINANCEIRA ---
                'Caixa Total': info.get('totalCash', 0),
                'Dívida Total': info.get('totalDebt', 0),
                'EBITDA (12m)': info.get('ebitda', 0),                 
                'Dívida/EBITDA': info.get('debtToEquity', 0),
                'Liquidez Corrente': info.get('currentRatio', 0),
                'Liquidez Imediata': info.get('quickRatio', 0),

                # --- DIVIDENDOS ---
                'Div. Yield %': (info.get('dividendYield', 0) or 0) * 100,
                'Yield Anual (Trailing)': (info.get('trailingAnnualDividendYield', 0) or 0) * 100,
                'Payout Ratio %': (info.get('payoutRatio', 0) or 0) * 100,
                'Data Ex-Div': info.get('exDividendDate', 'N/A'),

                # --- HISTÓRICO ---
                'Máxima 52sem': info.get('fiftyTwoWeekHigh', 0),
                'Mínima 52sem': info.get('fiftyTwoWeekLow', 0),
                
                # --- CONSENSO ---
                'Preço Alvo Médio': info.get('targetMeanPrice', 0),
                'Recomendação': info.get('recommendationKey', 'N/A').upper()
            }
            dataframe_stocks.append(dados_ativo)

        except Exception as e:
            print(f"ERRO DE PARSE em {ticker}: {e}")
            dataframe_stocks.append({'Ativo': ticker.replace('.SA', ''), 'Preço Atual': 0, 'Recomendação': 'ERRO_SCRIPT'})

    df_final = pd.DataFrame(dataframe_stocks)
    df_final['Atualizado em'] = pd.Timestamp.now(tz='America/Sao_Paulo').strftime("%Y-%m-%d %H:%M")
    df_final = df_final.fillna(0)

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {
            os.getenv("GS_ABA"): df_final,
        },
        os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    )

if __name__ == "__main__":
        update_sheet_bulk()