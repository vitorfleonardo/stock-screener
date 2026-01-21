import yfinance as yf
import pandas as pd
import time
import random
from load_to_sheets import carregar_dataframes_sheets
import os
from dotenv import load_dotenv
from datetime import datetime

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
    
def safe_get(info_dict, key, is_percent=False):
    """
    Tenta pegar a chave. 
    Se não existir ou for None -> Retorna None (vazio).
    Se existir -> Retorna o valor (multiplicado por 100 se for %).
    """
    valor = info_dict.get(key)
    
    # Se o valor for None, devolvemos None (para virar célula vazia no Excel)
    if valor is None:
        return None
    
    # Se for porcentagem, multiplica
    if is_percent:
        return valor * 100
    
    if isinstance(valor, (int, float)):
        return round(valor, 2)
        
    return valor

def format_timestamp(ts):
    """Converte Timestamp Unix para data legível YYYY-MM-DD"""
    if ts and isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    return None

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

            preco_close = None
            preco_open = None
            preco_high = None
            preco_low = None
            vol = None

            if ticker in dados_mercado.columns.levels[0]:
                if not dados_mercado[ticker].empty:
                    try:
                        val_close = round(float(dados_mercado[ticker]['Close'].iloc[0]), 2)
                        val_open = round(float(dados_mercado[ticker]['Open'].iloc[0]), 2)
                        val_high = round(float(dados_mercado[ticker]['High'].iloc[0]), 2)
                        val_low = round(float(dados_mercado[ticker]['Low'].iloc[0]), 2)
                        val_vol = int(dados_mercado[ticker]['Volume'].iloc[0])
                        
                        if not pd.isna(val_close): preco_close = val_close
                        if not pd.isna(val_open): preco_open = val_open
                        if not pd.isna(val_high): preco_high = val_high
                        if not pd.isna(val_low): preco_low = val_low
                        if not pd.isna(val_vol): vol = val_vol
                        
                    except:
                        pass

            dados_ativo = {
                'Ativo': ticker.replace('.SA', ''),
                'Nome': info.get('shortName', 'N/A'),           
                'Setor': info.get('sector', 'N/A'),             
                'Indústria': info.get('industry', 'N/A'),       

                # --- DADOS DE MERCADO ---
                'Preço Atual': preco_close,
                'Abertura': preco_open,
                'Máxima': preco_high,
                'Mínima': preco_low,
                'Volume': vol,
                'Média Volume (10d)': safe_get(info, 'averageVolume10days'),
                'Beta': safe_get(info, 'beta'),

                # --- TENDÊNCIA (MÉDIAS MÓVEIS) --- 
                'Média Móvel 50d': safe_get(info, 'fiftyDayAverage'),
                'Média Móvel 200d': safe_get(info, 'twoHundredDayAverage'),

                # --- VALUATION (Múltiplos) ---
                'P/L (Atual)': safe_get(info, 'trailingPE'),
                'P/L (Projetado)': safe_get(info, 'forwardPE'),
                'P/VP': safe_get(info, 'priceToBook'),
                'P/S': safe_get(info, 'priceToSalesTrailing12Months'),
                'EV/EBITDA': safe_get(info, 'enterpriseToEbitda'),
                'EV/Receita': safe_get(info, 'enterpriseToRevenue'),
                'Valor de Mercado': safe_get(info, 'marketCap'),
                'Enterprise Value': safe_get(info, 'enterpriseValue'),

                # --- DADOS POR AÇÃO (Fundamental) ---
                'LPA': safe_get(info, 'trailingEps'),      
                'VPA': safe_get(info, 'bookValue'),

                # --- EFICIÊNCIA & RENTABILIDADE ---
                'ROE': safe_get(info, 'returnOnEquity', is_percent=True),
                'ROA': safe_get(info, 'returnOnAssets', is_percent=True),
                'Margem Líquida': safe_get(info, 'profitMargins', is_percent=True),
                'Margem Operacional': safe_get(info, 'operatingMargins', is_percent=True),
                'Margem Bruta': safe_get(info, 'grossMargins', is_percent=True),
                'Cresc. Receita (yoy)': safe_get(info, 'revenueGrowth', is_percent=True),
                'Cresc. Lucro (yoy)': safe_get(info, 'earningsGrowth', is_percent=True),

                # --- SAÚDE FINANCEIRA ---
                'Caixa Total': safe_get(info, 'totalCash'),
                'Dívida Total': safe_get(info, 'totalDebt'),
                'EBITDA (12m)': safe_get(info, 'ebitda'),                 
                'Dívida/EBITDA': safe_get(info, 'debtToEquity'),
                'Liquidez Corrente': safe_get(info, 'currentRatio'),
                'Liquidez Imediata': safe_get(info, 'quickRatio'),

                # --- DIVIDENDOS ---
                'Div. Yield %': safe_get(info, 'dividendYield', is_percent=True),
                'Yield Anual (Trailing)': safe_get(info, 'trailingAnnualDividendYield', is_percent=True),
                'Payout Ratio %': safe_get(info, 'payoutRatio', is_percent=True),
                'Data Ex-Div': format_timestamp(safe_get(info, 'exDividendDate')),

                # --- HISTÓRICO ---
                'Máxima 52sem': safe_get(info, 'fiftyTwoWeekHigh'),
                'Mínima 52sem': safe_get(info, 'fiftyTwoWeekLow'),
                
                # --- CONSENSO ---
                'Preço Alvo Médio': safe_get(info, 'targetMeanPrice'),
                'Recomendação': info.get('recommendationKey', 'N/A').upper() if info.get('recommendationKey') else 'N/A'
            }
            dataframe_stocks.append(dados_ativo)

        except Exception as e:
            print(f"ERRO DE PARSE em {ticker}: {e}")
            dataframe_stocks.append({'Ativo': ticker.replace('.SA', ''), 'Preço Atual': 0, 'Recomendação': 'ERRO_SCRIPT'})

    df_final = pd.DataFrame(dataframe_stocks)
    df_final['Atualizado em'] = pd.Timestamp.now(tz='America/Sao_Paulo').strftime("%Y-%m-%d %H:%M")
    df_final = df_final.fillna("")

    carregar_dataframes_sheets(
        os.getenv("GS_STOCK_SCREENER"),
        {
            os.getenv("GS_ABA"): df_final,
        },
        os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    )

if __name__ == "__main__":
        update_sheet_bulk()