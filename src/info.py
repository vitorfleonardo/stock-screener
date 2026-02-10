from src.utils import safe_get, format_timestamp
import pandas as pd

def organize_data(ticker, info, dados_mercado):

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

            'Preço Atual':  preco_close, # FLOAT arrendodado para 2 casas decimais ou None
            'Abertura (1d)': preco_open, # FLOAT arrendodado para 2 casas decimais ou None
            'Máxima (1d)': preco_high, # FLOAT arrendodado para 2 casas decimais ou None
            'Mínima (1d)': preco_low, # FLOAT arrendodado para 2 casas decimais ou None
            'Volume Negociado (1d)': vol, # FLOAT arrendodado para 2 casas decimais ou None

            'Média Volume Negociado (10d)': safe_get(info, 'averageVolume10days'), # INT
            'Beta': safe_get(info, 'beta'), # FLOAT
            'Média Móvel 50d': safe_get(info, 'fiftyDayAverage'), # FLOAT
            'Média Móvel 200d': safe_get(info, 'twoHundredDayAverage'), # FLOAT
            'P/L (12m)': safe_get(info, 'trailingPE'), # float ou NoneType
            'P/L (Projetado)': safe_get(info, 'forwardPE'), # float ou NoneType
            'P/VP': safe_get(info, 'priceToBook'),# FLOAT
            'P/S': safe_get(info, 'priceToSalesTrailing12Months'), # FLOAT
            'EV/EBITDA': safe_get(info, 'enterpriseToEbitda'),  # FLOAT
            'EV/Receita': safe_get(info, 'enterpriseToRevenue'),  # FLOAT
            'Valor de Mercado': safe_get(info, 'marketCap'), # INT
            'Enterprise Value': safe_get(info, 'enterpriseValue'), # INT
            'LPA': safe_get(info, 'trailingEps'), # FLOAT      
            'VPA': safe_get(info, 'bookValue'), # FLOAT
            'ROE': safe_get(info, 'returnOnEquity', is_percent=True), # FLOAT
            'ROA': safe_get(info, 'returnOnAssets', is_percent=True), # FLOAT
            'Margem Bruta': safe_get(info, 'grossMargins', is_percent=True), # FLOAT
            'Margem Operacional': safe_get(info, 'operatingMargins', is_percent=True), # FLOAT
            'Margem Líquida': safe_get(info, 'profitMargins', is_percent=True), # FLOAT
            'Revenue Growth': safe_get(info, 'revenueGrowth', is_percent=True), # FLOAT
            'Earnings Growth': safe_get(info, 'earningsGrowth', is_percent=True), # FLOAT
            'Caixa Total': safe_get(info, 'totalCash'), # INT
            'Dívida Total': safe_get(info, 'totalDebt'), # INT
            'EBITDA (12m)': safe_get(info, 'ebitda'), # INT                
            'Dívida/EBITDA': safe_get(info, 'debtToEquity'),
            'Liquidez Corrente': safe_get(info, 'currentRatio'),
            'Liquidez Imediata': safe_get(info, 'quickRatio'),
            'Div. Yield (12m)': safe_get(info, 'trailingAnnualDividendYield', is_percent=True),
            'Div. Yield (Projetado)': safe_get(info, 'dividendYield', is_percent=True),
            'Payout Ratio %': safe_get(info, 'payoutRatio', is_percent=True),
            'Data Ex-Div': format_timestamp(safe_get(info, 'exDividendDate')),
            'Máxima 52sem': safe_get(info, 'fiftyTwoWeekHigh'),
            'Mínima 52sem': safe_get(info, 'fiftyTwoWeekLow'),
            'Preço Alvo Médio': safe_get(info, 'targetMeanPrice'),
            'Recomendação': info.get('recommendationKey', 'N/A').upper() if info.get('recommendationKey') else 'N/A',
            'Div. futuro (R$)': safe_get(info, 'dividendRate'),
            'Div. histórico (R$)': safe_get(info, 'trailingAnnualDividendRate'),
        }
        
    except Exception as e:
        print(f"Erro: {e}")
        return None
    
    return dados_ativo
