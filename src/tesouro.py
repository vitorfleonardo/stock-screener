import pandas as pd

def get_tesouro_direto():
    # URL oficial do Tesouro Transparente (CSV direto)
    # Fonte: https://www.tesourotransparente.gov.br/ckan/dataset/taxas-dos-titulos-ofertados-pelo-tesouro-direto
    url = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"
    
    try:
        df = pd.read_csv(url, sep=';', decimal=',')
        
        # Converte a coluna de data para datetime
        df['Data Base'] = pd.to_datetime(df['Data Base'], format='%d/%m/%Y')
        df['Data Vencimento'] = pd.to_datetime(df['Data Vencimento'], format='%d/%m/%Y')
        
        # Pega apenas os dados mais recentes (Último dia útil disponível)
        ultima_data = df['Data Base'].max()
        df_hoje = df[df['Data Base'] == ultima_data]
        
        print(f"Dados obtidos referentes ao dia: {ultima_data.strftime('%d/%m/%Y')}")
        
        return df_hoje

    except Exception as e:
        print(f"Erro ao baixar dados: {e}")
        return pd.DataFrame()
