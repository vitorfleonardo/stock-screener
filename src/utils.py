from datetime import datetime

def safe_get(info_dict, key, is_percent=False):
    """
    Tenta pegar a chave. 
    Se não existir ou for None -> Retorna None (vazio).
    Se existir -> Retorna o valor arrendondado com 2 casas decimais ou 4 se for %.
    """
    valor = info_dict.get(key)
    
    if valor is None:
        return None
    
    if is_percent:
        return round(valor, 4)
    
    if isinstance(valor, (int, float)):
        return round(valor, 2)
        
    return valor

def format_timestamp(ts):
    """Converte Timestamp Unix para data legível YYYY-MM-DD"""
    if ts and isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    return None