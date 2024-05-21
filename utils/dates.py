from datetime import datetime

def is_valid_date(date_str: str) -> bool:
    """Funcao para verificar se uma string e uma data valida

    Args:
        date_str (str): Data no formato string. Ex "20231231"

    Returns:
        _type_: True ou False
    """
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False