def clean_numero_registre_national(num_reg_nat: str):
    """
    Ne renvoie que les caractères numériques
    :param num_reg_nat:
    :return:
    """
    return ''.join(ch for ch in num_reg_nat if ch.isdigit())