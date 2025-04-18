import logging
import re
def clean_telefone(telefone_raw):
    """Limpa o número de telefone removendo caracteres especiais."""
    cleaned = re.sub(r'[ \(\)\-,\.\+]', '', telefone_raw)
    return cleaned

def adjust_phone_number_for_brazil(clean_number):
    ddi = '55'
    if not clean_number.startswith(ddi):
        clean_number = ddi + clean_number
    ddd = clean_number[2:4]
    number = clean_number[4:]
    if len(number) == 8:
        number = '9' + number
    adjusted_number = ddi + ddd + number
    return adjusted_number

def process_phone_number(phone_number):
    clean_number = clean_telefone(phone_number)
    if clean_number.startswith('55'):
        return adjust_phone_number_for_brazil(clean_number)
    return clean_number
