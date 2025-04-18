#!/usr/bin/env python3
"""
Script para testar a autenticação com a API do Google Ads.
Execute este script a partir da raiz do projeto para testar se a autenticação
com a conta de serviço está funcionando corretamente.
"""

import sys
import os
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Garantir que o diretório raiz do projeto esteja no sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Importar a função de teste
from ads_service.google_ads.google_ads import test_connection

if __name__ == "__main__":
    logging.info("Iniciando teste de autenticação com o Google Ads API...")
    
    success = test_connection()
    
    if success:
        logging.info("✅ TESTE PASSOU: Autenticação com o Google Ads API está funcionando corretamente!")
        sys.exit(0)
    else:
        logging.error("❌ TESTE FALHOU: Não foi possível autenticar com o Google Ads API.")
        sys.exit(1) 