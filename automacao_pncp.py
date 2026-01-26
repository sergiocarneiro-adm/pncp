import requests
import json
import time
import os
import locale
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Tenta configurar para inglês para garantir que a data saia como "Mon Dec"
try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except:
    pass 

class PNCPImporter:
    BASE_URL_CONSULTA = "https://pncp.gov.br/api/consulta"
    BASE_URL_INTEGRACAO = "https://pncp.gov.br/api/pncp"
    
    def __init__(self, cnpj="13650403000128"):
        self.cnpj = cnpj
        self.session = self.setup_session()
        self.cooldown_time = 1.0

    def setup_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Bot-Sincronizador-PNCP",
            "Accept": "application/json"
        })
        return session

    def _safe_request(self, url, params=None):
        try:
            time.sleep(self.cooldown_time)
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro na requisição: {e}")
            return None

    def listar_contratacoes(self, data_inicial, data_final, pagina=1, modalidade=None):
        url = f"{self.BASE_URL_CONSULTA}/v1/contratacoes/publicacao"
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "pagina": pagina,
            "tamanhoPagina": 10,
            "cnpj": self.cnpj
        }
        if modalidade:
            params["codigoModalidadeContratacao"] = modalidade
        return self._safe_request(url, params=params)

    def obter_itens_contratacao(self, cnpj, ano, sequencial):
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
        itens_completos = []
        pagina = 1
        while True:
            params = {"pagina": pagina, "tamanhoPagina": 100}
            dados = self._safe_request(url, params=params)
            if not dados: break
            
            items_list = []
            if isinstance(dados, list): items_list = dados
            elif isinstance(dados, dict): items_list = dados.get('data') or dados.get('resultado') or []
            
            if not items_list:
                if isinstance(dados, dict) and 'numeroItem' in dados: items_list = [dados]
                else: break
                
            itens_completos.extend(items_list)
            if len(items_list) < 100: break
            pagina += 1
        return itens_completos

    def obter_resultados_item(self, cnpj, ano, sequencial, numero_item):
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numero_item}/resultados"
        resultados = self._safe_request(url)
        if isinstance(resultados, list): return resultados
        if isinstance(resultados, dict): return [resultados]
        return []

    def processar_contratacao_completa(self, contratacao):
        try:
            cnpj_orgao = contratacao['orgaoEntidade']['cnpj']
            ano = contratacao['anoCompra']
            sequencial = contratacao['sequencialCompra']
            
            # Formata a data para o padrão do seu JSON antigo
            # "Mon Dec 29 2025 17:14:50 GMT-0300 (Brasilia Standard Time)"
            dt_raw = contratacao.get('dataPublicacaoPncp', '')
            if dt_raw:
                dt_obj = datetime.fromisoformat(dt_raw.replace('Z', '+00:00'))
                # Simula o formato do Google Sheets
                contratacao['dataPublicacao'] = dt_obj.strftime("%a %b %d %Y %H:%M:%S GMT-0300 (Brasilia Standard Time)")
            
            print(f"  -> Detalhando {ano}/{sequencial}...")
            itens = self.obter_itens_contratacao(cnpj_orgao, ano, sequencial)
            for item in itens:
                num = item.get('numeroItem')
                if num:
                    item['resultados_vencedores'] = self.obter_resultados_item(cnpj_orgao, ano, sequencial, num)
            
            contratacao['itens_detalhados'] = itens
            return contratacao
        except Exception as e:
            print(f"Erro no detalhamento: {e}")
            return contratacao

    def importar_tudo(self, d_ini, d_fim):
        todos_resultados = []
        modalidades = [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13]
        
        atual = datetime.strptime(d_ini, "%Y%m%d")
        fim_obj = datetime.strptime(d_fim, "%Y%m%d")
        
        while atual <= fim_obj:
            bloco_fim = min(atual + timedelta(days=14), fim_obj)
            s_ini, s_fim = atual.strftime("%Y%m%d"), bloco_fim.strftime("%Y%m%d")
            print(f"\n--- Periodo: {s_ini} a {s_fim} ---")
            
            for mod in modalidades:
                pagina = 1
                while True:
                    dados = self.listar_contratacoes(s_ini, s_fim, pagina, mod)
                    if not dados or 'data' not in dados or not dados['data']: break
                    
                    for contratacao in dados['data']:
                        completa = self.processar_contratacao_completa(contratacao)
                        todos_resultados.append(completa)
                    
                    if pagina >= dados.get('totalPaginas', 1): break
                    pagina += 1
            atual = bloco_fim + timedelta(days=1)
        return todos_resultados

# --- LOGICA PRINCIPAL ---
FILE_NAME = 'dados.json'

def main():
    dados_existentes = []
    data_inicio = "20260101"

    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, 'r', encoding='utf-8') as f:
            try:
                dados_existentes = json.load(f)
                if dados_existentes:
                    datas_str = [c.get('dataPublicacao', '') for c in dados_existentes if c.get('dataPublicacao')]
                    
                    datas_convertidas = []
                    for d in datas_str:
                        try:
                            # Extrai "Dec 29 2025" da string longa
                            partes = d.split(' ')
                            data_limpa = f"{partes[1]} {partes[2]} {partes[3]}"
                            dt = datetime.strptime(data_limpa, "%b %d %Y")
                            datas_convertidas.append(dt)
                        except: continue
                    
                    if datas_convertidas:
                        ultima_dt = max(datas_convertidas)
                        data_inicio = (ultima_dt + timedelta(days=1)).strftime("%Y%m%d")
            except Exception as e:
                print(f"Erro ao ler JSON: {e}")

    data_hoje = datetime.now().strftime("%Y%m%d")
    
    if data_inicio > data_hoje:
        print(f"Dados ja estao atualizados ate {data_inicio}. Nada a fazer.")
        return

    print(f"Iniciando sincronizacao de {data_inicio} ate {data_hoje}...")
    importer = PNCPImporter()
    novos_dados = importer.importar_tudo(data_inicio, data_hoje)

    if novos_dados:
        dados_finais = dados_existentes + novos_dados
        with open(FILE_NAME, 'w', encoding='utf-8') as f:
            json.dump(dados_finais, f, indent=4, ensure_ascii=False)
        print(f"\nSucesso! {len(novos_dados)} novos registros adicionados.")
    else:
        print("\nNenhuma nova contratacao encontrada.")

if __name__ == "__main__":
    main()

