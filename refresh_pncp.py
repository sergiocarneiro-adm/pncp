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

class PNCPRefresher:
    BASE_URL_INTEGRACAO = "https://pncp.gov.br/api/pncp"
    
    def __init__(self, cnpj="13650403000128"):
        self.cnpj = cnpj
        self.session = self.setup_session()
        self.cooldown_time = 0.5 # Menor cooldown para refresh

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Bot-Refresher-PNCP",
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

    def obter_dados_contratacao(self, cnpj, ano, sequencial):
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
        return self._safe_request(url)

    def obter_item_especifico(self, cnpj, ano, sequencial, numero_item):
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numero_item}"
        return self._safe_request(url)

    def obter_resultados_item(self, cnpj, ano, sequencial, numero_item):
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numero_item}/resultados"
        resultados = self._safe_request(url)
        if isinstance(resultados, list): return resultados
        if isinstance(resultados, dict): return [resultados]
        return []

    def formatar_para_html(self, contratacao, item):
        try:
            dt_pub_raw = contratacao.get('dataPublicacaoPncp', '')
            dt_pub_fmt = ""
            if dt_pub_raw:
                dt_obj = datetime.fromisoformat(dt_pub_raw.replace('Z', '+00:00'))
                dt_pub_fmt = dt_obj.strftime("%a %b %d %Y %H:%M:%S GMT-0300 (Brasilia Standard Time)")

            vencedor_nome = "SEM RESULTADO"
            vencedor_cnpj = ""
            valor_unit_homologado = 0
            valor_total_homologado = 0
            qtd_homologada = 0
            data_resultado_fmt = ""

            resultados = item.get('resultados_vencedores', [])
            if resultados:
                res = resultados[0]
                vencedor_nome = res.get('nomeRazaoSocialFornecedor', "SEM RESULTADO")
                vencedor_cnpj = res.get('niFornecedor', "")
                valor_unit_homologado = res.get('valorUnitarioHomologado', 0)
                valor_total_homologado = res.get('valorTotalHomologado', 0)
                qtd_homologada = res.get('quantidadeHomologada', 0)
                dt_res_raw = res.get('dataResultado', '')
                if dt_res_raw:
                    dt_res_obj = datetime.fromisoformat(dt_res_raw.replace('Z', '+00:00'))
                    data_resultado_fmt = dt_res_obj.strftime("%a %b %d %Y %H:%M:%S GMT-0300 (Brasilia Standard Time)")

            return {
                "orgao": contratacao.get('orgaoEntidade', {}).get('razaoSocial', ""),
                "ano": contratacao.get('anoCompra'),
                "compra": contratacao.get('numeroCompra', ""),
                "modalidade": contratacao.get('modalidadeNome', ""),
                "objeto": contratacao.get('objetoCompra', ""),
                "itemNo": item.get('numeroItem'),
                "descricao": item.get('descricao', ""),
                "quantidade": item.get('quantidade', 0),
                "unidade": item.get('unidadeMedida', ""),
                "valorUnitEstimado": item.get('valorUnitarioEstimado', 0),
                "valorTotalEstimado": item.get('valorTotal', 0),
                "vencedor": vencedor_nome,
                "cnpjVencedor": vencedor_cnpj,
                "valorUnitHomologado": valor_unit_homologado,
                "valorTotalHomologado": valor_total_homologado,
                "qtdHomologada": qtd_homologada,
                "situacaoItem": item.get('situacaoCompraItemNome', ""),
                "linkPNCP": f"https://pncp.gov.br/app/editais/{contratacao.get('orgaoEntidade', {}).get('cnpj')}/{contratacao.get('anoCompra')}/{contratacao.get('sequencialCompra')}",
                "processo": contratacao.get('processo', ""),
                "dataPublicacao": dt_pub_fmt,
                "dataResultado": data_resultado_fmt
            }
        except Exception as e:
            print(f"Erro na formatação: {e}")
            return None

# --- LOGICA DE ATUALIZACAO ---
FILE_NAME = 'dados.json'

def main():
    if not os.path.exists(FILE_NAME):
        print("Arquivo dados.json não encontrado.")
        return

    with open(FILE_NAME, 'r', encoding='utf-8') as f:
        content = json.load(f)
    
    is_dict = isinstance(content, dict)
    dados = content.get('data', []) if is_dict else content
    
    if not dados:
        print("Nenhum dado para atualizar.")
        return

    refresher = PNCPRefresher()
    
    # Filtramos itens que NÃO estão finalizados (Homologado ou Fracassado)
    # E focamos nos anos recentes para não sobrecarregar
    status_finais = ["Homologado", "Fracassado", "Anulado", "Revogado"]
    itens_para_atualizar = [
        (idx, item) for idx, item in enumerate(dados) 
        if item.get('situacaoItem') not in status_finais and item.get('ano') in [2025, 2026]
    ]

    if not itens_para_atualizar:
        print("Todos os itens recentes já estão com status final.")
        return

    print(f"Encontrados {len(itens_para_atualizar)} itens para verificar atualização...")
    
    alteracoes = 0
    # Cache para não repetir requisição da mesma compra (vários itens)
    cache_compras = {}

    for idx, item_antigo in itens_para_atualizar:
        try:
            # Extraímos o sequencial do link
            link = item_antigo.get('linkPNCP', '')
            partes = link.split('/')
            cnpj_orgao = partes[-3]
            ano = partes[-2]
            sequencial = partes[-1]
            num_item = item_antigo.get('itemNo')

            # Busca dados da compra (cache)
            if sequencial not in cache_compras:
                cache_compras[sequencial] = refresher.obter_dados_contratacao(cnpj_orgao, ano, sequencial)
            
            contratacao_nova = cache_compras[sequencial]
            if not contratacao_nova: continue

            # Busca dados do item
            item_novo_bruto = refresher.obter_item_especifico(cnpj_orgao, ano, sequencial, num_item)
            if not item_novo_bruto: continue

            # Busca resultados do item
            item_novo_bruto['resultados_vencedores'] = refresher.obter_resultados_item(cnpj_orgao, ano, sequencial, num_item)
            
            # Formata
            item_atualizado = refresher.formatar_para_html(contratacao_nova, item_novo_bruto)
            
            if item_atualizado:
                # Verifica se mudou algo importante (status ou vencedor)
                if (item_atualizado['situacaoItem'] != item_antigo['situacaoItem'] or 
                    item_atualizado['vencedor'] != item_antigo['vencedor']):
                    
                    print(f"  [!] Atualizado: {item_antigo['compra']} Item {num_item} -> {item_atualizado['situacaoItem']}")
                    dados[idx] = item_atualizado
                    alteracoes += 1
        except Exception as e:
            print(f"Erro ao processar item {item_antigo.get('compra')}: {e}")

    if alteracoes > 0:
        if is_dict:
            content['data'] = dados
            content['geradoEm'] = datetime.now().isoformat()
        else:
            content = dados
            
        with open(FILE_NAME, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=4, ensure_ascii=False)
        print(f"\nSucesso! {alteracoes} registros foram atualizados com novas informações.")
    else:
        print("\nNenhuma alteração de status encontrada nos itens verificados.")

if __name__ == "__main__":
    main()
