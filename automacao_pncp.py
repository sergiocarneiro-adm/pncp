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
        self.cooldown_time = 0.5 # Reduzi um pouco para ser mais rápido no refresh

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

    def obter_contratacao_especifica(self, cnpj, ano, sequencial):
        """Busca os dados atuais de uma contratação específica"""
        url = f"{self.BASE_URL_INTEGRACAO}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
        return self._safe_request(url)

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

    def formatar_para_html(self, contratacao, item):
        """Achata a estrutura para manter compatibilidade com o HTML antigo"""
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

            # Extrai CNPJ do órgão de forma segura
            cnpj_entidade = ""
            if 'orgaoEntidade' in contratacao:
                cnpj_entidade = contratacao['orgaoEntidade'].get('cnpj', "")

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
                "linkPNCP": f"https://pncp.gov.br/app/editais/{cnpj_entidade}/{contratacao.get('anoCompra')}/{contratacao.get('sequencialCompra')}",
                "processo": contratacao.get('processo', ""),
                "dataPublicacao": dt_pub_fmt,
                "dataResultado": data_resultado_fmt,
                "_sequencial": contratacao.get('sequencialCompra') # Campo interno para controle
            }
        except Exception as e:
            print(f"Erro na formatação: {e}")
            return None

    def processar_contratacao_completa(self, contratacao):
        try:
            cnpj_orgao = contratacao['orgaoEntidade']['cnpj']
            ano = contratacao['anoCompra']
            sequencial = contratacao['sequencialCompra']
            
            print(f"  -> Detalhando {ano}/{sequencial}...")
            itens = self.obter_itens_contratacao(cnpj_orgao, ano, sequencial)
            
            itens_achatados = []
            for item in itens:
                num = item.get('numeroItem')
                if num:
                    item['resultados_vencedores'] = self.obter_resultados_item(cnpj_orgao, ano, sequencial, num)
                
                item_formatado = self.formatar_para_html(contratacao, item)
                if item_formatado:
                    itens_achatados.append(item_formatado)
            
            return itens_achatados
        except Exception as e:
            print(f"Erro no detalhamento: {e}")
            return []

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
                        novos_itens = self.processar_contratacao_completa(contratacao)
                        todos_resultados.extend(novos_itens)
                    
                    if pagina >= dados.get('totalPaginas', 1): break
                    pagina += 1
            atual = bloco_fim + timedelta(days=1)
        return todos_resultados

# --- LOGICA PRINCIPAL ---
FILE_NAME = 'dados.json'

def main():
    dados_carregados = []
    is_dict_format = False
    full_json_data = {}
    data_inicio = "20240101"

    # 1. Carregar dados existentes
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, 'r', encoding='utf-8') as f:
            try:
                content = json.load(f)
                if isinstance(content, list):
                    dados_carregados = content
                    is_dict_format = False
                elif isinstance(content, dict):
                    full_json_data = content
                    dados_carregados = content.get('data', [])
                    is_dict_format = True
            except Exception as e:
                print(f"Erro ao ler JSON: {e}")

    importer = PNCPImporter()

    # 2. Lógica de REFRESH (Atualizar o que já existe e está em aberto)
    print("\n--- Iniciando atualização de status (Refresh) ---")
    # Filtramos itens que não estão finalizados (Homologado ou Fracassado)
    # E focamos em itens de 2025/2026 para não sobrecarregar
    itens_para_atualizar = [
        i for i in dados_carregados 
        if i.get('situacaoItem') not in ['Homologado', 'Fracassado', 'Anulado', 'Revogado']
        and i.get('ano') in [2025, 2026]
    ]
    
    if itens_para_atualizar:
        # Agrupamos por sequencial para não fazer requisições repetidas da mesma compra
        sequenciais = list(set([i.get('_sequencial') for i in itens_para_atualizar if i.get('_sequencial')]))
        print(f"Verificando {len(sequenciais)} contratações em aberto...")
        
        for seq in sequenciais:
            try:
                # Busca dados atualizados da compra
                compra_atualizada = importer.obter_contratacao_especifica(importer.cnpj, 2025 if seq > 500 else 2026, seq) # Simplificação do ano
                if not compra_atualizada: 
                    # Tenta o outro ano se falhar (devido à virada de ano)
                    compra_atualizada = importer.obter_contratacao_especifica(importer.cnpj, 2026 if seq < 500 else 2025, seq)
                
                if compra_atualizada:
                    novos_detalhes = importer.processar_contratacao_completa(compra_atualizada)
                    if novos_detalhes:
                        # Remove os itens antigos dessa compra e adiciona os novos
                        dados_carregados = [i for i in dados_carregados if i.get('_sequencial') != seq]
                        dados_carregados.extend(novos_detalhes)
                        print(f"  [OK] {seq} atualizado.")
            except Exception as e:
                print(f"  [ERRO] Falha ao atualizar {seq}: {e}")
    else:
        print("Nenhum item pendente de atualização encontrado.")

    # 3. Lógica de INCREMENTAL (Buscar novos dados)
    if dados_carregados:
        datas_str = [c.get('dataPublicacao', '') for c in dados_carregados if c.get('dataPublicacao')]
        datas_convertidas = []
        for d in datas_str:
            try:
                partes = d.split(' ')
                data_limpa = f"{partes[1]} {partes[2]} {partes[3]}"
                dt = datetime.strptime(data_limpa, "%b %d %Y")
                datas_convertidas.append(dt)
            except: continue
        if datas_convertidas:
            ultima_dt = max(datas_convertidas)
            data_inicio = (ultima_dt + timedelta(days=1)).strftime("%Y%m%d")

    data_hoje = datetime.now().strftime("%Y%m%d")
    
    if data_inicio <= data_hoje:
        print(f"\n--- Buscando novos dados de {data_inicio} até {data_hoje} ---")
        novos_dados = importer.importar_tudo(data_inicio, data_hoje)
        if novos_dados:
            dados_carregados.extend(novos_dados)
            print(f"Sucesso! {len(novos_dados)} novos registros adicionados.")
    else:
        print(f"\nDados já estão atualizados até {data_inicio}. Nenhuma nova busca necessária.")

    # 4. Salvar tudo
    if is_dict_format:
        full_json_data['data'] = dados_carregados
        full_json_data['totalRegistros'] = len(dados_carregados)
        full_json_data['geradoEm'] = datetime.now().isoformat()
        dados_finais = full_json_data
    else:
        dados_finais = dados_carregados
        
    with open(FILE_NAME, 'w', encoding='utf-8') as f:
        json.dump(dados_finais, f, indent=4, ensure_ascii=False)
    print("\nProcesso concluído com sucesso!")

if __name__ == "__main__":
    main()
