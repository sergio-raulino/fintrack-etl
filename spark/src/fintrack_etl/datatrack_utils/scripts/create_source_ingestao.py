# converter_para_json.py
import pandas as pd
import json

# --- Arquivos  ---
caminho_saida = '/home/TJCE-DOM-01/04653624356/projetos/airflow-bi/spark/scripts/data-lake-administrativo/sources'
caminho_arquivos_tabela =[
                          F'{caminho_saida}/modelo_planilha.xlsx'   
                        ]   
  
# --- Fim da Configuração ---

print(f"Lendo o arquivo de entrada: {caminho_arquivos_tabela}...")

for arquivo in caminho_arquivos_tabela:
    try:
        # Lê o arquivo EXCEL para um DataFrame pandas, preenchendo células vazias com ''
        dftabelas = pd.read_excel(arquivo, engine='openpyxl').fillna('')

        # Verifica se o DataFrame está vazio após a leitura
        if dftabelas.empty:
            raise ValueError("O arquivo Excel está vazio ou não contém dados legíveis.")

        # Pega as informações globais da primeira linha do arquivo
        sistema = dftabelas.iloc[0]['sistema'].lower()
        banco = dftabelas.iloc[0]['banco']
        esquema = dftabelas['esquema'].iloc[0].lower()
        nome_source = sistema
        
        #Oracle banco e schema
        if banco == 'oracle':
            nome_source = f"{sistema}_{esquema}"
            
        # Nome do arquivo de saída que será gerado
        caminho_json_saida = f'{caminho_saida}/{nome_source}.json'

        # Cria a estrutura base do nosso dicionário final
        estrutura_source = {
            'informacoes': {
                'banco': banco,
                'sistema': sistema,
                'schemas': {}
            }
        }
        print(estrutura_source)
        print("Processando linhas e construindo a estrutura JSON...")
        
        # Itera sobre cada linha do DataFrame para popular os detalhes de cada tabela
        for _, row in dftabelas.iterrows():
            nome_tabela = row['tabela']
            schema = row['esquema']
            if nome_tabela and schema:  
                if schema not in estrutura_source['informacoes']['schemas']:
                    estrutura_source['informacoes']['schemas'][schema] = {}
                estrutura_source['informacoes']['schemas'][schema][nome_tabela] = {
                    'tipo_carga': row['tipo_carga'],
                    'querycustom': row['query'],
                    'coluna_particao': row['coluna_particao'],
                    'tipo_coluna_particao': row["tipo_coluna_particao"],
                    'chaves': [chave.strip() for chave in row['chaves'].split(',') if chave.strip()],
                    'tipos_col_excluidos': [chave.strip() for chave in row['tipos_col_excluidos'].split(',') if chave.strip()],
                    'delta_colum': {                   
                                "name": row["nome_coluna_delta"],
                                "type": row["type_delta"],
                                "lowerBound": row["lowerbound"],
                                "upperBound": row["upperbound"],
                                "numPartitions": row["numpartitions"],
                                "create_row_number": row["create_row_number"],
                                "condition": str(row["condition"])
                            }  
                }

        # Salva o dicionário Python em um arquivo JSON
        # indent=4 cria uma formatação bonita e legível
        # ensure_ascii=False garante que caracteres como 'ç' e 'ã' sejam salvos corretamente
        with open(caminho_json_saida, 'w', encoding='utf-8') as f:
            json.dump(estrutura_source, f, indent=4, ensure_ascii=False)

        print(f"\nSUCESSO! O arquivo '{caminho_json_saida}' foi criado.")

    except FileNotFoundError:
        print(f"ERRO: O arquivo de entrada '{caminho_arquivos_tabela}' não foi encontrado. Verifique o nome e o local do arquivo.")
    except KeyError as e:
        print(f"ERRO: Uma coluna esperada não foi encontrada no arquivo Excel: {e}. Verifique os nomes das colunas (ex: 'Sistema', 'Tabela', etc.).")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")