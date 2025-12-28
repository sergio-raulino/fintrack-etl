import os
from dotenv import load_dotenv

class EnvLoader:
    @staticmethod
    def load_env_vars(caminho_env, variaveis_necessarias):
        """
        Carrega um conjunto de variáveis de ambiente a partir de um arquivo .env.

        :param caminho_env: Caminho para o arquivo .env
        :param variaveis_necessarias: Lista de nomes de variáveis que devem ser carregadas
        :return: Dicionário com as variáveis solicitadas
        """
        load_dotenv(caminho_env)

        env_vars = {
            var: os.getenv(var)
            for var in variaveis_necessarias
        }

        variaveis_faltando = [k for k, v in env_vars.items() if v is None]
        if variaveis_faltando:
            raise ValueError(f"Variáveis ausentes no .env: {variaveis_faltando}")

        return env_vars
