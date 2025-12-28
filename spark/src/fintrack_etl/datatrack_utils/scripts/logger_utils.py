import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


class LoggerUtils:
    """
    Classe utilitária para configurar e obter um logger padronizado.

    Ao instanciar:
      - Cria logs no console e arquivo
      - Nome do arquivo inclui timestamp (YYYY-MM-DD_HH-MM-SS)
      - Rotaciona logs diariamente, mantendo histórico configurável
    """

    def __init__(
        self,
        nome_logger: str = "tjce_logger",
        pasta_logs: str = "logs",
        nivel: int = logging.INFO,
        backup_days: int = 7,
        rotacionar: bool = True,
    ):
        """
        Inicializa e configura o logger.
        :param nome_logger: Nome do logger (aparece nos logs)
        :param pasta_logs: Diretório onde os logs serão salvos
        :param nivel: Nível mínimo de log (ex: logging.INFO)
        :param backup_days: Quantos dias de backup manter nos logs rotacionados
        :param rotacionar: Se True, cria logs diários com rotação automática
        """
        self.logger = logging.getLogger() 
        self.logger.setLevel(nivel)
        
        # Se já configuramos antes, não faz nada (evita handlers duplicados)
        if getattr(self.logger, "_tjce_configured", False):
            return
        
         # ---------- limpa handlers antigos  ----------
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)


        os.makedirs(pasta_logs, exist_ok=True)

        # Nome do arquivo com timestamp completo
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_filename = os.path.join(
            pasta_logs, f"{nome_logger}_{timestamp}.log"
        )

        formato = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


        # === Handler de Console ===
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formato)
        self.logger.addHandler(console_handler)

        # === Handler de Arquivo ===
        if rotacionar:
            # Cria rotação diária mantendo X dias de histórico
            file_handler = TimedRotatingFileHandler(
                log_filename,
                when="midnight",       # rotação diária
                backupCount=backup_days,
                encoding="utf-8",
            )
        else:
            # Sem rotação — apenas um arquivo por execução
            file_handler = logging.FileHandler(log_filename, encoding="utf-8")

        file_handler.setFormatter(formato)
        self.logger.addHandler(file_handler)

        self.logger._tjce_configured = True


    # === Métodos utilitários ===
    def info(self, msg: str):
        self.logger.info(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)

    def error(self, msg: str, e: Exception | None = None):
        if e:
            self.logger.error(f"{msg}\n", exc_info=e)
        else:
            self.logger.error(msg, exc_info=True)

    def get_logger(self):
        """Retorna o objeto Logger interno, caso precise de mais controle."""
        return self.logger
