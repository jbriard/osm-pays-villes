import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logger(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure le système de logging."""
    logger = logging.getLogger("osm_importer")
    logger.setLevel(getattr(logging, level.upper()))

    # Format des logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler fichier si spécifié
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
