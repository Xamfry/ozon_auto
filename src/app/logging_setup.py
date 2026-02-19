import logging
from pathlib import Path

def setup_logging(log_path: str = "data/autorus.log") -> logging.Logger:
    Path("data").mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("autorus")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # файл
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    # консоль
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
