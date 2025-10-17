# -*- coding: utf-8 -*-
"""
Logging system for ETL
"""

import logging
import sys
import os
import glob
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime

from etl.config import settings


def clean_old_logs():
    """
    Deletes all previous log files from the logs directory
    """
    try:
        logs_dir = settings.LOGS_DIR
        if not logs_dir.exists():
            return
        
        # Delete all .log files
        log_files = list(logs_dir.glob('etl_*.log'))
        for log_file in log_files:
            try:
                os.remove(log_file)
            except Exception as e:
                print(f"Warning: Could not delete {log_file}: {e}")
        
        # Delete all .json metric files
        metric_files = list(logs_dir.glob('etl_metrics_*.json'))
        for metric_file in metric_files:
            try:
                os.remove(metric_file)
            except Exception as e:
                print(f"Warning: Could not delete {metric_file}: {e}")
        
        if log_files or metric_files:
            print(f"Cleaned {len(log_files)} log files and {len(metric_files)} metric files from previous executions")
            
    except Exception as e:
        print(f"Warning: Error cleaning old logs: {e}")


def setup_logger(name: str = 'etl', log_file: str = None, clean_previous: bool = True) -> logging.Logger:
    """
    Configures and returns a logger
    
    Args:
        name: Logger name
        log_file: Log file name (optional)
        clean_previous: If True, deletes all previous log files (default: True)
    
    Returns:
        Configured logger
    """
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL))
    
    # Avoid duplicating handlers and cleaning logs multiple times
    if logger.handlers:
        return logger
    
    # Clean previous logs if requested (only on first setup)
    if clean_previous:
        clean_old_logs()
    
    # Create formatter
    formatter = logging.Formatter(settings.LOG_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    log_path = settings.LOGS_DIR / log_file
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=settings.LOG_FILE_MAX_BYTES,
        backupCount=settings.LOG_FILE_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, settings.LOG_LEVEL))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


# Global logger
logger = setup_logger()

