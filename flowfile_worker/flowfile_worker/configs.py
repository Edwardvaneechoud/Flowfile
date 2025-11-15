"""
Configuration module for Flowfile Worker.

This module provides centralized configuration management.
All configuration can be overridden via environment variables with the FLOWFILE_ prefix.
"""
import logging
import platform
import argparse
import os
from typing import Optional
from connectorx import __version__


# Configure logging
logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('FlowfileWorker')
logger.setLevel(logging.INFO)


class WorkerConfig:
    """
    Centralized configuration for Flowfile Worker.
    
    All settings can be overridden via environment variables with FLOWFILE_ prefix.
    Example: FLOWFILE_SERVICE_PORT=8080
    """
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        # Service configuration
        self.service_host = os.getenv(
            'FLOWFILE_SERVICE_HOST',
            "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
        )
        self.service_port = int(os.getenv('FLOWFILE_SERVICE_PORT', '63579'))
        
        # Core service configuration  
        self.core_host = os.getenv(
            'FLOWFILE_CORE_HOST',
            "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
        )
        self.core_port = int(os.getenv('FLOWFILE_CORE_PORT', '63578'))
        
        # Worker limits and resources
        max_workers_env = os.getenv('FLOWFILE_MAX_CONCURRENT_PROCESSES')
        self.max_concurrent_processes = int(max_workers_env) if max_workers_env else None
        
        # Cache configuration
        self.cache_expiration_hours = int(os.getenv('FLOWFILE_CACHE_EXPIRATION_HOURS', '24'))
        
        # Error handling configuration
        self.error_message_max_length = int(os.getenv('FLOWFILE_ERROR_MESSAGE_MAX_LENGTH', '1024'))
        
        # Test mode
        self.test_mode = 'TEST_MODE' in os.environ
        
        # Process management
        self.process_acquisition_timeout = int(os.getenv('FLOWFILE_PROCESS_ACQUISITION_TIMEOUT', '30'))
    
    @property
    def core_url(self) -> str:
        """
        Get the complete core service URL.
        
        Returns:
            Complete URL including protocol, host, and port
        """
        return f"http://{self.core_host}:{self.core_port}"
    
    @property
    def cache_expiration_seconds(self) -> int:
        """
        Get cache expiration in seconds.
        
        Returns:
            Cache expiration time in seconds
        """
        return self.cache_expiration_hours * 60 * 60
    
    @property
    def calculated_max_workers(self) -> int:
        """
        Calculate platform-appropriate max concurrent processes.
        
        Platform limits:
        - Windows: Lower limit due to handle restrictions (max 32)
        - Unix: Higher limit but respect system resources (max 61)
        
        Returns:
            Maximum number of concurrent worker processes
        """
        if self.max_concurrent_processes is not None:
            return self.max_concurrent_processes
            
        cpu_count = os.cpu_count() or 4
        
        if platform.system() == 'Windows':
            # Windows has lower handle limits
            default_max = min(32, cpu_count + 4)
        else:
            # Unix systems (Linux, macOS) can handle more
            default_max = min(61, cpu_count * 2)
        
        return default_max
    
    def validate_configuration(self) -> None:
        """
        Validate configuration values.
        
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.service_host:
            raise ValueError("Worker host cannot be empty")
        
        if not self.core_host:
            raise ValueError("Core host cannot be empty")
        
        if not (1 <= self.service_port <= 65535):
            raise ValueError(f"Invalid port number: {self.service_port}. Port must be between 1 and 65535.")
        
        if not (1 <= self.core_port <= 65535):
            raise ValueError(f"Invalid core port number: {self.core_port}. Port must be between 1 and 65535.")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Command line arguments take precedence over environment variables.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(description="Flowfile Worker Server")
    parser.add_argument(
        "--host",
        type=str,
        help="Host to bind worker to (overrides FLOWFILE_SERVICE_HOST)"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Port to bind worker to (overrides FLOWFILE_SERVICE_PORT)"
    )
    parser.add_argument(
        "--core-host",
        type=str,
        help="Host of the core service (overrides FLOWFILE_CORE_HOST)"
    )
    parser.add_argument(
        "--core-port",
        type=int,
        help="Port of the core service (overrides FLOWFILE_CORE_PORT)"
    )
    
    # Use known_args to handle PyInstaller's extra args
    args = parser.parse_known_args()[0]
    
    return args


def create_config() -> WorkerConfig:
    """
    Create configuration from environment variables and command line arguments.
    
    Priority (highest to lowest):
    1. Command line arguments
    2. Environment variables
    3. Default values
    
    Returns:
        WorkerConfig instance with all settings applied
    """
    # Parse command line arguments
    args = parse_args()
    
    # Override environment variables with command line arguments if provided
    if args.host is not None:
        os.environ['FLOWFILE_SERVICE_HOST'] = args.host
    if args.port is not None:
        os.environ['FLOWFILE_SERVICE_PORT'] = str(args.port)
    if args.core_host is not None:
        os.environ['FLOWFILE_CORE_HOST'] = args.core_host
    if args.core_port is not None:
        os.environ['FLOWFILE_CORE_PORT'] = str(args.core_port)
    
    # Create config from environment
    config = WorkerConfig()
    
    # Validate configuration
    config.validate_configuration()
    
    return config


# Create global config instance
config = create_config()

# Legacy variable names for backwards compatibility
SERVICE_HOST = config.service_host
SERVICE_PORT = config.service_port
CORE_HOST = config.core_host
CORE_PORT = config.core_port
FLOWFILE_CORE_URI = config.core_url
TEST_MODE = config.test_mode

# Log configuration
logger.info(f"ConnectorX version: {__version__}")
logger.info(f"Worker configured at {SERVICE_HOST}:{SERVICE_PORT}")
logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")
logger.info(f"Maximum concurrent processes: {config.calculated_max_workers}")
logger.info(f"Cache expiration: {config.cache_expiration_hours} hours")
logger.info(f"Test mode: {TEST_MODE}")