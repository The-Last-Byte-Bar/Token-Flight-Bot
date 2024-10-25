# config.py
import json
from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum

class DistributionType(str, Enum):
    LINEAR = "linear"
    LOGARITHMIC = "logarithmic"
    QUADRATIC = "quadratic"
    CONSTANT = "constant"

@dataclass
class TokenConfig:
    name: str
    description: str
    total_amount: int
    decimals: int
    distribution_type: DistributionType
    tokens_per_round: int
    token_id: str = ""

@dataclass
class Config:
    node_url: str
    explorer_url: str
    api_key: str
    network_type: str
    node_address: str
    minter_address: str
    recipient_wallets: List[str]
    blocks_between_dispense: int
    tokens: Optional[Dict[str, TokenConfig]] = None  # Make tokens optional

def load_config(file_path: str) -> Config:
    """Load configuration from either config.json or bot_info.json format"""
    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    
    # Check if this is a bot info file
    if 'proxy_contract_address' in config_data:
        # This is a bot info file
        return Config(
            node_url=config_data.get('node_url', ''),  # These might be empty for bot info
            explorer_url=config_data.get('explorer_url', ''),
            api_key=config_data.get('api_key', ''),
            network_type=config_data.get('network_type', ''),
            node_address=config_data['node_address'],
            minter_address=config_data.get('minter_address', config_data['node_address']),
            recipient_wallets=config_data['recipient_wallets'],
            blocks_between_dispense=config_data['blocks_between_dispense'],
            tokens={
                name: TokenConfig(
                    name=name,
                    description="",  # Bot info doesn't store descriptions
                    total_amount=token_info['total_amount'],
                    decimals=token_info['decimals'],
                    distribution_type=DistributionType(token_info['distribution_type']),
                    tokens_per_round=token_info['tokens_per_round'],
                    token_id=token_info['token_id']
                )
                for name, token_info in config_data['tokens'].items()
            } if 'tokens' in config_data else None
        )
    else:
        # This is a regular config file
        return Config(
            node_url=config_data['node']['nodeApi']['apiUrl'],
            explorer_url=config_data['node']['explorer_url'],
            api_key=config_data['node']['nodeApi']['apiKey'],
            network_type=config_data['node']['networkType'],
            node_address=config_data['node']['nodeAddress'],
            minter_address=config_data['parameters']['minterAddr'],
            recipient_wallets=config_data['parameters']['recipientWallets'],
            blocks_between_dispense=config_data['distribution']['blocksBetweenDispense']
        )

def validate_config(config: Config) -> None:
    """Validate configuration based on what type it is"""
    # Basic validation for required fields
    if not config.recipient_wallets or len(config.recipient_wallets) == 0:
        raise ValueError("recipient_wallets must be a non-empty list")
    
    if config.blocks_between_dispense <= 0:
        raise ValueError("blocks_between_dispense must be positive")
    
    # If we have tokens (bot info), validate them
    if config.tokens:
        for token_name, token_config in config.tokens.items():
            if token_config.total_amount <= 0 or token_config.tokens_per_round <= 0:
                raise ValueError(f"Token amounts must be positive for {token_name}")
            if not token_config.token_id:
                raise ValueError(f"Token ID must be provided for {token_name}")
    
    # For regular config, we only need basic node info
    elif not all([config.node_url, config.explorer_url, config.api_key, config.network_type,
                 config.node_address]):
        raise ValueError("All node configuration fields must be filled")