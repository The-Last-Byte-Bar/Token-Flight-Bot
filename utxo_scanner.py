# utxo_scanner.py
import logging
from ergo_python_appkit.appkit import ErgoAppKit
from org.ergoplatform.appkit import Address
from typing import List, Dict

logger = logging.getLogger(__name__)

def scan_proxy_utxos(appKit: ErgoAppKit, proxy_address: str, token_id: str = None) -> List[Dict]:
    """
    Scan for unspent boxes in the proxy contract
    
    Parameters:
    -----------
    appKit : ErgoAppKit
        The ErgoAppKit instance
    proxy_address : str
        Address of the proxy contract
    token_id : str, optional
        Specific token ID to scan for. If None, returns all boxes.
    
    Returns:
    --------
    List[Dict]
        List of UTXOs with their box IDs, values, and tokens
    """
    unspent_boxes = appKit.getUnspentBoxes(proxy_address)
    
    utxos = []
    for box in unspent_boxes:
        box_tokens = {token.getId().toString(): token.getValue() for token in box.getTokens()}
        
        # If token_id is specified, only include boxes containing that token
        if token_id is None or token_id in box_tokens:
            utxos.append({
                "box_id": box.getId().toString(),
                "value": box.getValue(),
                "tokens": box_tokens
            })
    
    if token_id:
        logger.info(f"Found {len(utxos)} UTXOs containing token {token_id}")
    else:
        logger.info(f"Found {len(utxos)} total UTXOs in proxy contract")
    
    return utxos