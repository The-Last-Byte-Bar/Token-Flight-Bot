import logging
from ergo_python_appkit.appkit import ErgoAppKit
from org.ergoplatform.appkit import Address
from typing import List, Dict

logger = logging.getLogger(__name__)

ERG_TO_NANOERG = 1e9
NANOERG_PER_RECIPIENT = int(0.001 * ERG_TO_NANOERG)
MIN_BOX_VALUE = int(0.001 * ERG_TO_NANOERG)
FEE = int(0.001 * ERG_TO_NANOERG)

def distribute_tokens(appKit: ErgoAppKit, utxos: List[Dict], token_id: str, recipient_addresses: List[str], tokens_per_round: int, proxy_contract) -> None:
    total_available_erg = sum(utxo['value'] for utxo in utxos)
    total_available_tokens = sum(utxo['tokens'].get(token_id, 0) for utxo in utxos)

    logger.info(f"Total available ERG: {total_available_erg/ERG_TO_NANOERG:.9f}")
    logger.info(f"Total available tokens: {total_available_tokens}")

    # Ensure we have enough tokens for at least one round
    if total_available_tokens < tokens_per_round:
        logger.warning(f"Not enough tokens for a full round. Available: {total_available_tokens}, Required: {tokens_per_round}")
        return

    # Calculate tokens per recipient for this round
    num_recipients = len(recipient_addresses)
    tokens_per_recipient = tokens_per_round // num_recipients
    
    if tokens_per_recipient == 0:
        logger.warning(f"Not enough tokens to distribute to all recipients. Tokens per round: {tokens_per_round}, Recipients: {num_recipients}")
        return

    # Recalculate the actual number of tokens to distribute this round
    tokens_to_distribute = tokens_per_recipient * num_recipients

    # Calculate ERG requirements
    erg_per_recipient = NANOERG_PER_RECIPIENT + MIN_BOX_VALUE
    total_erg_needed = (erg_per_recipient * num_recipients) + FEE + MIN_BOX_VALUE  # Include change box

    if total_available_erg < total_erg_needed:
        logger.warning(f"Not enough ERG for distribution. Available: {total_available_erg/ERG_TO_NANOERG:.9f}, Needed: {total_erg_needed/ERG_TO_NANOERG:.9f}")
        return

    logger.info(f"Distributing {tokens_to_distribute} tokens to {num_recipients} recipients")
    logger.info(f"Tokens per recipient: {tokens_per_recipient}")
    logger.info(f"ERG per recipient: {erg_per_recipient/ERG_TO_NANOERG:.9f}")

    input_boxes = []
    for utxo in utxos:
        boxes = appKit.getBoxesById([utxo['box_id']])
        if boxes:
            input_boxes.append(boxes[0])
        else:
            logger.warning(f"Box with id {utxo['box_id']} not found")

    if not input_boxes:
        logger.error("No valid input boxes found")
        return

    outputs = []
    total_erg_used = 0
    total_tokens_used = 0

    for recipient in recipient_addresses:
        output = appKit.buildOutBox(
            value=erg_per_recipient,
            tokens={token_id: tokens_per_recipient},
            registers=None,
            contract=appKit.contractFromAddress(recipient)
        )
        outputs.append(output)
        total_erg_used += erg_per_recipient
        total_tokens_used += tokens_per_recipient

    total_erg_used += FEE
    change_value = total_available_erg - total_erg_used
    change_tokens = total_available_tokens - total_tokens_used

    logger.info(f"Total ERG to be used: {total_erg_used/ERG_TO_NANOERG:.9f}")
    logger.info(f"Total tokens to be distributed: {total_tokens_used}")
    logger.info(f"Change ERG: {change_value/ERG_TO_NANOERG:.9f}")
    logger.info(f"Change tokens: {change_tokens}")

    if change_value > 0 or change_tokens > 0:
        change_output = appKit.buildOutBox(
            value=max(change_value, MIN_BOX_VALUE),
            tokens={token_id: change_tokens} if change_tokens > 0 else None,
            registers=None,
            contract=proxy_contract
        )
        outputs.append(change_output)

    try:
        unsigned_tx = appKit.buildUnsignedTransaction(
            inputs=input_boxes,
            outputs=outputs,
            fee=FEE,
            sendChangeTo=proxy_contract.toAddress()
        )
        signed_tx = appKit.signTransactionWithNode(unsigned_tx)
        tx_id = appKit.sendTransaction(signed_tx)
        logger.info(f"Tokens distributed to {num_recipients} recipients. Transaction ID: {tx_id}")
        logger.info(f"Distributed {total_tokens_used} tokens with {total_erg_used/ERG_TO_NANOERG:.9f} ERG")
    except Exception as e:
        logger.error(f"Failed to build or send transaction: {str(e)}")
        logger.info(f"Inputs: {[box.getId().toString() for box in input_boxes]}")
        logger.info(f"Outputs: {[output.getValue() for output in outputs]}")

# token_distribution.py
import logging
from ergo_python_appkit.appkit import ErgoAppKit
from org.ergoplatform.appkit import Address
from typing import List, Dict, Tuple
from dataclasses import dataclass
from decimal import Decimal
import math

logger = logging.getLogger(__name__)

ERG_TO_NANOERG = 1e9
NANOERG_PER_RECIPIENT = int(0.001 * ERG_TO_NANOERG)
MIN_BOX_VALUE = int(0.001 * ERG_TO_NANOERG)
FEE = int(0.001 * ERG_TO_NANOERG)

@dataclass
class TokenDistribution:
    token_id: str
    amount_per_recipient: int
    total_amount: int

def calculate_token_distributions(utxos: Dict[str, List[Dict]], 
                                token_configs: Dict[str, Dict],
                                current_round: int,
                                num_recipients: int) -> List[TokenDistribution]:
    """
    Calculate distribution amounts for all available tokens
    
    Parameters:
    -----------
    utxos : Dict[str, List[Dict]]
        Available UTXOs per token ID
    token_configs : Dict[str, Dict]
        Token configurations from bot_info
    current_round : int
        Current distribution round
    num_recipients : int
        Number of recipients
        
    Returns:
    --------
    List[TokenDistribution]
        Distribution information for each token
    """
    distributions = []
    
    for token_name, token_config in token_configs.items():
        token_id = token_config["token_id"]
        token_utxos = utxos.get(token_id, [])
        
        if not token_utxos:
            continue
            
        # Calculate available amounts
        total_available = sum(
            utxo['tokens'].get(token_id, 0) 
            for utxo in token_utxos
        )
        
        if total_available == 0:
            continue
            
        # Calculate distribution amount based on type and round
        distribution_type = token_config["distribution_type"]
        base_amount = token_config["tokens_per_round"]
        total_rounds = math.ceil(token_config["total_amount"] / base_amount)
        
        # Apply distribution formula based on type
        if distribution_type == "linear":
            round_amount = base_amount
        elif distribution_type == "logarithmic":
            factor = math.log(total_rounds - current_round + 1) / math.log(total_rounds)
            round_amount = int(base_amount * factor)
        elif distribution_type == "quadratic":
            factor = ((total_rounds - current_round) / total_rounds) ** 2
            round_amount = int(base_amount * factor)
        else:  # constant
            round_amount = base_amount
            
        # Calculate per-recipient amount
        amount_per_recipient = min(
            round_amount // num_recipients,
            total_available // num_recipients
        )
        
        if amount_per_recipient > 0:
            total_amount = amount_per_recipient * num_recipients
            distributions.append(TokenDistribution(
                token_id=token_id,
                amount_per_recipient=amount_per_recipient,
                total_amount=total_amount
            ))
            
    return distributions

def distribute_multiple_tokens(
    appKit: ErgoAppKit,
    all_utxos: Dict[str, List[Dict]],
    token_distributions: List[TokenDistribution],
    recipient_addresses: List[str],
    proxy_contract) -> str:
    """
    Distribute multiple tokens in a single transaction
    """
    # Calculate ERG requirements
    num_recipients = len(recipient_addresses)
    erg_per_recipient = NANOERG_PER_RECIPIENT + MIN_BOX_VALUE
    total_erg_needed = (erg_per_recipient * num_recipients) + FEE
    
    # Collect all input boxes
    input_boxes = []
    used_utxos = set()
    total_available_erg = 0
    
    # First, try to find boxes containing multiple tokens to minimize inputs
    for token_dist in token_distributions:
        token_utxos = all_utxos.get(token_dist.token_id, [])
        remaining_amount = token_dist.total_amount
        
        for utxo in token_utxos:
            if utxo['box_id'] in used_utxos:
                continue
                
            box = appKit.getBoxesById([utxo['box_id']])[0]
            if box:
                input_boxes.append(box)
                used_utxos.add(utxo['box_id'])
                total_available_erg += utxo['value']
                remaining_amount -= utxo['tokens'].get(token_dist.token_id, 0)
                
            if remaining_amount <= 0:
                break
                
    if total_available_erg < total_erg_needed:
        raise ValueError(f"Not enough ERG for distribution. Need {total_erg_needed/ERG_TO_NANOERG:.9f}, have {total_available_erg/ERG_TO_NANOERG:.9f}")
    
    # Create output boxes for each recipient
    outputs = []
    for recipient in recipient_addresses:
        # Prepare token amounts for this recipient
        token_amounts = {
            dist.token_id: dist.amount_per_recipient
            for dist in token_distributions
        }
        
        output = appKit.buildOutBox(
            value=erg_per_recipient,
            tokens=token_amounts,
            registers=None,
            contract=appKit.contractFromAddress(recipient)
        )
        outputs.append(output)
    
    # Calculate change amounts
    change_tokens = {}
    for token_dist in token_distributions:
        total_available = sum(
            utxo['tokens'].get(token_dist.token_id, 0)
            for utxo in all_utxos.get(token_dist.token_id, [])
            if utxo['box_id'] in used_utxos
        )
        change_amount = total_available - token_dist.total_amount
        if change_amount > 0:
            change_tokens[token_dist.token_id] = change_amount
    
    # Create change box if needed
    change_value = total_available_erg - total_erg_needed
    if change_value > 0 or change_tokens:
        change_output = appKit.buildOutBox(
            value=max(change_value, MIN_BOX_VALUE),
            tokens=change_tokens if change_tokens else None,
            registers=None,
            contract=proxy_contract
        )
        outputs.append(change_output)
    
    # Build and send transaction
    try:
        unsigned_tx = appKit.buildUnsignedTransaction(
            inputs=input_boxes,
            outputs=outputs,
            fee=FEE,
            sendChangeTo=proxy_contract.toAddress()
        )
        signed_tx = appKit.signTransactionWithNode(unsigned_tx)
        tx_id = appKit.sendTransaction(signed_tx)
        
        logger.info("Multi-token distribution successful!")
        logger.info(f"Transaction ID: {tx_id}")
        for dist in token_distributions:
            logger.info(f"Distributed {dist.total_amount} of token {dist.token_id}")
            logger.info(f"Amount per recipient: {dist.amount_per_recipient}")
        
        return tx_id
        
    except Exception as e:
        logger.error(f"Failed to build or send transaction: {str(e)}")
        raise