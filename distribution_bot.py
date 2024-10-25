# distribution_bot.py
import asyncio
import logging
import sys
import json
import argparse
from ergo_python_appkit.appkit import ErgoAppKit
from config import load_config, TokenConfig, DistributionType
from utxo_scanner import scan_proxy_utxos
from org.ergoplatform.appkit import Address
from dataclasses import dataclass
from typing import List, Dict, Set
from decimal import Decimal
import math
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

@dataclass
class UTXOSet:
    boxes: List[Dict]
    total_erg: int
    token_amounts: Dict[str, int]
    box_ids: Set[str]
    
def select_utxos(all_utxos: Dict[str, List[Dict]], token_distributions: List[TokenDistribution], num_recipients: int) -> UTXOSet:
    """
    Intelligently select UTXOs for all token distributions while avoiding double-spending
    """
    needed_erg = (NANOERG_PER_RECIPIENT + MIN_BOX_VALUE) * num_recipients + FEE
    selected_boxes = []
    selected_box_ids = set()
    total_erg = 0
    token_amounts = {dist.token_id: 0 for dist in token_distributions}
    
    # First pass: try to find boxes with multiple tokens
    multi_token_boxes = []
    for token_id, utxos in all_utxos.items():
        for utxo in utxos:
            if len(utxo['tokens']) > 1:
                multi_token_boxes.append(utxo)
    
    # Sort multi-token boxes by number of needed tokens they contain
    multi_token_boxes.sort(key=lambda box: sum(1 for tid in token_amounts.keys() if tid in box['tokens']), reverse=True)
    
    # Use multi-token boxes first
    for box in multi_token_boxes:
        if box['box_id'] not in selected_box_ids:
            selected_boxes.append(box)
            selected_box_ids.add(box['box_id'])
            total_erg += box['value']
            for token_id, amount in box['tokens'].items():
                if token_id in token_amounts:
                    token_amounts[token_id] += amount
    
    # Second pass: fill in missing amounts with single-token boxes
    for dist in token_distributions:
        if token_amounts[dist.token_id] < dist.total_amount:
            needed_amount = dist.total_amount - token_amounts[dist.token_id]
            
            # Sort boxes by amount to minimize box count
            available_boxes = [
                box for box in all_utxos.get(dist.token_id, [])
                if box['box_id'] not in selected_box_ids
            ]
            available_boxes.sort(key=lambda box: box['tokens'].get(dist.token_id, 0), reverse=True)
            
            for box in available_boxes:
                if token_amounts[dist.token_id] >= dist.total_amount:
                    break
                    
                selected_boxes.append(box)
                selected_box_ids.add(box['box_id'])
                total_erg += box['value']
                token_amounts[dist.token_id] += box['tokens'].get(dist.token_id, 0)
    
    # Verify we have enough of everything
    missing_tokens = []
    for dist in token_distributions:
        if token_amounts[dist.token_id] < dist.total_amount:
            missing_tokens.append(f"{dist.token_id}: need {dist.total_amount}, have {token_amounts[dist.token_id]}")
    
    if missing_tokens:
        raise ValueError(f"Insufficient token amounts: {', '.join(missing_tokens)}")
    
    if total_erg < needed_erg:
        raise ValueError(f"Insufficient ERG: need {needed_erg/ERG_TO_NANOERG:.9f}, have {total_erg/ERG_TO_NANOERG:.9f}")
    
    return UTXOSet(
        boxes=selected_boxes,
        total_erg=total_erg,
        token_amounts=token_amounts,
        box_ids=selected_box_ids
    )
    
def parse_arguments():
    parser = argparse.ArgumentParser(description='Token Distribution Bot')
    parser.add_argument('config', help='Path to config.json file')
    parser.add_argument('bot_info', help='Path to bot_info.json file')
    parser.add_argument('--log-level', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       default='INFO',
                       help='Set the logging level')
    return parser.parse_args()

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

async def main():
    args = parse_arguments()
    logging.getLogger().setLevel(args.log_level)
    
    try:
        # Load config
        logger.info(f"Loading config from {args.config}")
        config = load_config(args.config)
        
        # Load bot information
        logger.info(f"Loading bot info from {args.bot_info}")
        try:
            with open(args.bot_info, "r") as f:
                bot_info = json.load(f)
        except FileNotFoundError:
            logger.error(f"Bot info file not found: {args.bot_info}")
            sys.exit(1)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in bot info file: {args.bot_info}")
            sys.exit(1)
            
        # Validate required bot info fields
        required_fields = [
            "proxy_contract_address",
            "tokens",
            "recipient_wallets",
            "blocks_between_dispense",
            "unlock_height"
        ]
        
        missing_fields = [field for field in required_fields if field not in bot_info]
        if missing_fields:
            logger.error(f"Missing required fields in bot info: {', '.join(missing_fields)}")
            sys.exit(1)
        
        proxy_address = bot_info["proxy_contract_address"]
        tokens_info = bot_info["tokens"]
        recipient_wallets = bot_info["recipient_wallets"]
        blocks_between_dispense = bot_info["blocks_between_dispense"]
        
        logger.info(f"Initializing with proxy address: {proxy_address}")
        logger.info(f"Number of tokens to distribute: {len(tokens_info)}")
        logger.info(f"Number of recipients: {len(recipient_wallets)}")
        logger.info(f"Blocks between dispense: {blocks_between_dispense}")
        
        appKit = ErgoAppKit(
            config.node_url,
            config.network_type,
            config.explorer_url,
            config.api_key
        )
        
        proxy_contract = appKit.contractFromAddress(proxy_address)
        current_round = 1
        
        while True:
            try:
                # Scan for all UTXOs at once
                all_utxos = {}
                utxo_scan_time = time.time()
                
                for token_name, token_info in tokens_info.items():
                    token_id = token_info["token_id"]
                    utxos = scan_proxy_utxos(appKit, proxy_address, token_id)
                    if utxos:
                        all_utxos[token_id] = utxos
                        logger.debug(f"Found {len(utxos)} UTXOs for {token_name}")
                
                if all_utxos:
                    try:
                        # Calculate all token distributions first
                        token_distributions = calculate_token_distributions(
                            all_utxos,
                            tokens_info,
                            current_round,
                            len(recipient_wallets)
                        )
                        
                        if token_distributions:
                            logger.info(f"Planning distribution for {len(token_distributions)} tokens")
                            
                            # Select UTXOs atomically for all distributions
                            try:
                                utxo_set = select_utxos(
                                    all_utxos,
                                    token_distributions,
                                    len(recipient_wallets)
                                )
                                
                                # Verify boxes are still available
                                box_age = time.time() - utxo_scan_time
                                if box_age > 30:  # If boxes are more than 30 seconds old, rescan
                                    logger.info("Rescanning boxes to ensure availability...")
                                    for box_id in utxo_set.box_ids:
                                        if not appKit.getBoxesById([box_id]):
                                            raise ValueError(f"Box {box_id} no longer available")
                                
                                # Log distribution plan
                                logger.info("Distribution plan:")
                                for dist in token_distributions:
                                    token_name = next(
                                        name for name, info in tokens_info.items() 
                                        if info["token_id"] == dist.token_id
                                    )
                                    logger.info(f"- {token_name}: {dist.total_amount} tokens ({dist.amount_per_recipient} per recipient)")
                                
                                # Execute distribution
                                tx_id = distribute_multiple_tokens(
                                    appKit,
                                    utxo_set.boxes,
                                    token_distributions,
                                    recipient_wallets,
                                    proxy_contract
                                )
                                
                                logger.info(f"Successfully distributed tokens in transaction {tx_id}")
                                
                            except ValueError as ve:
                                logger.warning(f"UTXO selection failed: {str(ve)}")
                        else:
                            logger.info("No tokens ready for distribution this round")
                            
                    except Exception as e:
                        logger.error(f"Error planning distribution: {str(e)}")
                        logger.exception("Full stack trace:")
                else:
                    logger.info("No UTXOs available")
                
                current_round += 1
                await asyncio.sleep(blocks_between_dispense * 120)
                
            except Exception as e:
                logger.error(f"Error during distribution cycle: {str(e)}")
                logger.exception("Full stack trace:")
                await asyncio.sleep(60)
                
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.exception("Full stack trace:")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())