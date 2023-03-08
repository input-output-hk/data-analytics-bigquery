import json
import pandas as pd

def query_epoch_tables(epoch_no):
    return [
            query_epoch_param(epoch_no),
            query_param_proposal(epoch_no)
    ]


def query_epoch_param(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.str)) AS hash_b64 FROM
                 (SELECT
                    '('|| (epoch_no)
                       ||',' || (min_fee_a)
                       ||',' || (min_fee_b)
                       ||',' || (max_block_size) 
                       ||',' || (max_tx_size)
                       ||',' || (max_bh_size)    
                       ||',' || (key_deposit) 
                       ||',' || (pool_deposit)
                       ||',' || (max_epoch)    
                       ||',' || (optimal_pool_count) 
                       ||',' || (influence)                 
                       ||',' || (monetary_expand_rate) 
                       ||',' || (treasury_growth_rate)
                       ||',' || (decentralisation)    
                       ||',' || (COALESCE(extra_entropy, 'null')) 
                       ||',' || (protocol_major)                 
                       ||',' || (protocol_minor) 
                       ||',' || (min_utxo_value)   
                       ||',' || (min_pool_cost)    
                       ||',' || (COALESCE(nonce, 'null'))
                       ||',' || (COALESCE(CAST(coins_per_utxo_size AS STRING), 'null'))          
                       ||',' || (COALESCE(CAST(price_mem AS STRING) , 'null'))      
                       ||',' || (COALESCE(CAST(price_step AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_tx_ex_mem AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_tx_ex_steps AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_block_ex_mem AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_block_ex_steps AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_val_size AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(collateral_percent AS STRING) , 'null'))       
                       ||',' || (COALESCE(CAST(max_collateral_inputs AS STRING) , 'null'))       
                       ||')' AS str
                  FROM cardano_mainnet.epoch_param
                  WHERE epoch_no = {epoch_no}
                ) AS innerq;""",
            f"""SELECT encode(SHA256(subq.str::bytea),'base64') AS hash_b64 FROM
                    (SELECT 
                    '('|| (epoch_no)
                       ||',' || (min_fee_a)
                       ||',' || (min_fee_b)
                       ||',' || (max_block_size) 
                       ||',' || (max_tx_size)
                       ||',' || (max_bh_size)    
                       ||',' || (key_deposit) 
                       ||',' || (pool_deposit)
                       ||',' || (max_epoch)    
                       ||',' || (optimal_pool_count) 
                       ||',' || (influence)                 
                       ||',' || (monetary_expand_rate) 
                       ||',' || (treasury_growth_rate)
                       ||',' || (decentralisation)    
                       ||',' || (COALESCE(extra_entropy, 'null'))  
                       ||',' || (protocol_major)                 
                       ||',' || (protocol_minor) 
                       ||',' || (min_utxo_value)   
                       ||',' || (min_pool_cost)    
                       ||',' || (COALESCE(nonce, 'null'))
                       ||',' || (COALESCE(coins_per_utxo_size::text, 'null'))            
                       ||',' || (COALESCE(price_mem::text, 'null'))      
                       ||',' || (COALESCE(to_char(price_step, 'FM90.9999999'), 'null'))       
                       ||',' || (COALESCE(max_tx_ex_mem::text, 'null'))       
                       ||',' || (COALESCE(max_tx_ex_steps::text, 'null'))       
                       ||',' || (COALESCE(max_block_ex_mem::text, 'null'))       
                       ||',' || (COALESCE(max_block_ex_steps::text, 'null'))       
                       ||',' || (COALESCE(max_val_size::text, 'null'))       
                       ||',' || (COALESCE(collateral_percent::text, 'null'))       
                       ||',' || (COALESCE(max_collateral_inputs::text, 'null'))       
                       ||')' AS str
                      FROM analytics.vw_bq_epoch_param WHERE epoch_no = {epoch_no}) AS subq""",
            lambda x: x, lambda x: x)


def query_param_proposal(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.str)) AS hash_b64 FROM
                 (SELECT
                    '('|| (epoch_no)
                       ||',' || (key)
                       ||',' || (COALESCE(CAST(min_fee_a AS STRING), 'null'))
                       ||',' || (COALESCE(CAST(min_fee_b AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(max_block_size AS STRING), 'null'))
                       ||',' || (COALESCE(CAST(max_tx_size AS STRING), 'null'))    
                       ||',' || (COALESCE(CAST(max_bh_size AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(key_deposit AS STRING), 'null'))
                       ||',' || (COALESCE(CAST(pool_deposit AS STRING), 'null'))    
                       ||',' || (COALESCE(CAST(max_epoch AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(optimal_pool_count AS STRING), 'null'))                 
                       ||',' || (COALESCE(CAST(influence AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(monetary_expand_rate AS STRING), 'null'))
                       ||',' || (COALESCE(CAST(treasury_growth_rate AS STRING), 'null'))    
                       ||',' || (COALESCE(CAST(decentralisation AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(entropy AS STRING), 'null'))                 
                       ||',' || (COALESCE(CAST(protocol_major AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(protocol_minor AS STRING), 'null'))   
                       ||',' || (COALESCE(CAST(min_utxo_value AS STRING), 'null'))    
                       ||',' || (COALESCE(CAST(min_pool_cost AS STRING), 'null')) 
                       ||',' || (COALESCE(CAST(coins_per_utxo_size AS STRING), 'null'))                 
                       ||',' || (COALESCE(CAST(price_mem AS STRING), 'null'))      
                       ||',' || (COALESCE(CAST(price_step AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_tx_ex_mem AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_tx_ex_steps AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_block_ex_mem AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_block_ex_steps AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_val_size AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(collateral_percent AS STRING), 'null'))       
                       ||',' || (COALESCE(CAST(max_collateral_inputs AS STRING), 'null'))     
                       ||',' || (registered_tx_slot_no)       
                       ||',' || (registered_tx_index)    
                       ||')' AS str
                  FROM cardano_mainnet.param_proposal
                  WHERE epoch_no = {epoch_no}
                  ORDER BY epoch_no, registered_tx_slot_no, registered_tx_index, key ASC
                ) AS innerq;""",
            f"""SELECT encode(SHA256(subq.str::bytea),'base64') AS hash_b64 FROM
                    (SELECT 
                    '('|| (epoch_no)
                       ||',' || ("key")
                       ||',' || (COALESCE(min_fee_a::text, 'null'))
                       ||',' || (COALESCE(min_fee_b::text, 'null')) 
                       ||',' || (COALESCE(max_block_size::text, 'null'))
                       ||',' || (COALESCE(max_tx_size::text, 'null'))    
                       ||',' || (COALESCE(max_bh_size::text, 'null')) 
                       ||',' || (COALESCE(key_deposit::text, 'null'))
                       ||',' || (COALESCE(pool_deposit::text, 'null'))    
                       ||',' || (COALESCE(max_epoch::text, 'null')) 
                       ||',' || (COALESCE(optimal_pool_count::text, 'null'))                 
                       ||',' || (COALESCE(influence::text, 'null')) 
                       ||',' || (COALESCE(monetary_expand_rate::text, 'null'))
                       ||',' || (COALESCE(treasury_growth_rate::text, 'null'))    
                       ||',' || (COALESCE(decentralisation::text, 'null')) 
                       ||',' || (COALESCE(entropy, 'null'))                 
                       ||',' || (COALESCE(protocol_major::text, 'null')) 
                       ||',' || (COALESCE(protocol_minor::text, 'null'))   
                       ||',' || (COALESCE(min_utxo_value::text, 'null'))    
                       ||',' || (COALESCE(min_pool_cost::text, 'null')) 
                       ||',' || (COALESCE(coins_per_utxo_size::text, 'null'))                 
                       ||',' || (COALESCE(price_mem::text, 'null'))      
                       ||',' || (COALESCE(price_step::text, 'null'))       
                       ||',' || (COALESCE(max_tx_ex_mem::text, 'null'))       
                       ||',' || (COALESCE(max_tx_ex_steps::text, 'null'))       
                       ||',' || (COALESCE(max_block_ex_mem::text, 'null'))       
                       ||',' || (COALESCE(max_block_ex_steps::text, 'null'))       
                       ||',' || (COALESCE(max_val_size::text, 'null'))       
                       ||',' || (COALESCE(collateral_percent::text, 'null'))       
                       ||',' || (COALESCE(max_collateral_inputs::text, 'null'))  
                       ||',' || (registered_tx_slot_no)       
                       ||',' || (registered_tx_index)  
                       ||')' AS str
                      FROM analytics.vw_bq_param_proposal WHERE epoch_no = {epoch_no}) AS subq""",
            lambda x: x, lambda x: x)
