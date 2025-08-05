# kafka_utils.py
import uuid
import logging
from typing import Dict, Any
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def consume_function(messages: list, **kwargs) -> Dict[str, Any]:
    """Обрабатывает пачку сообщений и возвращает статистику"""
    CONN_ID_KAP = 'kap_247_db'
    SCHEMA_NAME = 'kap_247_scpl'
    TMP_TABLE = 'bulkagentservicesevent_tmp'
    OFFSET_STORAGE_KEY = kwargs.get('offset_storage_key', 'default_last_offset')
    
    rqUid = kwargs.get('rqUid', str(uuid.uuid4()))
    
    if not messages:
        return {'processed': 0, 'last_offset': None}
    
    conn = BaseHook.get_connection(CONN_ID_KAP)
    engine = create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
    )
    
    processed = 0
    last_offset = None
    
    try:
        with engine.begin() as conn:
            records = []
            for message in messages:
                if not message or not hasattr(message, 'value'):
                    continue
                
                msg_value = message.value()
                if not msg_value:
                    continue
                
                try:
                    decoded_message = msg_value.decode('utf-8')
                    message_key = message.key().decode('utf-8') if message.key() else str(uuid.uuid4())
                    records.append({
                        "key": message_key,
                        "message": decoded_message,
                        "offset": message.offset()
                    })
                    last_offset = message.offset()
                except Exception as e:
                    logger.error(f"Ошибка декодирования сообщения: {str(e)}")
                    continue
            
            if records:
                conn.execute(
                    text(f"""
                    INSERT INTO {SCHEMA_NAME}.{TMP_TABLE} 
                    (message_key, text_json, processing_status, kafka_offset)
                    VALUES (:key, :message, 'PROCESSED', :offset)
                    """),
                    [{"key": r['key'], "message": r['message'], "offset": r['offset']} for r in records]
                )
                processed = len(records)
                
                if last_offset is not None:
                    Variable.set(OFFSET_STORAGE_KEY, str(last_offset))
        
        logger.info(f"Успешно обработано {processed} сообщений. Последний offset: {last_offset}")
        return {'processed': processed, 'last_offset': last_offset}
    
    except Exception as e:
        logger.error(f"Критическая ошибка обработки: {str(e)}", exc_info=True)
        raise