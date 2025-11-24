import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Inicio de ejecución",
                "evento": event
            }
        }
        print(json.dumps(log_entrada))
        
        # Validar estructura del evento
        if 'body' not in event:
            raise KeyError("El evento no contiene el campo 'body'")
            
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log de éxito
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "response_metadata": response.get('ResponseMetadata', {})
            }
        }
        print(json.dumps(log_exito))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }

    except KeyError as e:
        error_log = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Falta campo requerido en el evento",
                "error": str(e),
                "evento_recibido": event
            }
        }
        print(json.dumps(error_log))
        return {
            'statusCode': 400,
            'error': f'Falta campo requerido: {str(e)}'
        }
        
    except Exception as e:
        error_log = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error interno del servidor",
                "error": str(e),
                "evento_recibido": event
            }
        }
        print(json.dumps(error_log))
        return {
            'statusCode': 500,
            'error': f'Error interno: {str(e)}'
        }