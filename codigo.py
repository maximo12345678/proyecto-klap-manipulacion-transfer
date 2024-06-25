import pandas as pd
import pytz
from datetime import datetime
from datetime import timedelta
import time
import boto3
import os
import psycopg2
import s3fs
import openpyxl
import math
from io import StringIO
from libuser import *


# Crea DataFrame para archivo LOG    
dfRegistrosProceso = pd.DataFrame(columns=["Fecha-Hora", "Descripcion"])


# Funcion principal
def lambda_handler(event, context):
    
    # Inicializamos la variable global creada afuera.
    global dfRegistrosProceso
    
    # Fecha
    cltime = pytz.timezone('America/Santiago')
    dateLocal = datetime.now(cltime)
    vFechaProceso = dateLocal.strftime('%Y-%m-%d_%H-%M-%S')

    print(f"Empieza ejecucion. {vFechaProceso}")
    
    # Datos Input
    fileNameParametrosInput = 'Parametros_proceso.par'
    fileNameEstadoInput = 'Transfer_Banco_Estado.xlsx'
    prefijoBciInput = "Transfer_Bci_id_"

    
    # Datos Output.
    fileNameNominaEstado = f"NOMINA_Banco_Estado_{vFechaProceso}.txt"
    fileNameNominaBci = f"NOMINA_Banco_Bci_{vFechaProceso}.txt"
    fileNameTefEstado = f'TEF_Banco_Estado_{vFechaProceso}'
    fileNameTefBci = f'TEF_Banco_Bci_{vFechaProceso}'
    fileNameTransferEstado = f"Transfer_Banco_Estado_{vFechaProceso}.xlsx"
    
    
    # Definimos variables necesarias para el proceso
    s3Resource = boto3.resource('s3')
    s3Client = boto3.client('s3')
    bucket = os.environ['BUCKET']
    respuesta = ""
    columnas_formato_bci = [
                'Nº Cuenta de Cargo', 
                'Nº Cuenta de Destino', 
                'Banco Destino', 
                'Rut Beneficiario', 
                'Dig. Verif. Beneficiario', 
                'Nombre Beneficiario', 
                'Monto Transferencia', 
                'Nro.Factura Boleta (1)', 
                'Nº Orden de Compra(1)', 
                'Tipo de Pago(2)', 
                'Mensaje Destinatario (3)', 
                'Email Destinatario(3)', 
                'Cuenta Destino inscrita como(4)'
    ]    
    columnas_formato_santander = [
                'Cta_origen', 
                'moneda_origen', 
                'Cta_destino', 
                'moneda_destino', 
                'Cod_banco', 
                'RUT benef.', 
                'nombre benef.', 
                'Mto Total', 
                'Glosa TEF', 
                'Correo', 
                'Glosa correo', 
                'Glosa Cartola Cliente', 
                'Glosa Cartola Beneficiario'
    ]    
    columnas_formato_chile = [
                'RUT BENEFICIARIO', 
                'DV', 
                'NOMBRE', 
                'CORREO', 
                'BANCO', 
                'CUENTA', 
                'MONTO'
    ]    
    procesoHabilitado = False

    
    agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Inicio proceso Manipulacion de Transfer.")
    

    # Recupera parametros del proceso
    traerArchivoParametrosS3 = getFileCsv(bucket, s3Client, fileNameParametrosInput, dateLocal)
    
    # Validamos que se haya descargado correctamente el archivo, para no seguir con el proceso innecesariamente
    if (traerArchivoParametrosS3['estado'] == "Correcto"):
        
        dfPARAMETROS = fRECUPERA_PARAMETROS(pd, traerArchivoParametrosS3['rutaTmp'])
        
        print("Archivo de parametros traido correctamente.")
        
        agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Archivo de Parametros traido correctamente.")
        
        pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI = (fLEER_PARAMETRO(dfPARAMETROS, 'PAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI')).upper()
        
        busquedaTransferBci = checkFileS3(s3Resource, bucket, prefijoBciInput, "Bci", 480)
        fileNameTransferBancoBci = busquedaTransferBci["nameFile"]   
        print("Respuesta busqueda transfer Bci: ", busquedaTransferBci)
        
        # Si se trajo bien el archivo de transfer bci, entra.
        if (busquedaTransferBci["estado"]):
            
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Archivo de Transfer BCI traido correctamente.")
            
            if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'SI'):
                
                # Si entra aca, buscamos el transfer de banco estado tambien.
                tiempoDisponible = 480 - busquedaTransferBci["tiempoUsado"]
                busquedaTransferEstado = checkFileS3(s3Resource, bucket, fileNameEstadoInput, "Estado", tiempoDisponible)
                fileNameTransferBancoEstado = busquedaTransferEstado["nameFile"]  
                print("Respuesta busqueda transfer Estado: ", busquedaTransferEstado)
                
                if (busquedaTransferEstado["estado"]):
                    agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Archivo de Transfer ESTADO traido correctamente.")
                    procesoHabilitado = True
                else:
                    respuesta = "Fallido."
                    agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Error al buscar el Transfer de Banco Estado, no fue encontrado.")
                    errorProceso(dateLocal, s3Resource, bucket, vFechaProceso)
            else:
                procesoHabilitado = True
        else:
            respuesta = "Fallido."
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Error al buscar el Transfer de Banco Bci, no fue encontrado.")
            errorProceso(dateLocal, s3Resource, bucket, vFechaProceso)
        
        
        if (procesoHabilitado):
            
            print("--------------------")
            
            print('Inicio Proceso : ' + vFechaProceso)
          
          
            pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO = fLEER_PARAMETRO(dfPARAMETROS, 'MONTO_MAXIMO_DISPONIBLE_BCO_ESTADO')
            pMONTO_MAXIMO_DISPONIBLE_BCO_BCI = fLEER_PARAMETRO(dfPARAMETROS, 'MONTO_MAXIMO_DISPONIBLE_BCO_BCI')
            
            print('')
            print('====================')
            print('Parametros recibidos')
            print('====================')
            print('pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI : ' + pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI)
            print('pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO : ' + str(pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO))
            print('pMONTO_MAXIMO_DISPONIBLE_BCO_BCI : ' + str(pMONTO_MAXIMO_DISPONIBLE_BCO_BCI))
            print('')
            
            # Registramos en el LOG los parametros:
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "====================")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Parametros recibidos")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Pagar transacciones Banco ESTADO X Banco BCI: {pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI}")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Monto maximo disponible Banco ESTADO: {str(pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO)}")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Monto maximo disponible Banco BCI: {str(pMONTO_MAXIMO_DISPONIBLE_BCO_BCI)}")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "====================")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Inicia procesamiento de archivos.")
            
            
            if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'SI'):
                
                print("Se va a procesar el transfer de Banco Estado.")
                
                traerArchivoBancoEstadosS3 = getFileCsv(bucket, s3Client, fileNameTransferBancoEstado, dateLocal)
                
                # Validamos que se haya descargado correctamente el archivo, para no seguir con el proceso innecesariamente
                if (traerArchivoBancoEstadosS3['estado'] == "Correcto"):
                
                    # Recupera el Dataframe del archivo Banco Estado
                    dfTRANSFER_BANCO_ESTADO = fRECUPERA_TRANSFER_BANCO_ESTADO(pd, traerArchivoBancoEstadosS3['rutaTmp'])
                    
                    # Recupera monto total a pagar del archivo Banco Estado
                    cTRANSFER_BANCO_ESTADO_SUMA_MONTO = fLEER_TRANSFER_BANCO_ESTADO_SUMA_MONTO(dfTRANSFER_BANCO_ESTADO)
                    
                    print(f'Monto total a pagar transfer Banco Estado: {cTRANSFER_BANCO_ESTADO_SUMA_MONTO}')
                    
                    
                    # Si el monto disponible en Banco Estado es insuficiente para pagar el monto total a pagar del transfer Banco Estado    
                    if (pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO < cTRANSFER_BANCO_ESTADO_SUMA_MONTO):
                        
                        # Genera Dataframe con todas las transacciones que alcanzan a pagarse con el saldo del Banco Estado, desde aca ya se llama a la funcion que sube el archivo al Bucket.
                        dfOUT_TRANSFER_BANCO_ESTADO = fGENERA_TRANSFER_BANCO_ESTADO_X_MONTO_DISPONIBLE(pd, dfTRANSFER_BANCO_ESTADO, pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO, bucket, s3Client, fileNameTransferBancoEstado, fileNameTransferEstado, openpyxl, psycopg2, vFechaProceso)
                        
                        # Genera Dataframe con todas las transacciones que no alcanzo para pagar con el saldo de Banco Estado, y luego se pagara BCI.
                        dfOUT_TRANSFER_BANCO_BCI = fGENERA_TRANSFER_BANCO_BCI_NO_BANCO_ESTADO(pd, dfTRANSFER_BANCO_ESTADO, pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO)
                        
                        
                        print("DataFrame que queda en archivo Estado: ", dfOUT_TRANSFER_BANCO_ESTADO)
                        print("DataFrame de transacciones de Estado, que se pagaran con Bci: ", dfOUT_TRANSFER_BANCO_BCI)
                        
                        print(f'Monto pagado por Banco Estado : {fLEER_OUT_TRANSFER_BANCO_ESTADO_SUMA_MONTO(dfOUT_TRANSFER_BANCO_ESTADO)}')
                        print(f'Monto a pagar por Banco BCI del banco Estado : {fLEER_OUT_TRANSFER_BANCO_BCI_SUMA_MONTO(dfOUT_TRANSFER_BANCO_BCI)}')
                        
                        
                        df = fCATEGORIZA_FORMA_PAGO_FLUJO(pd, dfOUT_TRANSFER_BANCO_BCI, pMONTO_MAXIMO_DISPONIBLE_BCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI)
                        print("DataFrame categorizado: ", df)
                        
                        fGENERA_ARCHIVO_NOMINA(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client, fileNameNominaEstado, fileNameNominaBci, psycopg2, datetime, vFechaProceso)
                        fGENERA_ARCHIVO_TEF(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client, fileNameTefEstado, psycopg2, math, columnas_formato_bci, columnas_formato_santander, vFechaProceso)
                        fGENERA_ARCHIVO_LBTR(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client,  psycopg2, columnas_formato_chile, vFechaProceso)
                        
                        
                        pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI = "NO"
                        pMONTO_MAXIMO_DISPONIBLE_BCO_BCI = pMONTO_MAXIMO_DISPONIBLE_BCO_BCI - fLEER_OUT_TRANSFER_BANCO_BCI_SUMA_MONTO(dfOUT_TRANSFER_BANCO_BCI)
                        print("Monto BCI restante: ", pMONTO_MAXIMO_DISPONIBLE_BCO_BCI)
                    
                    # Aca entra cuando el monto disponible del Banco Estado, alcance para pagar el monto total.
                    else:
                        print('El monto disponible de Banco Estado es suficiente para pagar el monto total a pagar.')
                        pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI = "NO"
                
                else:
                    respuesta = "Fallido."
                    errorProceso(dateLocal, s3Resource, bucket, vFechaProceso)
            
            if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'NO'):
                
                print("Se van a procesar las transacciones del Banco Bci.")
                
                traerArchivoBancoBciS3 = getFileCsv(bucket, s3Client, fileNameTransferBancoBci, dateLocal)
                
                # Validamos que se haya descargado correctamente el archivo, para no seguir con el proceso innecesariamente
                if (traerArchivoBancoBciS3['estado'] == "Correcto"):
                    
                    dfTRANSFER_BANCO_BCI = pd.read_csv(traerArchivoBancoBciS3['rutaTmp'], sep='\n', header = None)
                    
                    df = fCATEGORIZA_FORMA_PAGO_FLUJO(pd, dfTRANSFER_BANCO_BCI, pMONTO_MAXIMO_DISPONIBLE_BCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI)
                    print("DataFrame categorizado : ", df)
                    
                    fGENERA_ARCHIVO_NOMINA(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client, fileNameNominaEstado, fileNameNominaBci, psycopg2, datetime, vFechaProceso)
                    fGENERA_ARCHIVO_TEF(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client, fileNameTefBci, psycopg2, math, columnas_formato_bci, columnas_formato_santander, vFechaProceso)
                    fGENERA_ARCHIVO_LBTR(pd, df, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, bucket, s3Client,  psycopg2, columnas_formato_chile, vFechaProceso)
                  
                    respuesta = "Exitoso!"
                    
                else:
                    respuesta = "Fallido."
                    errorProceso(dateLocal, s3Resource, bucket, vFechaProceso)
            
            if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI != 'SI' and pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI != 'NO'):
                print("El parametro tiene que ser SI o NO, en este caso no coincide con ninguno de los dos.")
            
            
            print('Termino Proceso : ' + vFechaProceso)
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Proceso terminado.")
            
            print(f"En la carpeta OUTPUT busca la carpeta '{vFechaProceso}' para ver los resultados.")
            agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"En la carpeta OUTPUT busca la carpeta '{vFechaProceso}' para ver los resultados.")
            
            envioCorreo(dateLocal)
            
            # Subir archivo LOG al bucket.
            subirArchivoLog(s3Resource, bucket, vFechaProceso)
           
            # Borramos los archivos transfer del bucket de la carpeta Input y los copiamos en la carpeta 'archivos_procesados'
            # moverArchivosInput(s3Client, bucket, vFechaProceso, fileNameTransferBancoBci)
            # moverArchivosInput(s3Client, bucket, vFechaProceso, fileNameTransferBancoEstado)
            
            # Limpiamos el DF global, solo filas no columnas..
            dfRegistrosProceso.drop(dfRegistrosProceso.index, inplace=True)
            print("Log limpio: ", dfRegistrosProceso)
        
        
    else:
        respuesta = "Fallido."
        errorProceso(dateLocal, s3Resource, bucket, vFechaProceso)
        
    
    return {
        'statusCode': 200,
        'body': respuesta
    }
    


# Trae una lista de los archivos que estan en el bucket, buscando por un PREFIJO.
def getListFilesFromS3(s3Resource, bucket, nameFile):
    
    prefijoCompleto = f"manipulacion-transfer/input/{nameFile}"
    
    bucketFiles = s3Resource.Bucket(bucket)
    return [obj.key for obj in bucketFiles.objects.filter(Prefix=prefijoCompleto)]
    
    
# Buscamos si el archivo que se quiere buscar, esta dentro de la lista. sigue intentando hasta encontrarlo  
def checkFileS3(s3Resource, bucket, prefijoNameFile, banco, tiempoMaximo):
    
    print("------------------------------")
    print("Comienza busqueda de archivo.")
    print("Tiempo disponible de busqueda: ", tiempoMaximo)
    print(f"Buscando: '{prefijoNameFile}'")


    bandera = False
    tiempo = 0
    intentos = 0

    response = {
        "estado": False,
        "nameFile": "",
        "tiempoUsado": 0
    }

    filesInBucket = getListFilesFromS3(s3Resource, bucket, prefijoNameFile)
    
    while (not bandera):
        
        intentos += 1
        print("=================")
        print(f"Intento N°{intentos}")
        
        if (tiempo >= tiempoMaximo):
            print("Se supero el tiempo permitido para esperar la llegada de los archivos necesarios para empezar el proceso.")
            break;
        
        if (len(filesInBucket) == 1):
            print("El archivo fue encontrado en el bucket.")
            bandera = True 
            response["estado"] = True
            response["tiempoUsado"] = tiempo
            response["nameFile"] = filesInBucket[0].split("/")[2]
           
        else:
            print("El archivo no fue encontrado en la lista.")
            print("Espera de 10 segundos...")
            time.sleep(10)
            tiempo+=10
            filesInBucket = getListFilesFromS3(s3Resource, bucket, prefijoNameFile)
        
    
    print("=================")
    return response


# Funcion que envia correo, recibe parametro con el cuerpo.
def envioCorreo(dateLocal):
    
    global dfRegistrosProceso

    print("Comienza envio de correo.")
    
    # Definimos las variables.
    RECIPIENTS = os.environ['CORREOS'].split(',')
    CONFIGURATION_SET = os.environ['CONFIGURACION_CORREO']
    AWS_REGION = os.environ['REGION']
    SENDER = "procesos@bst.cl"
    SUBJECT = "Notificacion Manipulacion de Transfer"
    CHARSET = "UTF-8"
    client = boto3.client('ses',region_name=AWS_REGION)
    
    BODY_TEXT = ""
    BODY_HTML = """
	            <html>
				  <head>
					<style>
						table {
						  font-family: arial, sans-serif;
						  border-collapse: collapse;
						  width: 50%;
						}

						td, th {
						  border: 1px solid #dddddd;
						  text-align: center;
						  padding: 8px;
						}

						tr:nth-child(even) {
						  background-color: #dddddd;
						}
					</style>
				  </head>
				  <body>
			        Estimados(as)<br><br>
			        Se informa el termino del proceso de Manipulacion de Transfer.
			        <br><br>
			    """

    for index, row in dfRegistrosProceso.iterrows():
	    BODY_HTML = BODY_HTML + dfRegistrosProceso.loc[index]['Fecha-Hora'] + " - " + dfRegistrosProceso.loc[index]['Descripcion'] + "<br>"

    BODY_HTML = BODY_HTML + "<br></body></html>"
    
    # Se intenta enviar el correo.
    try:
        print("Correos a enviar: ", RECIPIENTS)
        
        for RECIPIENT in RECIPIENTS:
            response = client.send_email(
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER,
                ConfigurationSetName=CONFIGURATION_SET,
            )
            print(f"Enviado bien a '{RECIPIENT}'.")
            
        
        print("Correo/s enviado correctamente.")
        # Lo ponemos aca, ya que el correo ya se envio. Y en el mensaje del correo, no queda bien el mensaje 'comienza envio de correo'. Pero si van estos dos mensajes para el archivo LOG.
        agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Comienza envio de correo.")
        agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Correo enviado correctamente.")

    except (Exception, psycopg2.Error) as error :
        print(f"Hubo un error en el envio del correo a '{RECIPIENT}'. ERROR = {error}")
        agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Hubo un error en el envio del correo a '{RECIPIENT}'. ERROR = {error}")


#    
def moverArchivosInput(s3Client, bucket, fecha, fileName):

    fileInput = f"manipulacion-transfer/input/{fileName}"
    fileProcess = f"manipulacion-transfer/archivos-procesados/{fecha}/{fileName}"
    
    try:
        print("Se movera el archivo Transfer de la carpeta 'input' a la carpeta 'archivos-procesados'.")
        # nos fijamos que el archivo este en el bucket, ya que quizas se elimino manualmente ya.
        s3Client.head_object(Bucket=bucket, Key=fileInput)
        s3Client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': fileInput}, Key=fileProcess)
        s3Client.delete_object(Bucket=bucket, Key=fileInput)
        print(f"Archivo '{fileInput}' movido a '{fileProcess}'")
        
    except Exception as e:
        print(f'Error: {e}')
  
  
   
# Funcion reutilizable para llamar cada vez que se quiera agregar un registro al LOG.  
def agregarRegistroLog(fecha, descripcion):
    global dfRegistrosProceso
    
    dfRegistrosProceso = dfRegistrosProceso.append({"Fecha-Hora": fecha, "Descripcion": descripcion}, ignore_index=True)

  
# Subimos el archivo LOG al S3.  
def subirArchivoLog(s3Resource, bucket, fecha):
    global dfRegistrosProceso
    
    nameFile = f"manipulacion-transfer/output/{fecha}/LOG_{fecha}.txt"

    # variable donde nos queda guardado el csv.
    bufferCsv = StringIO()
    
    # creamos el archivo csv.
    dfRegistrosProceso.to_csv(bufferCsv, index = False) 
    print("Log: ", bufferCsv.getvalue())
    
    # enviamos el CSV al S3
    s3Resource.Object(bucket, nameFile).put(Body = bufferCsv.getvalue())
    print("Carga al S3 correctamente")
    
   
# Funcion que llamamos desde 3 puntos, en la llamada de los 3 archivos de input. Para reutilizar, solo se llama en caso de error.
def errorProceso(dateLocal, s3Resource, bucket, vFechaProceso):
    
    agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), "Proceso finalizado por error.")
    envioCorreo(dateLocal)
    
    # subir archivo al bucket.
    subirArchivoLog(s3Resource, bucket, vFechaProceso)
    
    
# Trae el archivo CSV con el diccionario.
def getFileCsv(bucket, s3Client, fileName, dateLocal):
    
    global dfRegistrosProceso
    
    respuesta = {
        "estado": "",
        "rutaTmp": ""
    }
    
    rutaTmp = f"/tmp/input-{fileName}"
    ruta = f"manipulacion-transfer/input/{fileName}"

    print("Ruta de archivo a traer: ", ruta)

    #agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Ruta de archivo a traer: {ruta}.")

    
    try:
        # fs = s3fs.S3FileSystem(anon=False)
        
        # if (fs.exists(bucket + "/" + ruta)):
            
        # Buscamos el archivo en el S3 y se descarga en la carpeta temporal TMP
        with open(rutaTmp, 'wb') as data:
            s3Client.download_fileobj(bucket, ruta, data)
        
        respuesta["estado"] = "Correcto"
        respuesta["rutaTmp"] = rutaTmp
          
        # else:
        #     print("El archivo no existe en el bucket.")
        #     respuesta["estado"] = "Error"
        
        return respuesta

    except (Exception, psycopg2.Error) as error :
        print(f"Hubo un error en la descarga del archivo '{fileName}': ERROR = {error}")
        respuesta["estado"] = "Error"
        agregarRegistroLog(str(dateLocal.strftime('%Y-%m-%d_%H-%M-%S')), f"Error al descargar el archivo '{fileName}': {error}.")
        return respuesta
    
