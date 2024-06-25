
# Funcion que recibe como parametro la ruta donde esta ubicado el archivo con los parametros, lo toma y captura el csv con la data y la retorna.
def fRECUPERA_PARAMETROS(pPD, pPATH_ARCHIVO):
    dfPARAMETROS = pPD.read_csv(pPATH_ARCHIVO, sep=';')
    return dfPARAMETROS
   
    
# Funcion que al recibir el nombre del parametro especifico, lo busca en el csv con los parametros, retorna su valor.
def fLEER_PARAMETRO(pPD, pNOMBRE_PARAMETRO):
    return pPD.loc[0][pNOMBRE_PARAMETRO]


# Funcion que recibe como parametro la ruta donde esta ubicado el archivo de Banco Estado.
def fRECUPERA_TRANSFER_BANCO_ESTADO(pPD, pPATH_ARCHIVO):
    df = pPD.read_excel(pPATH_ARCHIVO, header=None, sheet_name='DETALLE', skiprows=4)
    return df


# Funcion que recibe como parametro el Dataframe con todos los registros del archivo Banco Estado, obtiene la suma de todos los montos.
def fLEER_TRANSFER_BANCO_ESTADO_SUMA_MONTO(pPD):
    return pPD[8].sum()
  
    

# Funcion que captura el monto total de las transacciones que quedaron en el Dataframe de Banco Estado.
def fLEER_OUT_TRANSFER_BANCO_ESTADO_SUMA_MONTO(pPD):
    
    if (len(pPD) > 0):
    
        suma = pPD['v_1_monto'].astype(int).sum()
        
        return suma
    else:
        return 0


# Funcion que captura el monto total de las transacciones que quedaron en el Dataframe de Banco Estado.
def fLEER_OUT_TRANSFER_BANCO_BCI_SUMA_MONTO(pPD):
    
    suma = pPD['v_1_monto'].astype(int).sum()
    
    return suma
 
 
 
# Generar DF con las transacciones que se pueden pagar con el monto disponible de Banco Estado por el monto disponible. Recibe pandas, el dataframe del archivo y el monto disponible del Banco Estado.
def fGENERA_TRANSFER_BANCO_ESTADO_X_MONTO_DISPONIBLE(pPD, pTRANSFER_BANCO_ESTADO, pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO, BUCKET, S3, filenameBancoEstadoInput, filenameBancoEstadoOutput, openpyxl, psycopg2, vFechaProceso):
    
    # Define un dataframe vacio
    df = pPD.DataFrame()
    vMontoAcumulado = 0
    posicion = 0
    
    # Se recorre el dataframe del archivo Banco Estado original
    for index, row in pTRANSFER_BANCO_ESTADO.iterrows():

        if vMontoAcumulado <= pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO:
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 1:
                v_1_id = pTRANSFER_BANCO_ESTADO.loc[index][0]
                v_1_rut = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_1_razon_social_o_nombres_y_apellidos = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_1_email = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_1_banco = pTRANSFER_BANCO_ESTADO.loc[index][4]
                v_1_forma_de_pago = pTRANSFER_BANCO_ESTADO.loc[index][5]
                v_1_numero_de_cuenta = pTRANSFER_BANCO_ESTADO.loc[index][6]
                v_1_sector_fin = pTRANSFER_BANCO_ESTADO.loc[index][7]
                v_1_monto = pTRANSFER_BANCO_ESTADO.loc[index][8]
                
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 2:
                v_2_id = pTRANSFER_BANCO_ESTADO.loc[index][0]
                v_2_fecha_doc = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_2_monto_doc = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_2_numero_doc = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_2_tipo_doc = pTRANSFER_BANCO_ESTADO.loc[index][4]
                
                vMontoAcumulado = vMontoAcumulado + v_1_monto
                
                if vMontoAcumulado <= pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO:
                    df = df.append({  
                                    'v_1_id': v_1_id,
                                    'v_1_rut': v_1_rut,
                                    'v_1_razon_social_o_nombres_y_apellidos': v_1_razon_social_o_nombres_y_apellidos,
                                    'v_1_email': v_1_email,
                                    'v_1_banco': v_1_banco,
                                    'v_1_forma_de_pago': v_1_forma_de_pago,
                                    'v_1_numero_de_cuenta': v_1_numero_de_cuenta, 
                                    'v_1_sector_fin': v_1_sector_fin,
                                    'v_1_monto': v_1_monto,
                                    'v_2_id': v_2_id,
                                    'v_2_fecha_doc': v_2_fecha_doc,
                                    'v_2_monto_doc': v_2_monto_doc, 
                                    'v_2_numero_doc': v_2_numero_doc,
                                    'v_2_tipo_doc': v_2_tipo_doc
                                    }, ignore_index=True)
        
        else:
            posicion = index
            break;
        
        
    fGENERAR_ARCHIVO_BANCO_ESTADO(BUCKET, S3, filenameBancoEstadoInput, filenameBancoEstadoOutput, posicion, openpyxl, psycopg2, vFechaProceso)
    return df
 
 
# Generar DF con las transacciones que No se pueden pagar con el monto disponible de Banco Estado, para que luego las pague BCI. Recibe pandas, el dataframe del archivo y el monto disponible del Banco Estado.
def fGENERA_TRANSFER_BANCO_BCI_NO_BANCO_ESTADO(pPD, pTRANSFER_BANCO_ESTADO, pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO):
   
    df = pPD.DataFrame()
    vMontoAcumulado = 0
    
    for index, row in pTRANSFER_BANCO_ESTADO.iterrows():
        
        if vMontoAcumulado > pMONTO_MAXIMO_DISPONIBLE_BCO_ESTADO:
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 1:
                v_1_id = int(float(pTRANSFER_BANCO_ESTADO.loc[index][0]))
                v_1_rut = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_1_razon_social_o_nombres_y_apellidos = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_1_email = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_1_banco = int(float(pTRANSFER_BANCO_ESTADO.loc[index][4]))
                v_1_forma_de_pago = int(float(pTRANSFER_BANCO_ESTADO.loc[index][5]))
                v_1_numero_de_cuenta = int(float(pTRANSFER_BANCO_ESTADO.loc[index][6]))
                v_1_sector_fin = int(float(pTRANSFER_BANCO_ESTADO.loc[index][7]))
                v_1_monto = int(float(pTRANSFER_BANCO_ESTADO.loc[index][8]))
                
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 2:
                v_2_id = int(float(pTRANSFER_BANCO_ESTADO.loc[index][0]))
                v_2_fecha_doc = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_2_monto_doc = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_2_numero_doc = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_2_tipo_doc = int(float(pTRANSFER_BANCO_ESTADO.loc[index][4]))
                
                df = df.append({  
                                'v_1_id': v_1_id,
                                'v_1_rut': v_1_rut,
                                'v_1_razon_social_o_nombres_y_apellidos': v_1_razon_social_o_nombres_y_apellidos,
                                'v_1_email': v_1_email,
                                'v_1_banco': v_1_banco,
                                'v_1_forma_de_pago': v_1_forma_de_pago,
                                'v_1_numero_de_cuenta': v_1_numero_de_cuenta, 
                                'v_1_sector_fin': v_1_sector_fin,
                                'v_1_monto': v_1_monto,
                                'v_2_id': v_2_id,
                                'v_2_fecha_doc': v_2_fecha_doc,
                                'v_2_monto_doc': v_2_monto_doc, 
                                'v_2_numero_doc': v_2_numero_doc,
                                'v_2_tipo_doc': v_2_tipo_doc
                                }, ignore_index=True)
        else:
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 1:
                v_1_id = int(float(pTRANSFER_BANCO_ESTADO.loc[index][0]))
                v_1_rut = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_1_razon_social_o_nombres_y_apellidos = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_1_email = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_1_banco = int(float(pTRANSFER_BANCO_ESTADO.loc[index][4]))
                v_1_forma_de_pago = int(float(pTRANSFER_BANCO_ESTADO.loc[index][5]))
                v_1_numero_de_cuenta = int(float(pTRANSFER_BANCO_ESTADO.loc[index][6]))
                v_1_sector_fin = int(float(pTRANSFER_BANCO_ESTADO.loc[index][7]))
                v_1_monto = int(float(pTRANSFER_BANCO_ESTADO.loc[index][8]))
                vMontoAcumulado = vMontoAcumulado + int(float(pTRANSFER_BANCO_ESTADO.loc[index][8]))
                
            if pTRANSFER_BANCO_ESTADO.loc[index][0] == 2:
                v_2_id = int(float(pTRANSFER_BANCO_ESTADO.loc[index][0]))
                v_2_fecha_doc = pTRANSFER_BANCO_ESTADO.loc[index][1]
                v_2_monto_doc = pTRANSFER_BANCO_ESTADO.loc[index][2]
                v_2_numero_doc = pTRANSFER_BANCO_ESTADO.loc[index][3]
                v_2_tipo_doc = int(float(pTRANSFER_BANCO_ESTADO.loc[index][4]))
    
    return df

 
 
# Le asigna una forma de pago a cada transaccion que va a pagar el banco BCI, y una llave para luego agrupar.
def fCATEGORIZA_FORMA_PAGO_FLUJO(pPD, pTRANSFER_BANCO_BCI, pMONTO_MAXIMO_DISPONIBLE_BCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI):


    if pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'SI':
        
        codigo_banco = 'v_1_banco'
        
        dfTransacciones = pTRANSFER_BANCO_BCI
        
        # Agregar columnas v_llave y v_forma_pago
        dfTransacciones['v_llave'] = ''
        dfTransacciones['v_forma_pago'] = ''
        
        # Crear la llave para cada transacción
        for index, row in dfTransacciones.iterrows():
            dfTransacciones.at[index, 'v_llave'] = str(row['v_1_rut'][:-1]) + "_" + str(row['v_1_rut'][-1]) + "_" + str(row['v_1_banco']) + "_" + str(row['v_1_numero_de_cuenta'])

    else:
        # Unifica lineas de transfer
        dfTransacciones = pPD.DataFrame()
        
        codigo_banco = 'v_0_codigo_banco'
        
        for index, row in pTRANSFER_BANCO_BCI.iterrows():
            
            
            vLineaArchivo = str(pTRANSFER_BANCO_BCI.loc[index][0])
            
            if vLineaArchivo[0] == '0':
                v_0_original = vLineaArchivo
                v_0_rut_beneficiario = int(vLineaArchivo[11:20])
                v_0_dvr_beneficiario = vLineaArchivo[20:21]
                v_0_rut_beneficiario = str(v_0_rut_beneficiario) + v_0_dvr_beneficiario
                v_0_nombre_beneficiario = vLineaArchivo[21:71]
                v_0_correo = vLineaArchivo[82:122]
                v_0_codigo_banco = int(vLineaArchivo[123:128])
                v_0_numero_cuenta = int(vLineaArchivo[128:153])
                v_0_monto = int(vLineaArchivo[153:168])
                v_0_fecha_pago = vLineaArchivo[168:176]
                v_0_llave = str(v_0_rut_beneficiario) + "_" + str(v_0_dvr_beneficiario) + "_" + str(v_0_codigo_banco) + "_" + str(v_0_numero_cuenta)
                
            if vLineaArchivo[0] == '1':
                v_1_original = vLineaArchivo
                v_1_numero_transfer = vLineaArchivo[1:11]
                v_1_monto = v_0_monto
                v_1_numero_liquidacion = vLineaArchivo[57:65]
                
                
                dfTransacciones = dfTransacciones.append({  
                                            'v_0_rut_beneficiario': v_0_rut_beneficiario,
                                            'v_0_dvr_beneficiario': v_0_dvr_beneficiario,
                                            'v_0_nombre_beneficiario': v_0_nombre_beneficiario,
                                            'v_0_correo': v_0_correo,
                                            'v_0_codigo_banco': v_0_codigo_banco,
                                            'v_0_numero_cuenta': v_0_numero_cuenta, 
                                            'v_0_monto': v_0_monto,
                                            'v_0_fecha_pago': v_0_fecha_pago,
                                            'v_1_numero_transfer': v_1_numero_transfer,
                                            'v_1_monto': v_1_monto,
                                            'v_1_numero_liquidacion': v_1_numero_liquidacion, 
                                            'v_0_original': v_0_original,
                                            'v_1_original': v_1_original,
                                            'v_llave': v_0_llave,
                                            'v_forma_pago': ''
                                        }, ignore_index=True)
                
  
    # Aca vamos a guardar el resultado, del dataframe con las transacciones con su forma de pago, y agrupadas las que sean necesarias.
    dfResultado = pPD.DataFrame()
    dfAgrupado = pPD.DataFrame()


    # Agrupar todas las transacciones con la misma llave
    for key, group in dfTransacciones.groupby(['v_llave']):
        
        # Aca guardo el monto agrupado de la/las transacciones
        suma = group['v_1_monto'].sum()
        
        # Tomamos la primer fila por las dudas que solo sea una, y de aca tomamos los datos.
        primer_fila = group.iloc[0, :]
        
        if pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'SI':
            
            transaccion = {
                'v_1_id': primer_fila['v_1_id'],
                'v_1_rut': primer_fila['v_1_rut'],
                'v_1_razon_social_o_nombres_y_apellidos': primer_fila['v_1_razon_social_o_nombres_y_apellidos'],
                'v_1_email': primer_fila['v_1_email'],
                'v_1_banco':primer_fila['v_1_banco'],
                'v_1_forma_de_pago': primer_fila['v_1_forma_de_pago'],
                'v_1_numero_de_cuenta': primer_fila['v_1_numero_de_cuenta'], 
                'v_1_sector_fin': primer_fila['v_1_sector_fin'],
                'v_1_monto': suma,
                'v_2_id': primer_fila['v_2_id'],
                'v_2_fecha_doc': primer_fila['v_2_fecha_doc'],
                'v_2_monto_doc': primer_fila['v_2_monto_doc'], 
                'v_2_numero_doc': primer_fila['v_2_numero_doc'],
                'v_2_tipo_doc': primer_fila['v_2_tipo_doc'],
                'v_forma_pago': '',
                'v_llave': key, 
            }
            
        
        else:
            
            transaccion = {
                'v_0_rut_beneficiario': primer_fila['v_0_rut_beneficiario'],
                'v_0_dvr_beneficiario': primer_fila['v_0_dvr_beneficiario'],
                'v_0_nombre_beneficiario': primer_fila['v_0_nombre_beneficiario'],
                'v_0_correo': primer_fila['v_0_correo'],
                'v_0_codigo_banco':primer_fila['v_0_codigo_banco'],
                'v_0_numero_cuenta': primer_fila['v_0_numero_cuenta'],
                'v_0_monto': suma, 
                'v_0_fecha_pago': primer_fila['v_0_fecha_pago'],
                'v_1_numero_transfer':  primer_fila['v_1_numero_transfer'],
                'v_1_monto': suma,
                'v_1_numero_liquidacion': primer_fila['v_1_numero_liquidacion'],
                'v_0_original': primer_fila['v_0_original'], 
                'v_1_original': primer_fila['v_1_original'],
                'v_llave': key, 
                'v_forma_pago': ''
            }
       
        dfAgrupado = dfAgrupado.append(transaccion, ignore_index=True)


    # Ordenar de menor a mayor el dataframe auxiliar
    dfAgrupado = dfAgrupado.sort_values(by='v_1_monto')
    
    # Inicializar variable monto_acumulado
    monto_acumulado = 0

    # Recorrer dataframe agrupado, asi comparamos el monto disponible a pagar con el monto agrupado.
    for index, row in dfAgrupado.iterrows():
        monto_acumulado += int(row['v_1_monto'])
        if (monto_acumulado <= pMONTO_MAXIMO_DISPONIBLE_BCO_BCI):
            transaccion = dfTransacciones[dfTransacciones['v_llave'] == row['v_llave']]
            transaccion.loc[:, 'v_forma_pago'] = "Nomina"
         
        elif (row[codigo_banco] == 16 or row[codigo_banco] == 37): 
            transaccion = row
            transaccion['v_forma_pago'] = 'TEF'
            
        elif (int(row['v_1_monto']) < 14000000):
            transaccion = row
            transaccion['v_forma_pago'] = 'TEF'
        else:
            transaccion = row
            transaccion['v_forma_pago'] = 'LBTR'
       
        dfResultado = dfResultado.append(transaccion, ignore_index=True)
    
    
    dfResultado = dfResultado.sort_values(by='v_1_monto')
    

    return dfResultado



# Funcion que genera el archivo NOMINA
def fGENERA_ARCHIVO_NOMINA(pPD, dfOUT_TRANSFER_BANCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, BUCKET, S3, FILENAME_NOMINA_ESTADO, FILENAME_NOMINA_BCI, psycopg2, datetime, vFechaProceso):
    
    cod_transaccion = FILENAME_NOMINA_BCI.split("_")[2]
     
    
    # Se inicializa el dataframe vacio, se va a ir llenando con las transacciones.
    dfNomina = pPD.DataFrame()
    
    
    if pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'SI':
       
        print("Generando el archivo Nomina para las transacciones de Banco Estado")
     
        # Obtenemos el dataframe filtrado solo con las transacciones que van a forma de pago Nomina.
        df = dfOUT_TRANSFER_BANCO_BCI[dfOUT_TRANSFER_BANCO_BCI['v_forma_pago']=="Nomina"]
        
        if (len(df) > 0):
            
            for index, row in df.iterrows():
                
                fecha = datetime.strptime(str(row["v_2_fecha_doc"]), "%d%m%Y")
                fecha_nuevo_formato = fecha.strftime("%Y%m%d")
                
                linea_1 = "0" + "09954690060" + str(row["v_1_rut"]).zfill(9) + str(row["v_1_razon_social_o_nombres_y_apellidos"]).ljust(50) + "E" + "0000000000" + str(row["v_1_email"]).ljust(40) + "A" + str(int(row["v_1_banco"])).zfill(5) + str(int(row["v_1_numero_de_cuenta"])).zfill(25) + str(int(row["v_1_monto"])).zfill(15) + fecha_nuevo_formato + "02" + "PAGO DE TRANSACCIONES ISWITCH FIJA".ljust(34)
                linea_2 = "1" + str(cod_transaccion).zfill(10) + "LIQUIDACION".ljust(20) + str(int(row["v_1_monto"])).zfill(15) + "LIQUIDACION " + str(row["v_2_numero_doc"])
                dfNomina = dfNomina.append({'linea_nomina': linea_1}, ignore_index=True)
                dfNomina = dfNomina.append({'linea_nomina': linea_2}, ignore_index=True)
            
            filename = FILENAME_NOMINA_ESTADO
            rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{FILENAME_NOMINA_ESTADO}"
        
        
    if pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == 'NO':
        
        print("Generando el archivo Nomina para las transacciones de Banco Bci")
      
        df = dfOUT_TRANSFER_BANCO_BCI[dfOUT_TRANSFER_BANCO_BCI['v_forma_pago']=="Nomina"]
        
        if (len(df) > 0):
            
            for index, row in df.iterrows():
                dfNomina = dfNomina.append({'linea_nomina': df.loc[index]['v_0_original']}, ignore_index=True)
                dfNomina = dfNomina.append({'linea_nomina': df.loc[index]['v_1_original']}, ignore_index=True)
            
            filename = FILENAME_NOMINA_BCI
            rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{FILENAME_NOMINA_BCI}"
            
        

    
    if (len(dfNomina) > 0):
        
        dfNomina.to_csv(rutaTmpOutput, index=False, header=False)
        putFileToS3(BUCKET, S3, filename, psycopg2, vFechaProceso)
        
    else:
        
        print("No se genero un archivo Nomina porque no existen registros para esta forma de pago.")
        
    



# Funcion que genera el archivo TEF
def fGENERA_ARCHIVO_TEF(pPD, dfOUT_TRANSFER_BANCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, BUCKET, S3, FILENAME_TEF, psycopg2, math, columnas_formato_bci, columnas_formato_santander, vFechaProceso):
    
    print("Generando el/los archivo/s TEF para las transacciones.")

    
    df = dfOUT_TRANSFER_BANCO_BCI[dfOUT_TRANSFER_BANCO_BCI['v_forma_pago']=="TEF"]

    transacciones = []


    if (len(df) > 0):
        
        if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == "SI"):        
            
            dfTef = pPD.DataFrame(columns = columnas_formato_bci)
            
            for index, row in df.iterrows():
                
                monto = int(row["v_1_monto"])
                
                if monto > 7000000:
                    
                    for i in range(2):
                        
                        if (i == 0):
                            monto_transaccion = 7000000
                        else:
                            monto_transaccion = monto - 7000000
                        
                        transacciones.append([
                            '',
                            row["v_1_numero_de_cuenta"],
                            row["v_1_banco"],
                            row["v_1_rut"][:-1],
                            row["v_1_rut"][-1],
                            row["v_1_razon_social_o_nombres_y_apellidos"],
                            monto_transaccion,
                            '',
                            '',
                            'OTR',
                            'PAGO_TARJETAS_KLAP',
                            row["v_1_email"],
                            row["v_1_razon_social_o_nombres_y_apellidos"]
                        ])
               
                else:
                    transacciones.append([
                        '',
                        row["v_1_numero_de_cuenta"],
                        row["v_1_banco"],
                        row["v_1_rut"][:-1],
                        row["v_1_rut"][-1],
                        row["v_1_razon_social_o_nombres_y_apellidos"],
                        row["v_1_monto"],
                        '',
                        '',
                        'OTR',
                        'PAGO_TARJETAS_KLAP',
                        row["v_1_email"],
                        row["v_1_razon_social_o_nombres_y_apellidos"]
                    ])
           
            dfTef = dfTef.append(pPD.DataFrame(transacciones, columns=columnas_formato_bci), ignore_index=True)
            
            namefile = f'{FILENAME_TEF}_Formato_Bci'
            guardarArchivoTEFAlS3(dfTef, namefile, 499, BUCKET, S3, psycopg2, math, vFechaProceso)
        
        else:
            
            dfTefBciCodBci = df[(df['v_0_codigo_banco'] == 16)]
            dfTefBciCodSantander =  df[(df['v_0_codigo_banco'] == 37)]
            dfTefBciCodDistinto = df[((df['v_0_codigo_banco'] != 16) & (df['v_0_codigo_banco'] != 37))] #va a ser formato BCI
            
            # Aca entra cuando NO esta vacio
            if (len(dfTefBciCodBci) > 0):
                print("TEF Formato Bci")
                
                dfTefBciCodBciSalida = pPD.DataFrame()
                
                transacciones = []
              
                for index, row in dfTefBciCodBci.iterrows():
                    transacciones.append([
                        '',
                        row["v_0_numero_cuenta"],
                        row["v_0_codigo_banco"],
                        str(row["v_0_rut_beneficiario"])[:-1],
                        str(row["v_0_rut_beneficiario"])[-1],
                        row["v_0_nombre_beneficiario"],
                        row["v_0_monto"],
                        '',
                        '',
                        'OTR',
                        'PAGO_TARJETAS_KLAP',
                        row["v_0_correo"],
                        row["v_0_nombre_beneficiario"]
                    ])
                
                dfTefBciCodBciSalida = dfTefBciCodBciSalida.append(pPD.DataFrame(transacciones, columns = columnas_formato_bci), ignore_index=True)
                
                namefile = f'{FILENAME_TEF}_Codigo_Bci_Formato_Bci'
                guardarArchivoTEFAlS3(dfTefBciCodBciSalida, namefile, 499, BUCKET, S3, psycopg2, math, vFechaProceso)
           
            if (len(dfTefBciCodSantander) > 0):
                print("TEF Formato Santander")
                
                dfTefBciCodSantanderSalida = pPD.DataFrame()
                
                transacciones = []
                
                for index, row in dfTefBciCodSantander.iterrows():
                    transacciones.append([
                        '',
                        'CLP',
                        row["v_0_numero_cuenta"],
                        'CLP',
                        row["v_0_codigo_banco"],
                        row["v_0_rut_beneficiario"],
                        row["v_0_nombre_beneficiario"],
                        row["v_0_monto"],
                        'PAGO VENTAS ADQ',
                        row["v_0_correo"],
                        'PAGO VENTAS ADQ',
                        'PAGO VENTAS ADQ',
                        'PAGO VENTAS ADQ'
                    ])
                
                
                dfTefBciCodSantanderSalida = dfTefBciCodSantanderSalida.append(pPD.DataFrame(transacciones, columns = columnas_formato_santander), ignore_index=True)
                
                namefile = f'{FILENAME_TEF}_Codigo_Santander_Formato_Santander.xlsx'
                guardarArchivoTEFAlS3(dfTefBciCodSantanderSalida, namefile, 300, BUCKET, S3, psycopg2, math, vFechaProceso)
            
            if (len(dfTefBciCodDistinto) > 0):
                print("TEF Formato Bci (tsx codigo distinto a bci y santander)")
                
                dfTefBciCodDistintoSalida = pPD.DataFrame()
                
                transacciones = []   
                
                for index, row in dfTefBciCodDistinto.iterrows():
                    
                    monto = int(row["v_0_monto"])
                    
                    if monto > 7000000:
                        
                        for i in range(2):
                            
                            if (i == 0):
                                monto_transaccion = 7000000
                            else:
                                monto_transaccion = monto - 7000000
                            
                            transacciones.append([
                                '',
                                row["v_0_numero_cuenta"],
                                row["v_0_codigo_banco"],
                                str(row["v_0_rut_beneficiario"])[:-1],
                                str(row["v_0_rut_beneficiario"])[-1],
                                row["v_0_nombre_beneficiario"],
                                monto_transaccion,
                                '',
                                '',
                                'OTR',
                                'PAGO_TARJETAS_KLAP',
                                row["v_0_correo"],
                                row["v_0_nombre_beneficiario"]
                            ])
                    else:
                        transacciones.append([
                            '',
                            row["v_0_numero_cuenta"],
                            row["v_0_codigo_banco"],
                            str(row["v_0_rut_beneficiario"])[:-1],
                            str(row["v_0_rut_beneficiario"])[-1],
                            row["v_0_nombre_beneficiario"],
                            row["v_0_monto"],
                            '',
                            '',
                            'OTR',
                            'PAGO_TARJETAS_KLAP',
                            row["v_0_correo"],
                            row["v_0_nombre_beneficiario"]
                        ])
               
                dfTefBciCodDistintoSalida = dfTefBciCodDistintoSalida.append(pPD.DataFrame(transacciones, columns=columnas_formato_bci), ignore_index=True)
                
                namefile = f'{FILENAME_TEF}_Codigo_Distinto_Formato_Bci'
                guardarArchivoTEFAlS3(dfTefBciCodDistintoSalida, namefile, 499, BUCKET, S3, psycopg2, math, vFechaProceso)
                
    else:
        print("No se genero un archivo TEF porque no existen registros para esta forma de pago.")




# Funcion que sube un excel al bucket
def guardarArchivoTEFAlS3(dfTef, FILENAME_TEF, cantRows, bucket, s3, psycopg2, math, vFechaProceso):
    

    decimal = float(cantRows)

    # Partir el archivo en caso de que supere los 499 registros
    if len(dfTef) > cantRows:
        n = int(math.ceil(len(dfTef) / decimal))
        for i in range(n):
            start = i * cantRows
            end = (i+1) * cantRows
            df_tef_i = dfTef.iloc[start:end]
            nombre_indexado = f'{FILENAME_TEF}_N°{i+1}.xlsx'
            rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{nombre_indexado}"
            df_tef_i.to_excel(rutaTmpOutput, index=False)
            putFileToS3(bucket, s3, nombre_indexado, psycopg2, vFechaProceso)
        
    else:
        fileName = f'{FILENAME_TEF}.xlsx'
        rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{fileName}"
        dfTef.to_excel(rutaTmpOutput, index=False)
        putFileToS3(bucket, s3, fileName, psycopg2, vFechaProceso)




# Funcion que genera el archivo LBTR
def fGENERA_ARCHIVO_LBTR(pPD, dfOUT_TRANSFER_BANCO_BCI, pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI, BUCKET, S3, psycopg2, columnas_formato_chile, vFechaProceso):

    df = dfOUT_TRANSFER_BANCO_BCI
    

    # Los campos son distintos, los dataframes son distintos, la salida de archivos y la logica tambien. Por eso en este caso de LBTR, nos conviene hacer una pregunta general de SI o NO, ya que igual no se va a repetir codigo.
    # Aca entra cuando es el transfer de Banco Estado (se paga con BCI) pero es solo un LBTR con formato CHILE.
    if (pPAGAR_TRANSACCIONES_BCO_ESTADO_X_BCO_BCI == "SI"):
        
        print("Generando el/los archivo/s LBTR para las transacciones de Banco Estado")
        
        dfEstado = df[(df['v_forma_pago']=="LBTR")]
        
        dfLbtr = pPD.DataFrame()
        
        transacciones = []
        
        
        if (len(dfEstado) > 0):
        
            for index, row in dfEstado.iterrows():
                
                transacciones.append([
                    row["v_1_rut"][:-1],
                    row["v_1_rut"][-1],
                    row["v_1_razon_social_o_nombres_y_apellidos"],
                    row["v_1_email"],
                    row["v_1_banco"],
                    row["v_1_numero_de_cuenta"],
                    row["v_1_monto"]
                ])
            
            
            dfLbtr = dfLbtr.append(pPD.DataFrame(transacciones, columns = columnas_formato_chile), ignore_index=True)
            
            namefile = f'LBTR_Banco_Estado_{vFechaProceso}_Formato_CHILE.xlsx'
            
            guardarArchivoYAlS3(dfLbtr, namefile, BUCKET, S3, psycopg2, vFechaProceso)
            
        else:
            print("No hay transacciones con forma de pago LBTR en este caso.")
    
    # Sabemos que aca es el transfer de BCI. Entonces van a ser 3 archivos LBTR, uno por cada codigo de banco, osea por cada formato.
    else:
        print("Generando el/los archivo/s LBTR para las transacciones de Banco Bci")
        
        dfLbtrBciCodChile = df[(df['v_forma_pago']=="LBTR") & ((df['v_0_codigo_banco'] != 16) & (df['v_0_codigo_banco'] != 37))]
        
        
        if (len(dfLbtrBciCodChile) > 0):
            print("LBTR chile: ", dfLbtrBciCodChile)
            
            dfLbtrBciCodChileSalida = pPD.DataFrame()
            
            transacciones = []
            
            for index, row in dfLbtrBciCodChile.iterrows():
                
                transacciones.append([
                    str(row["v_0_rut_beneficiario"])[:-1],
                    str(row["v_0_rut_beneficiario"])[-1],
                    row["v_0_nombre_beneficiario"],
                    row["v_0_correo"],
                    row["v_0_codigo_banco"],
                    row["v_0_numero_cuenta"],
                    row["v_0_monto"]
                ])
            
            
            dfLbtrBciCodChileSalida = dfLbtrBciCodChileSalida.append(pPD.DataFrame(transacciones, columns = columnas_formato_chile), ignore_index=True)
            
            namefile = f'LBTR_Banco_Bci_{vFechaProceso}_Formato_CHILE.xlsx'
            
            guardarArchivoYAlS3(dfLbtrBciCodChileSalida, namefile, BUCKET, S3, psycopg2, vFechaProceso)
            
        else:
            print("No hay transacciones con forma de pago LBTR en este caso.")


# Funcion que sube un excel al bucket
def guardarArchivoYAlS3(df, fileName, bucket, s3, psycopg2, vFechaProceso):
    rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{fileName}"
    df.to_excel(rutaTmpOutput, index=False)
    putFileToS3(bucket, s3, fileName, psycopg2, vFechaProceso)



# Funcion que genera el archivo BANCO ESTADO  
def fGENERAR_ARCHIVO_BANCO_ESTADO(BUCKET, S3, filenameBancoEstadoInput, filenameBancoEstadoOutput, posicion, openpyxl, psycopg2, vFechaProceso):
    
    rutaTmpInput = f"/tmp/input-{filenameBancoEstadoInput}"
    rutaTmpOutput = f"/tmp/output-{vFechaProceso}-{filenameBancoEstadoOutput}"
    
    try:
        # toma el archivo original que subimos a la carpeta tmp y le eliminamos los registros hasta la posicion que indicamos.    
        posicionFormateada = posicion+3
        posicionInt = int(posicionFormateada)
        
        print("Filename: ", filenameBancoEstadoInput)
        print("Posicion: ", posicionInt)
        
        
        # Abrir archivo excel
        archivo = openpyxl.load_workbook(rutaTmpInput)
        hoja = archivo['DETALLE']
        
        hoja.delete_rows(posicionInt, hoja.max_row - posicionInt + 1)
        
        archivo.save(rutaTmpOutput)
        
        putFileToS3(BUCKET, S3, filenameBancoEstadoOutput, psycopg2, vFechaProceso)
        
        
    except (Exception, psycopg2.Error) as error :
        print("Error al dividir el archivo Banco Estado para subir solo las transacciones que alcanzaron con el monto disponible de Banco Estado.: ERROR = ", error)


 
# Funcion que sube archivo al S3.
def putFileToS3(bucket, s3, fileName, psycopg2, vFechaProceso):
    
    respuesta = {
        "estado": "",
        "rutaTmp": ""
    }
    
    rutaTmp = f"/tmp/output-{vFechaProceso}-{fileName}"
    ruta = f"manipulacion-transfer/output/{vFechaProceso}/{fileName}"
    
    try:
        print("Inicia subida de archivo a S3.")
        
        # Subimos el archivo al bucket
        s3.upload_file(rutaTmp, bucket, ruta)
        
        print(f"Archivo '{fileName}' subido a S3 correctamente.")
        
        respuesta["estado"] = "Correcto"
        respuesta["rutaTmp"] = rutaTmp
        
    except (Exception, psycopg2.Error) as error :
        print("No se pudo subir el archivo al bucket: ERROR = ", error)
        respuesta["estado"] = "Error"
    
    return respuesta