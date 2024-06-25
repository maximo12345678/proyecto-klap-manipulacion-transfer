Proceso que fue una solucion para un proceso que el cliente hacia manualmente. Basicamente en base a cada banco, habian
distintos formatos de archivos para estructurar las transacciones, cada banco tenia su estructura con ciertos datos, en distinta forma de mostrarse y ordenarse.
Este proceso iniciaba de una manera particular, el cliente tenia un servidor sftp conectado al S3, al crear una carpeta y cargar los archivos de input
(un excel por cada banco) estos caen al S3, el cual desde ahi gatilla el Lambda, desarrollado en python. 
El proceso comienza, se toma el archivo de input, se formatea y todo. Pero la complejidad estaba en que el archivo de output generado,
por ejemplo para banco SANTANDER, tenia que tener solo las transacciones que se alcancen a pagar (sumando los montos) con el monto total disponible
a pagar del banco Santander, y luego esas transacciones sobrantes las pagaria BCI entonces tendrian que ir en el archivo de formato BCI.
Se va generando un reporte de todo y finalmente se cargan todos los archivos en el S3 y se envia un correo indicando el fin del proceso.

