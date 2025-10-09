# Acceso concurrente a una base de datos compartida con procesos de lectura, escritura, backup y chequeo de consistencia
Se dispone de una base de datos compartida entre N procesos concurrentes. El sistema debe garantizar un acceso controlado a la base de datos, siguiendo los siguientes lineamientos:
## Operaciones de lectura
- Pueden ejecutarse de forma concurrente hasta un máximo de tres procesos lectores.
- Los lectores requieren un permiso de lectura compartido.
- Si existen escritores en espera, no se permite la entrada de nuevos lectores hasta que los escritores hayan finalizado.
## Operaciones de escritura
- La escritura requiere un permiso exclusivo sobre la base de datos.
- No puede coexistir con lecturas ni con otras escrituras en curso.
- Cuando hay un escritor en espera, este tiene prioridad sobre nuevos lectores.
## Proceso de backup
- Se ejecuta de manera periódica y posee prioridad sobre todos los demás procesos.
- El backup debe esperar a que finalicen las lecturas y escrituras en curso para iniciar.
- Una vez iniciado, bloquea el ingreso de nuevos procesos de lectura y escritura hasta su finalización.
## Proceso de chequeo de consistencia
- Puede ejecutarse en cualquier momento, siempre que no haya escrituras en curso.
- Compite únicamente con las operaciones de escritura, pudiendo coexistir con procesos de lectura activos.
- Durante el chequeo, si se detectan referencias inconsistentes, este proceso realiza una actualización del permiso de lectura a escritura, con el fin de eliminar la referencia inválida.
# Objetivo
Modelar e implementar la sincronización necesaria para asegurar la correcta concurrencia entre procesos lectores, escritores, el backup y el chequeo de consistencia, respetando las reglas de prioridad, exclusión y concurrencia detalladas.
