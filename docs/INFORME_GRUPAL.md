# DFSha · Proyecto 1 — Fase II: implementación grupal

Andrés Felipe Vélez Estrada (000551866) · Juan Sebastián Quintero Hernández (000492758)
Arquitecturas de Nube y Sistemas Distribuidos 2026-2 · Opción 1 (cliente/servidor S2S)

## 1. Matriz comparativa de las propuestas individuales

Las dos propuestas coincidían en lo esencial: plano de control con 3 servidores de metadatos replicados por Raft, nodos de datos con R = 3, visibilidad de cierre a apertura y gRPC. Las diferencias estaban en el detalle de la concurrencia y la seguridad.

| Aspecto | Propuesta de Andrés | Propuesta de Juan Sebastián | Decisión del equipo |
| --- | --- | --- | --- |
| Plano de control | Control Cluster de 3–5 nodos con Raft | 3 MS con Raft (1 líder + 2 seguidores) | 3 MS con Raft (ambas) |
| Tamaño de bloque | 128 MB (estándar HDFS) | 64 MiB (8 MiB en pruebas) | 8 MiB en el prototipo, configurable (`DFSHA_BLOCK_SIZE`) |
| Replicación | R = 3, quórum 2 de 3, primario reenvía | R = 3, W = 2, pipeline DN1→DN2→DN3 | R = 3, W = 2, pipeline (Juan) |
| Modelo de datos | Copy-on-write sobre la lista de bloques | Bloques inmutables, versión atómica | Bloques inmutables + versión atómica (ambas) |
| Granularidad del lock | Lease exclusivo de archivo completo | Lease por rango de bloques | Lease de archivo completo (Andrés) |
| Control de concurrencia | Pesimista (lease) | Pesimista (lease + token de cerco) + optimista (`base_version`) | Las dos capas de Juan sobre el lease de Andrés |
| Autorización en los DN | No especificada | Tokens de bloque firmados por el MS | Tokens de bloque HMAC (Juan) |
| Cifrado en reposo | AES-256-GCM con envelope encryption | AES-256-GCM, DEK por bloque envuelta con KEK (Vault) | DEK por bloque + KEK por variable de entorno (sin Vault) |
| Canal | mTLS en los tres enlaces | TLS 1.3 cliente, mTLS interno | mTLS en todos los enlaces (cliente con certificado + JWT) |
| Descubrimiento | Endpoint lógico (DNS o balanceador) | DNS privado `meta.dfsha.internal` | Route 53 privado `dfsha.internal` (Juan) |
| Detección de fallos | Heartbeat 5 s, DOWN a 60 s | Heartbeat 3 s, MUERTO a 30 s | Heartbeat 3 s, SUSPECT 8 s, DEAD 15 s (más corto para la demo) |
| Monitoreo | No especificado | Prometheus + Grafana | Comando `dfsha status` y logs (Prometheus queda fuera) |

## 2. Registro de decisiones arquitectónicas (ADR)

| ID | Decisión | Motivo | Cambia respecto a |
| --- | --- | --- | --- |
| ADR-1 | Raft con la biblioteca pysyncobj en lugar de una implementación propia | Implementar Raft desde cero no cabía en el tiempo; pysyncobj da elección de líder, replicación del log y cifrado del tráfico Raft con contraseña | Ambas (no especificaban implementación) |
| ADR-2 | Persistencia del MS con instantáneas (`fullDumpFile`) cada ~20 s, sin journal en disco | En EC2 el journal mmap de pysyncobj falló con `IndexError: mmap slice assignment` y tumbó el nodo. Mientras haya mayoría viva la durabilidad la da la replicación 2 de 3; la instantánea cubre el reinicio simultáneo de los 3 MS | Ambas (log en disco) |
| ADR-3 | Lease de archivo completo + `base_version` | El lease por rango obliga a fusionar confirmaciones parciales; con archivo completo la semántica es más simple y la capa optimista sigue protegiendo a quien no toma lock | Juan |
| ADR-4 | Bloques de 8 MiB en el prototipo | Las t3.micro tienen 1 GB de RAM y el cliente arma cada bloque en memoria; con 8 MiB un archivo de 100 MB se reparte en 13 bloques y se ve el particionamiento | Ambas (64/128 MB) |
| ADR-5 | mTLS también para el cliente | Un solo tipo de servidor TLS por nodo simplifica el despliegue; el usuario se autentica además con JWT | Juan (TLS simple al cliente) |
| ADR-6 | KEK en variable de entorno (S3 privado) en lugar de Vault/KMS | Evita un nodo extra; la rotación de la KEK queda como limitación | Ambas |
| ADR-7 | Cuenta personal de AWS en lugar de AWS Academy | La sesión de Academy no estaba disponible; el despliegue es el mismo con `PROFILE=LabInstanceProfile` | Enunciado 5.1. **Actualización 8-nov:** el despliegue se repitió y se validó en AWS Academy Learner Lab (ver sección 7) |
| ADR-8 | Journal Raft en disco (corrige ADR-2) | La falla `mmap slice assignment` era un error de pysyncobj 0.3.12: `ResizableFile.write` agranda el archivo una sola vez (×2) y una entrada mayor que eso no cabe. Se corrige ese método al importar y se activa `journalFile`; un commit confirmado sobrevive ahora al reinicio abrupto de los 3 MS | ADR-2 |
| ADR-9 | Sesiones de carga verificadas y recibos de almacenamiento | `AllocateBlocks` liga cada `upload_id` al usuario, la ruta y los `block_id` asignados; cada DN firma un recibo HMAC (`block_id`, dirección, SHA-256, tamaño) y el MS solo cuenta copias con recibo válido en DN distintos. Un bloque reutilizado debe pertenecer a la versión actual del mismo archivo | Corrige commits fabricados y referencias a bloques ajenos |
| ADR-10 | Bloques realmente inmutables en el DN | Repetir los mismos bytes es idempotente; bytes distintos bajo un `block_id` existente se rechazan (`BLOCK_EXISTS`); `fsync` del directorio tras el `rename` | Sobrescritura posible con un token W vigente |
| ADR-11 | Instantánea de lectura protegida 10 min | `Stat`/`open` fija los bloques de esa versión durante `DFSHA_PIN_TTL` (600 s, igual que la vida del token de lectura); el recolector no los borra mientras estén fijados | La lectura repetible fallaba tras 60 s de GC |
| ADR-12 | Autorización por rol del certificado y secretos mínimos en el cliente | `Heartbeat` exige certificado de nodo (CN `dfsha-node`), `Status` exige JWT para clientes; la instancia cliente solo recibe `ca.crt`, `client.crt/.key`, `DFSHA_META` y `DFSHA_BLOCK_SIZE`; `ca.key` no se distribuye | Un cliente podía registrar un DN falso y tenía el secreto JWT |

## 3. Arquitectura implementada

- **Cliente** (`dfsha/client.py`, `dfsha/cli.py`): solo conoce `meta.dfsha.internal:7000`. Resuelve las tres IP de los MS, prueba cualquiera y sigue la pista `NOT_LEADER <líder>`. Parte los archivos en bloques, los sube en paralelo (4 a la vez por defecto) y los descarga de cualquier réplica viva, verificando el SHA-256 de cada bloque y del archivo completo.
- **MS** (`dfsha/meta_server.py`): estado replicado por Raft con el espacio de nombres, las versiones de cada archivo, la ubicación de las réplicas y los leases con token de cerco. Solo el líder atiende. Un hilo del líder (gestor de réplicas) marca DN caídos, ordena re-replicar los bloques que quedaron con menos de 3 copias y borra de forma diferida los bloques sin referencias.
- **DN** (`dfsha/data_server.py`): recibe un bloque por streaming, verifica el token y el hash, lo cifra con AES-256-GCM, lo escribe en `tmp/` con `fsync` y lo renombra de forma atómica a `blocks/`. Luego lo reenvía al siguiente DN del pipeline. Cada 3 s envía heartbeat con su lista de bloques y ejecuta las órdenes del MS.

Despliegue en AWS (us-east-1, VPC por defecto): 8 instancias t3.micro con Amazon Linux 2023 y Docker (ms-1..3, dn-1..4, client), EBS gp3 cifrado, grupo de seguridad que solo permite tráfico entre miembros (sin puertos abiertos a Internet), zona privada Route 53 `dfsha.internal`, artefactos y secretos en un bucket S3 privado, rol IAM propio y administración por SSM.

## 4. Semántica declarada de RF3

- `open(path, mode)`: `r`, `w` (crear/reemplazar), `a` (anexar), `rw` (modificación parcial). Fija la versión vigente como instantánea de lectura; sus bloques se conservan al menos 10 min (`DFSHA_PIN_TTL`) aunque otro cliente publique una versión nueva.
- `read(offset, n)`: lee de la instantánea; nunca ve escrituras sin confirmar.
- `write(offset, data)`: admite creación, reemplazo, anexado y modificación parcial. Cada bloque tocado se descarga, se modifica y se sube como bloque nuevo (copia en escritura). Escribir más allá del final rellena el hueco con ceros.
- `lock()`: lease EXCLUSIVE de archivo completo (30 s) con token de cerco creciente.
- `close()` / `commit()`: confirma la nueva versión en una sola entrada Raft. Si otro tiene el lease, responde `LOCKED`; si la versión cambió desde el `open`, responde `VERSION_CONFLICT`.

Garantía comprobable: una escritura está confirmada cuando el MS responde OK al `Commit`; en ese momento cada bloque nuevo está en al menos 2 DN distintos que firmaron un recibo de almacenamiento verificado por el MS, la versión está escrita en el journal Raft de la mayoría de los MS (sobrevive aunque los 3 MS se reinicien a la vez), y todo `open`/`stat` posterior la ve.

## 5. Resultados de las pruebas (EC2, 8 de noviembre de 2026)

La batería completa (`run_tests.sh all`) se ejecutó dos veces: antes y después de las correcciones de la revisión final. En ambas corridas pasaron las 7 pruebas.

| Prueba | Resultado observado |
| --- | --- |
| Carga y recuperación | Archivo de 100 MB en 13 bloques de 8 MiB, cada uno en 3 DN distintos. SHA-256 original y recuperado idénticos (`85ce7d1d…7b1aa`). put 2,11 s, get 1,10 s |
| Acceso RF3 | `read(0,20)`, `write(offset 5)` con lock (v2), `append` (v3); el contenido final refleja las tres operaciones |
| Concurrencia | Cliente A con lock retiene 8 s; cliente B recibe `LOCKED`. Sin locks, B confirma sobre una versión vieja y recibe `VERSION_CONFLICT {current_version: 3}` |
| Replicación y fallos | Con dn-2 detenido, el get de 100 MB lee de otras réplicas y el hash coincide. dn-2 pasa a DEAD y el gestor re-replica (bloques sub-replicados: 0). Al detener el MS líder, otro MS es elegido y los metadatos vuelven en 0,5–1,0 s (dos corridas). Justo después de la elección el nuevo líder reporta bloques sub-replicados mientras dn-2 sigue caído; al restaurar los nodos el clúster vuelve a 0 bloques sub-replicados |
| Consistencia | Con dn-3 desconectado, el put de 24 MB salta ese DN y confirma con las réplicas vivas; los lectores leen la versión completa. Al volver dn-3 se reincorpora y el clúster queda con 0 bloques sub-replicados |
| Seguridad | juan recibe `NOT_OWNER` al leer `/users/andres/...`; contraseña errónea da `BAD_CREDENTIALS`. Una conexión TLS sin certificado de cliente y otra en texto plano son rechazadas (`UNAVAILABLE`). Una captura con tcpdump en dn-1 durante 6 cargas registró 34 553 bytes en el puerto 8000 y 0 líneas con el texto del archivo (canal cifrado). El texto plano tampoco aparece en los bloques en disco (AES-256-GCM) |
| Persistencia del plano de control | Con los 3 MS reiniciados a la vez (`docker restart` simultáneo), el clúster eligió líder (ms-1) y conservó los 9 archivos y sus versiones. **Corrección (8-nov):** esa prueba no cubría el último commit; con `docker kill` justo después de confirmar, la versión original perdía el archivo (solo había instantáneas cada ~20 s). Con ADR-8 el archivo sobrevive (sección 7) |
| Rendimiento | Ver tabla siguiente |

Throughput promedio (3 repeticiones, MB/s; el tiempo incluye ~0,5 s de arranque del contenedor de la CLI):

| Tamaño | put, 1 bloque a la vez | put, 4 en paralelo | get, 1 a la vez | get, 4 en paralelo |
| --- | --- | --- | --- | --- |
| 16 MB | 12,0 | 16,1 | 20,3 | 22,0 |
| 64 MB | 19,8 | 34,5 | 40,0 | 51,7 |
| 128 MB | 20,9 | 41,7 | 48,3 | 65,0 |

Con 4 bloques en paralelo el put de 128 MB duplica el throughput (20,9 → 41,7 MB/s): el particionamiento reparte la escritura entre varios DN. El get es más rápido que el put porque no paga la cadena de réplicas. La latencia media de `stat` fue 0,51 s, casi toda arranque del contenedor de la CLI. Datos crudos en [resultados/rendimiento.csv](resultados/rendimiento.csv).

## 6. Limitaciones

- El cliente carga cada archivo en memoria antes de partirlo; con 1 GB de RAM el tamaño práctico es de unos cientos de MB.
- El MS guarda todo el espacio de nombres en memoria (journal + instantáneas en disco); no hay federación. El journal usa mmap: sobrevive a la caída de procesos, pero ante un corte de energía simultáneo de los 3 MS podrían perderse las últimas entradas no volcadas por el sistema operativo.
- Las sesiones de carga y las instantáneas fijadas viven en memoria del líder: si el líder cambia durante una carga, el `Commit` responde `UNKNOWN_UPLOAD` y el cliente debe repetir la carga.
- La KEK vive en una variable de entorno y no rota; en producción iría en KMS.
- El certificado de cliente es compartido; la identidad del usuario la da el JWT. En AWS Academy todas las instancias usan `LabInstanceProfile` (no se pueden crear roles por función), así que una instancia comprometida podría leer `cluster.env` del bucket S3; con cuenta propia, el rol del cliente no debería tener acceso a ese objeto.
- Las versiones antiguas no se conservan: sus bloques se recolectan 60 s después de dejar de estar referenciados (o 10 min después del último `open` que las fijó).
- Todas las instancias están en una sola zona de disponibilidad (subred por defecto): se tolera la caída de nodos, no la de la zona.
- Un DN que regresa tras una caída conserva sus copias, por lo que algunos bloques quedan con 4 réplicas; no se eliminan las copias sobrantes.

## 7. Validación en AWS Academy y correcciones (8 de noviembre de 2026)

Se replicó la infraestructura en AWS Academy Learner Lab (cuenta del laboratorio, us-east-1, `PROFILE=LabInstanceProfile`) con los scripts de `deploy/`: 8 instancias t3.micro (ms-1..3, dn-1..4, client), EBS gp3 cifrado en las 8, grupo de seguridad `dfsha-sg` cuya única regla de entrada es el propio grupo, zona privada Route 53 `dfsha.internal` y bucket S3 privado (bloqueo de acceso público activado).

**Código original.** La batería `run_tests.sh all` reprodujo los resultados de la sección 5 (hash idéntico en 100 MB / 13 bloques, LOCKED y VERSION_CONFLICT, NOT_OWNER, canal y disco sin texto plano, lectura con un DN caído, nueva elección en 0,49 s). Una auditoría adicional (`tests/auditoria_seguridad.py`) confirmó en EC2: commit con sesión inventada aceptado, lectura de un bloque de otro usuario, sobrescritura de un bloque "inmutable", `Status` sin JWT, registro de un DN falso con el certificado de cliente, `IndexError` al escribir más allá del final, instantánea perdida tras 80 s y pérdida del último commit al matar los 3 MS. Evidencias: `resultados/academy_2026-11-08/codigo_original_*.out`.

**Código corregido (ADR-8 a ADR-12).** Tras redesplegar con `deploy/redeploy.sh`, la batería ampliada (ahora con verificaciones PASA/FALLA, auditoría y durabilidad) dio 25 PASA y 2 FALLA; las dos fallas se debían a un archivo fabricado que había quedado en los metadatos durante la auditoría del código original (un bloque inexistente que nunca puede re-replicarse). Tras borrarlo, `consistencia` y `fallos` pasaron completas (9/9). Resúmenes en `resultados/academy_2026-11-08/resumen_*.txt`.

Rendimiento con el código corregido (3 repeticiones, MB/s, incluye ~0,5 s de arranque del contenedor CLI; datos en `resultados/academy_2026-11-08/rendimiento.csv`):

| Tamaño | put, 1 a la vez | put, 4 en paralelo | get, 1 a la vez | get, 4 en paralelo |
| --- | --- | --- | --- | --- |
| 16 MB | 12,7 | 15,5 | 20,2 | 22,1 |
| 64 MB | 20,7 | 34,4 | 41,0 | 48,3 |
| 256 MB | 20,2 | 34,6 | 37,0 | 50,2 |

Las correcciones no cambian el rendimiento de forma apreciable (la corrida con el código original en el mismo clúster dio 20,0 / 35,0 / 35,8 / 48,3 MB/s para 256 MB). `stat` promedió 0,53 s y los metadatos volvieron 1,18 s después de detener al líder.
