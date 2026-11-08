# DFSha — Sistema de archivos distribuido (Proyecto 1, Fase II)

Prototipo grupal de la Opción 1 (cliente/servidor con servicio distribuido entre servidores).
Arquitectura, decisiones y resultados: [docs/INFORME_GRUPAL.md](docs/INFORME_GRUPAL.md).

Integrantes: Andrés Felipe Vélez Estrada (000551866) · Juan Sebastián Quintero Hernández (000492758)

## Componentes

| Componente | Código | Instancias en AWS | Puerto |
| --- | --- | --- | --- |
| Servicio de metadatos (MS), Raft | `dfsha/meta_server.py` | ms-1, ms-2, ms-3 | 7000 gRPC, 7001 Raft |
| Nodo de datos (DN) | `dfsha/data_server.py` | dn-1 … dn-4 | 8000 gRPC |
| Cliente (biblioteca + CLI `dfsha`) | `dfsha/client.py`, `dfsha/cli.py` | client | — |
| Contrato gRPC (IDL) | `proto/dfsha.proto` | — | — |

## Prueba local (una sola máquina, sin Docker)

Requiere Python 3.10+ y openssl.

```bash
sh scripts/gen_certs.sh certs          # CA interna + certificados mTLS
bash scripts/local_cluster.sh start    # 3 MS + 4 DN como procesos
export PYTHONPATH=$PWD DFSHA_CERTS=$PWD/certs DFSHA_BLOCK_SIZE=1048576 \
       DFSHA_META=127.0.0.1:7100,127.0.0.1:7200,127.0.0.1:7300
python3 -m dfsha.cli login andres      # contraseña: andres123
python3 -m dfsha.cli status
bash scripts/local_cluster.sh stop
```

## Despliegue en AWS (desde CloudShell)

```bash
sh scripts/gen_certs.sh certs
tar -czf dfsha.tar.gz Dockerfile requirements.txt proto dfsha scripts deploy tests certs
bash deploy/launch.sh            # bucket S3 privado, rol IAM, SG, zona DNS dfsha.internal y 8 EC2
bash deploy/launch.sh status     # instancias e IP privadas
bash deploy/redeploy.sh          # publica una nueva versión del código en todas las instancias
bash deploy/launch.sh destroy       # termina las instancias
bash deploy/launch.sh destroy-all   # además borra bucket, zona DNS, grupo de seguridad y rol IAM
```

En AWS Academy se usa el perfil del laboratorio: `PROFILE=LabInstanceProfile bash deploy/launch.sh`.
Las instancias no exponen puertos a Internet; se administran por SSM.

## Uso de la CLI (en la instancia `client`)

```bash
dfsha login andres
dfsha mkdir datos && dfsha put video.mp4 datos/video.mp4 && dfsha get datos/video.mp4 copia.mp4
dfsha ls datos && dfsha stat datos/video.mp4 && dfsha rm datos/video.mp4
dfsha cat notas.txt --offset 0 --len 20                 # open + read
dfsha write notas.txt --offset 5 --data "texto"         # open rw + lock + write + close
dfsha append notas.txt --data "más texto"               # open a + write + close
dfsha status                                            # líder Raft y estado de los DN
```

## Pruebas exigidas (enunciado 5.2)

```bash
bash /opt/dfsha/tests/run_tests.sh all     # o: carga | rf3 | concurrencia | fallos | consistencia | seguridad | rendimiento
```

Cada ejecución deja un `evidencia_<fecha>.log` y `rendimiento.csv` en `~/resultados`.
