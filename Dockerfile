FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto ./proto
COPY dfsha ./dfsha
RUN python -m grpc_tools.protoc -I proto --python_out=dfsha --grpc_python_out=dfsha proto/dfsha.proto \
 && sed -i 's/^import dfsha_pb2/from dfsha import dfsha_pb2/' dfsha/dfsha_pb2_grpc.py

COPY scripts/dfsha /usr/local/bin/dfsha
RUN chmod +x /usr/local/bin/dfsha
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
