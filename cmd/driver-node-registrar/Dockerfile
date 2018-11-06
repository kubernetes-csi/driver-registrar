FROM alpine
LABEL maintainers="Kubernetes Authors"
LABEL description="CSI Driver Node Registrar"

COPY ./bin/driver-node-registrar driver-node-registrar
ENTRYPOINT ["/driver-node-registrar"]
