FROM alpine
LABEL maintainers="Kubernetes Authors"
LABEL description="CSI Driver registrar"

COPY ./bin/driver-kube-registrar driver-kube-registrar
ENTRYPOINT ["/driver-kube-registrar"]
