
### Tao minikube
minikube start --memory=8192


=========================================================================================
Kiem tra cac name space :             
  kubectl get namespaces
=========================================================================================
Xem cac pod trong namespace: 
  kubectl get pods -n default








### Xem Log cua kafka trong k8s
kubectl logs deployment/strimzi-cluster-operator -n kafka -f

### Tao kafka custom resource voi kraft kafka - voi single node
kubectl apply -f https://strimzi.io/examples/latest/kafka/kraft/kafka-single-node.yaml -n kafka 

### Tao topic trong kafka ( Cu the la tao producer)

kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic


### Mo 1 side terminal ( Tao consumer) 
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning

### Xoa kafka 
kubectl -n kafka delete $(kubectl get strimzi -o name -n kafka)

### When you want to fully remove the Strimzi cluster operator and associated definitions, you can run:
kubectl -n kafka delete -f 'https://strimzi.io/install/latest?namespace=kafka'


### Xoa namespace Kafka 
kubectl delete namespace kafka

### ========================================================================================= 
### Kafka Topic ###

### TIm xem kafka-topics.sh nam o dau ( ma nay de thuc thi cac lenh o kafka)
kubectl exec -it my-cluster-dual-role-0 -n kafka -- find / -name kafka-topics.sh

  ==================== KET QUA ================== /opt/kafka/bin/kafka-topics.sh

### Kiem tra danh sach cac topic
kubectl exec -it my-cluster-dual-role-0 -n kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

### Hien thi chi tiet thong tin ve 1 pod trong namespace
kubectl describe pod my-cluster-dual-role-0 -n kafka


==================================================================================================================

### Xem cac Release Helm 
helm list -n default


### Cài đặt Prometheus và Grafana, cùng với một số công cụ khác như Alertmanager, Node Exporter, và các tài nguyên liên quan để triển khai một hệ thống giám sát toàn diện trên Kubernetes.
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack


==================================================================================================================


### Dang nhap vao grafana

  kubectl --namespace default get secrets prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 -d ; echo


 admin : prom-operator
=======================================================================================
### Kiem tra xem ten cua pod chua grafana o dau
 kubectl get pods -n default

### Foward prometheus ra cong 9090
kubectl --namespace default port-forward prometheus-prometheus-kube-prometheus-prometheus-0 9090:9090

### Forward grafana ra cong 3000
prometheus-grafana-6854b47bf4-8z9g6































==================================================================================================
### Cài đặt Strimzi Kafka Operator vào namespace mà bạn muốn sử dụng 
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace kafka --create-namespace




===================================================================================================
###  https://github.com/strimzi/strimzi-kafka-operator/tree/main/examples/metrics
Tai Lieu cuc ki cuc ki cuc ki quan trong 
### https://strimzi.io/docs/operators/latest/deploying.html
===================================================================================================