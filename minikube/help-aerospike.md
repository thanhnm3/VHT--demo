
### Tao aerospike 
kubectl apply -f aerospike-deployment.yaml

### Tao pod aerospike tool 
kubectl apply -f aerospike-tools.yaml

### Ket noi aerospike den aerospike tool 
kubectl exec -it aerospike-tools -n aerospike -- /bin/sh
### truy cap vao asadm 
asadm -h aerospike:3000
### truy cap vao aql de kiem thu
aql -h aerospike:3000