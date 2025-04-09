# Kafka and Prometheus Setup

This project provides a basic setup for deploying Kafka and Zookeeper on Kubernetes, along with Prometheus for metrics monitoring. The setup includes the necessary Kubernetes manifests for each component.

## Project Structure

```
kafka-prometheus-setup
├── manifests
│   ├── kafka.yaml          # Kubernetes manifest for Kafka deployment
│   ├── zookeeper.yaml      # Kubernetes manifest for Zookeeper deployment
│   ├── prometheus.yaml     # Kubernetes manifest for Prometheus deployment
│   └── service-monitor.yaml # ServiceMonitor for Prometheus to scrape metrics
└── README.md               # Documentation for the project
```

## Prerequisites

- Kubernetes cluster
- kubectl command-line tool
- Helm (optional, if using Helm charts)

## Deployment Instructions

1. **Deploy Zookeeper**:
   Apply the Zookeeper manifest to create a Zookeeper instance.
   ```
   kubectl apply -f manifests/zookeeper.yaml
   ```

2. **Deploy Kafka**:
   After Zookeeper is running, apply the Kafka manifest to create a Kafka broker.
   ```
   kubectl apply -f manifests/kafka.yaml
   ```

3. **Deploy Prometheus**:
   Apply the Prometheus manifest to set up Prometheus for monitoring.
   ```
   kubectl apply -f manifests/prometheus.yaml
   ```

4. **Set Up Service Monitor**:
   Apply the ServiceMonitor manifest to configure Prometheus to scrape metrics from Kafka and Zookeeper.
   ```
   kubectl apply -f manifests/service-monitor.yaml
   ```

## Accessing Metrics

Once the deployments are up and running, you can access the Prometheus UI to view the metrics collected from Kafka and Zookeeper.

1. **Port Forward Prometheus**:
   Forward the Prometheus service to your local machine.
   ```
   kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090
   ```

2. **Open Prometheus UI**:
   Navigate to `http://localhost:9090` in your web browser to access the Prometheus dashboard.

## Notes

- Ensure that your Kubernetes cluster has sufficient resources to run Kafka, Zookeeper, and Prometheus.
- Modify the manifests as necessary to fit your specific requirements, such as resource limits and replicas.

This setup provides a foundational structure for running Kafka with monitoring capabilities. Adjust configurations as needed for production environments.