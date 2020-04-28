# Setup a development cluster

In order to be able to properly test in a clustered setup, the development cluster requires
certain tools to be installed in it.

> The following assumes that the cluster was already created and configured locally, e.g. there
> a valid Kubernetes context configuration on your local machine

To set up a development cluster, first ensure you are using the right Kubernetes context pointing to
the cluster you want to set up.

> Note that unless otherwise stated, you only need to set up a cluster for development once

## Requirements

The same requirements apply here as the general project (e.g. Helm v3.x.x).

It's a good idea to update your Helm charts before proceeding.

```sh
helm repo update
```

## Monitoring

To monitor your cluster, we will set up a the [Prometheus Operator helm chart](https://github.com/helm/charts/tree/master/stable/prometheus-operator),
which will install the following components the operator and Grafana.

To do so, run:

```sh
helm install metrics stable/prometheus-operator --atomic
```

If you want to set up an ingress for Grafana (e.g. public endpoint), you can run

```sh
kubectl apply -f grafana-load-balancer.yml
```

You can then obtain your Grafana URL by checking the ingress' external IP, e.g.

```sh
kubectl get svc metrics-grafana-loadbalancer -o "custom-columns=ip:status.loadBalancer.ingress[0].ip"
```

If you don't need an ingress, you can simply proxy the Grafana port so it's available locally:

```sh
kubectl port-forward svc/metrics-grafana-loadbalancer :80
```

## Istio

In order to enable Istio, make sure you completed the [prerequisites](https://istio.io/docs/setup/install/istioctl/#prerequisites).

> If you're not sure if Istio is already installed on your cluster, you can simply verify the installation first

To install Istio on your cluster, run:

```sh
kubectl apply -f istio-manifest.yml
```

Then once finished, verify that the installation is correct:

```sh
istioctl verify-install -f istio-manifest.yml
```

> The manifest was generated using `istioctl manifest generate --set addonComponents.prometheus.enabled=false`

## Linkerd

A lightweight alternative to Istio, you can also set up linkerd. In order to enable linkerd, it's a good idea to install the [linkerd CLI](https://linkerd.io/2/getting-started/#step-1-install-the-cli).

> If you're not sure if linkerd is already installed on your cluster, you can simply verify the installation first

To install Istio on your cluster, run:

```sh
kubectl apply -f linkerd-manifest.yml -f linkerd-service-monitor.yml
```

Then once finished, verify that the installation is correct:

```sh
linkerd check
```
