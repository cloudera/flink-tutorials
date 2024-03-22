# Flink on Kubernetes

## Table of contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Build](#build)
4. [Defining the job](#defining-the-job)
5. [Run the job](#run-the-job)
6. [Job management](#job-management)
    + [Accessing Flink dashboard](#accessing-flink-dashboard)
    + [Restart](#restart)
    + [Suspend/Start](#suspendstart)
    + [Savepoint](#savepoint)
7. [Ingress](#ingress)
8. [Sidecars](#sidecars)


## Overview

The purpose of this tutorial is to teach you how to deploy your Flink application on a Kubernetes cluster. We will use a fairly simple Flink job and the main focus will be using the Flink Kubernetes Operator.

## Requirements

You will need to have a Kubernetes cluster ready, this tutorial will assume that you have Minikube installed. To start Minikube use the following command with the amount of resources you wish to use:
```bash
minikube start --memory 16384 --cpus 8
```

This will update your current Kubernetes configuration and now you should be able to use Minikube as a Kubernetes server. To continue please install Flink Kubernetes Operator:
```bash
# You will need cert-manager for the Operators webhook component
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
kubectl wait -n cert-manager --for=condition=Available deployment --all

# Install CSA Operator via Helm
helm install csa-operator --namespace flink --set 'flink-kubernetes-operator.image.imagePullSecrets[0].name=[***SECRET NAME***]' oci://container.repository.cloudera.com/cloudera-helm/csa-operator/csa-operator --version 0.8.0-b10
```

## Build

Before you start the tutorial, check out the repository and build the artifacts. You will also need to create a Docker image that will contain the built Flink jar using the following commands:
```bash
git clone https://github.com/cloudera/flink-tutorials.git
cd flink-tutorials/flink-kubernetes-tutorial
mvn clean package

# You will need to use Minikube's Docker environment and build the image there. 
# This ensures that the image will be present in the Kubernetes cluster.
eval $(minikube -p minikube docker-env)
docker build -t flink-kubernetes-tutorial .
```

## Defining the job

Before we run the job, we will need to create a `FlinkDeployment` resource in Kubernetes. This resource type was installed on your Kubernetes cluster when you installed the Operator, and can be used to define a Flink deployment, e.g. Flink application jar, Docker image, resources, Flink configuration, etc.  

This is a basic example you can use to deploy this job, save this as `flink-deployment.yaml`.

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-kubernetes-tutorial
spec:
  image: flink-kubernetes-tutorial
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  serviceAccount: flink
  mode: native
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    args: ["--rowsPerSec", "10"]
    jarURI: local:///opt/flink/usrlib/flink-kubernetes-tutorial.jar
    parallelism: 4
    state: running
    upgradeMode: stateless
```

Most of these configurations are self-explanatory, but there are some things to note:
- `flinkVersion`: We need to define the Flink version packaged in our Docker image so the Operator can decide how to deploy our app and communicate with it.
- `serviceAccount`: This is the Kubernetes ServiceAccount of Flink with all the permissions required to create Flink applications inside the cluster, the default value is `flink`
- `mode`: 
  - `native`: default and recommended, this will use Flink's integration with Kubernetes to e.g. spin up new TaskManagers.
  - `standalone`: all resources will be created at start by the Operator, Flink itself will be unaware that it's running inside Kubernetes.
- `job.state`: can be `running` or `suspended`, useful for stopping the job without removing its `FlinkDeployment` resource.
- `job.upgradeMode`:
  - `stateless`: No state will be saved.
  - `last-state`: Uses Flink HA metadata to resume jobs.
  - `savepoint`: Uses Flink savepoints to cancel and resume jobs.

# Run the job

Now that we have a running Operator, built our image containing the Flink jar, we can finally deploy our job and start processing. Use the following command to add the `FlinkDeployment` we have just defined to the `flink` namespace:
```bash
kubectl -n flink apply -f flink-deployment.yaml
```

This new resource will be automatically seen by the Operator, and we should be able to see that our `FlinkDeployment` resource switches to `RUNNING` state:

```bash
$ kubectl -n flink get FlinkDeployment
NAME                        JOB STATUS   LIFECYCLE STATE
flink-kubernetes-tutorial   RUNNING      STABLE
```

This is done by the Operator setting `status.jobStatus.state` to `RUNNING` in the `flink-kubernetes-tutorial` resource.

# Job management

Now that you have a running job, it's time for operations. 

### Accessing Flink dashboard

When the job is running, Flink will automatically create a new Kubernetes `Service` to which we can connect using port-forwarding. For this example you can use the following command to connect to the Flink Dashboard or its REST API. This will listen on http://localhost:8081 forwarding all requests to the Flink service until cancelled or the service gets deleted.
```bash
kubectl -n flink port-forward service/flink-kubernetes-tutorial-rest 8081:8081
```

### Restart

If you do any changes to the `FlinkDeployment` that warrants a restart, the Operator will take care of that automatically. For example, you want to change the job arguments:

```bash
kubectl -n flink patch FlinkDeployment flink-kubernetes-tutorial --type=merge --patch='{"spec":{"job":{"args":["--rowsPerSec", "100"]}}}'
```

In case you want to restart the deployment without any changes done to the definition, you can update `spec.restartNonce` which will restart the job if it's different than its previous value:
```bash
kubectl -n flink patch FlinkDeployment flink-kubernetes-tutorial --type=merge --patch='{"spec":{"restartNonce":1234}}'
```

### Suspend/Start

For stopping a deployment, `spec.job.state` needs to be modified to the correct value: `running` / `suspended`

```bash
kubectl -n flink patch FlinkDeployment flink-kubernetes-tutorial --type=merge --patch='{"spec":{"job":{"state":"suspended"}}}'
kubectl -n flink patch FlinkDeployment flink-kubernetes-tutorial --type=merge --patch='{"spec":{"job":{"state":"running"}}}'
```


### Savepoint

For savepoints to work, Flink will need a durable storage to be able to save its data. You can set it up easily in Minikube using a `hostPath` Volume. The following example will mount the contents of `/tmp/flink-durable/` on the Minikube VM to all Flink containers.  

Before doing that though, on Minikube we will need to make sure that `/tmp/flink-durable/` exists and has the correct permissions. For this, we will need to SSH into Minikube's VM:
```bash
echo "sudo mkdir -p /data/flink && sudo chmod 777 /data/flink" | minikube ssh
```

After creating the directory on the Minikube VM, delete the previous `FlinkDeployment` and create a new one with the following specification:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-kubernetes-tutorial
spec:
  image: flink-kubernetes-tutorial
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.savepoints.dir: file:///opt/flink/durable/savepoints
    state.checkpoints.dir: file:///opt/flink/durable/checkpoints
    high-availability.storageDir: file:///opt/flink/durable/ha
    kubernetes.operator.periodic.savepoint.interval: 2h
  serviceAccount: flink
  mode: native
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
          - mountPath: /opt/flink/durable
            name: flink-volume
      volumes:
      - name: flink-volume
        hostPath:
          path: /data/flink/
          type: Directory
  job:
    args: ["--rowsPerSec", "10", "--outputPath", "/opt/flink/durable"]
    jarURI: local:///opt/flink/usrlib/flink-kubernetes-tutorial.jar
    parallelism: 4
    state: running
    upgradeMode: savepoint
```

In this definition you will see the following changes in addition to our original one:
- Savepoint and checkpoint configurations in `spec.flinkConfiguration`. Periodic savepoints were also enabled which will be trigger by the Operator.
- new volumes and mounts defined under `spec.podTemplate.spec` (the name needs to be `flink-main-container` to modify the Flink container itself)
- `upgradeMode` set to `savepoint` so we will be able to trigger savepoints and save them.
- Changed output path of our application to `/opt/flink/durable` so we won't lose progress.

You can use the previous command to create the new deployment:
```bash
kubectl -n flink delete FlinkDeployment flink-kubernetes-tutorial
kubectl -n flink apply -f flink-deployment.yaml
```

After the job is running, you will be able to trigger a savepoint using the following command:

```bash
kubectl -n flink patch FlinkDeployment flink-kubernetes-tutorial --type=merge --patch='{"spec":{"job":{"savepointTriggerNonce":1234}}}'
```

In case you suspend this job in the future, the Operator will automatically take a savepoint of it, and resume it using that savepoint if you restart it later.

# Ingress

Manually port-forwarding the service port might be sufficient for smaller local jobs, but for production we are better off using some kind of ingress solution.  

Ingress controllers let us route traffic from outside the Kubernetes cluster to our `Service` resources by providing a single point of entry and routing the traffic based on the request data (e.g. URL path) to the correct services. It can also be used to easily set up HTTPS for our services, without installing any certificates to Flink itself. One of the most popular Ingress controllers is the [NGINX Ingress controller](https://www.nginx.com/products/nginx-ingress-controller/), which is very simple to install in Minikube:

```bash
minikube addons enable ingress
```

Next up, the Ingress controller will require us to create `Ingress` resources in the Kubernetes cluster with the required filters and configurations that describe when and how to route requests to our Flink service. Luckily we won't have to do this ourselves as the Operator can automatically create this if we instruct it to:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-kubernetes-tutorial
spec:
  image: flink-kubernetes-tutorial
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  serviceAccount: flink
  mode: native
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: local:///opt/flink/usrlib/flink-kubernetes-tutorial.jar
    parallelism: 4
    state: running
    upgradeMode: stateless
  ingress:
    template: "/{{namespace}}/{{name}}(/|$)(.*)"
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: "/$2"
```

The only thing that was added to the first example is `spec.ingress`, which in this example will result in this `Ingress` being created:

Note: In case you use HAProxy (which is the default on OpenShift), you will probably need to change some configurations, e.g.:
```yaml
ingress:
  template: "openshift.test/{{namespace}}/{{name}}"
  annotations:
    haproxy.router.openshift.io/rewrite-target: /
```

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
  name: flink-kubernetes-tutorial
  namespace: flink
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - backend:
              service:
                name: flink-kubernetes-tutorial-rest
                port:
                  number: 8081
            path: /flink/flink-kubernetes-tutorial(/|$)(.*)
            pathType: ImplementationSpecific
```

You can use the previous command to create the new deployment:
```bash
kubectl -n flink delete FlinkDeployment flink-kubernetes-tutorial
kubectl -n flink apply -f flink-deployment.yaml
```

In case of Minikube you will also need to execute the following command to be able to connect to the NGINX Ingress controller:
```bash
minikube tunnel
```

After the tunnel is open, you should be able to use your browser and navigate to http://localhost/flink/flink-kubernetes-tutorial/ and see the Flink Dashboard. 

You can see that the Operator has replaced the template `/{{namespace}}/{{name}}(/|$)(.*)` with `/flink/flink-kubernetes-tutorial(/|$)(.*)` which correspond to our job's namespace and name. This makes it a lot easier to run multiple jobs, even in multiple namespaces with the same ingress configuration.  

The next thing you can notice is that we have specified two Regex capturing groups in the path filter. With the `nginx.ingress.kubernetes.io/rewrite-target` annotation we instructed the Ingress controller to rewrite the URI path to only contain characters matched by the second capture group (which is `(.*)`)  

This will re-write the path of `http://localhost/flink/flink-kubernetes-tutorial/#/job/running` to simply be `/#/job/running` when routing it to the Flink service.

You can also experiment with using the template `template: "flink.mydomain.com/{{namespace}}/{{name}}(/|$)(.*)"` for example. This will add the host `flink.mydomain.com` to the rules list and allows for even greater customization.

# Sidecars

Sometimes it's just not enough to have the Flink container running in your Kubernetes pod, because you want to extend your deployment in a way that Flink does not allow you to out of the box. By defining sidecars, we can instruct the Operator to create other containers in the Flink JobManager/TaskManager pods. Some examples:
- Download some artifacts (e.g. jars) before executing the job.
- Collect metrics/logs from Flink during runtime and analyzing/saving them.

The following example will set up another container running next to Flink in all the pods created, periodically outputting the size of the log file. While this does not add much value, you can use your imagination to add more useful logic to it:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-kubernetes-tutorial
spec:
  image: flink-kubernetes-tutorial
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  serviceAccount: flink
  mode: native
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: local:///opt/flink/usrlib/flink-kubernetes-tutorial.jar
    parallelism: 4
    state: running
    upgradeMode: stateless
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
            - mountPath: /opt/flink/log
              name: flink-logs
        - name: sidecar
          image: busybox
          command: [ 'sh','-c','while true; do wc -l /flink-logs/*.log; sleep 5; done' ]
          volumeMounts:
            - mountPath: /flink-logs
              name: flink-logs
      initContainers:
        # Sample sidecar container
        - name: sidecar-init
          image: busybox
          command: [ 'sh', '-c', 'echo initContainer loaded' ]
      volumes:
        - name: flink-logs
          emptyDir: {}
```

You can use the previous command to create the new deployment:
```bash
kubectl -n flink delete FlinkDeployment flink-kubernetes-tutorial
kubectl -n flink apply -f flink-deployment.yaml
```

This will create a new temporary volume called `flink-logs` that the Flink container will write its logs to, as it will be mounted to the default log output path, which is `/opt/flink/log`. This will also be mounted to a BusyBox container, which will periodically print its line count.

What's important to note here is that if we want to modify the Flink container itself, then we will have to use the name `flink-main-container` and the Operator will merge our configurations when creating the container.

There will also be an init-container, which is a type of container that needs to finish running and exit with code 0 before the other containers can start. This can be used to download artifacts for our Flink jobs for example.
