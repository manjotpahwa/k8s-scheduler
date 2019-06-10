# Kubernetes-scheduler
Price sensitive Kubernetes re-scheduler with support for adding more custom functions for measuring price.

Can work on all Kubernetes distros but optimizing only Google Cloud Platform.

Currently there are two predicate functions supported:
1. Random: Pick a random VM and assign a random weight.
2. Prioritizing preemptible VMs: If it is a preemptible VM, prioritize it higher.

It's still a very dumb function and can be improved a lot.
