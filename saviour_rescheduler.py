# Generic scheduler template


# currently no APIs to get price dynamically, so will manually create a list of
# prices until they release the APIs.

import random
from kubernetes import client, config
from datetime import datetime


SCHEDULER_NAME = "protoype-pricer"
PREEMPTIBLE_NODE_LABEL = 'cloud.google.com/gke-preemptible=true'

config.load_kube_config()
v1 = client.CoreV1Api()


def test_func():
    # Configs can be set in Configuration class directly or using helper utility
    config.load_kube_config()
    gs = GenericScheduler(preemptible_priority_function, random_predicate_function)

    print("Listing pods with their IPs:")
    ret = v1.list_pod_for_all_namespaces(watch=False)
    nodes_list = v1.list_node().items
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace,
            i.metadata.name))
        if i.metadata.namespace != 'kube-system':
            gs.schedule_pod_on_best_node(i, nodes_list)
    return


# Randomly chooses priority of node
# Arguments:
#   node: v1node* the node in question
#   pod: v1pod, the pod in question
def preemptible_priority_function(node, pod):
    if is_preemptible_node(node):
        return 100
    return random.choice((1, 2, 3, 4, 5))


# Returns whether the node in question is a preemptible VM or not.
# Arguments:
#   node: v1node* the node in question
def is_preemptible_node(node):
    if PREEMPTIBLE_NODE_LABEL in node.metadata.labels:
        return True


# Randomly chooses priority of node
# Arguments:
#   node: v1node* the node in question
#   pod: v1pod, the pod in question
def random_priority_function(node, pod):
    return random.choice((1, 2, 3, 4, 5))


# Arguments:
#   node: v1node* the node in question
#   pod: v1pod, the pod in question
# Randomly chooses whether the node should return or not
def random_predicate_function(node, pod):
    return random.choice((0, 1))


# Main scheduler class which takes in a priority function, a predicate function
# and the Kubernetes client.
class GenericScheduler:
    # List of priority functions to be applied on the filtered nodes list.
    priority_functions = []
    # List of predicate functions to be applied on nodes list to filter.
    predicate_functions = []
    # Use this init function to add a single function in the scheduler's
    # priority and predicate functions list.
    # Priority functions list is the list of priority function.
    # Predicate functions listn is the criteria used to evaluate the pod
    # scheduling.
    def __init__(self, priority_function, predicate_function):
        self.priority_functions.append(priority_function)
        self.predicate_functions.append(predicate_function)

    # Appends a function to the scheduler's priority function list.
    def add_priority_function(self, priority_func):
        self.priority_functions.append(priority_func)

    # Appends a function tos the scheduler's  predicate function list.
    def predicate_function(self, predicate_func):
        self.predicate_function.append(predicate_func)

    # Returns a list of nodes that are in a ready state to be used.
    # Returns: List consists of V1Node objects.
    def nodes_available(self):
        nodes_list = []
        for n in v1.list_node().items():
            for status in n.status_conditions:
                if status.status == 'True' and status.type == 'Ready':
                    nodes_list.append(n)
        return nodes_list

    def schedule_pod_on_best_node(self, pod, nodes_list):
        best_node_name = self.choose_best_node(pod, nodes_list).metadata.name
        if pod.spec.node_name != best_node_name:
            self.bind_pod_on_best_node(pod.metadata.name, best_node_name)
        #self.emit_event_on_binding(pod)

    # Chooses the best node according to the options available in the node
    # pools. Then sets a binding for the pod to the node.
    # Arguments:
    #   pod_name - name of the pod to be scheduled. Type str, name of pod.
    #   nodes_list - list of the available nodes. Type list of V1Node. 
    # Returns:
    #   The best node to schedule pod on of type V1Node
    def choose_best_node(self, pod, nodes_list):
        filtered_nodes = self.filter_nodes_with_predicates(nodes_list, pod)
        prioritized_nodes = nodes_list
        if filtered_nodes:
            prioritized_nodes = self.prioritize_nodes(filtered_nodes, pod)
        else:
            print 'no filtered node and choosing the first one'
        print 'Printing best node name'
        print prioritized_nodes[0].metadata.name
        return prioritized_nodes[0]

    # Filters all the possible nodes with the list of predicates we have.
    # Arguments:
    #   nodes_list of type V1Node
    #   pod - pod to be scheduled V1Pod
    # Returns:
    #   list of filtered nodes, of type V1Node.
    def filter_nodes_with_predicates(self, nodes_list, pod):
        filtered_nodes = []
        for node in nodes_list:
            if self.predicates_apply(node, pod):
                filtered_nodes.append(node)
        return filtered_nodes

    # Applies all predicates of the scheduler class on the combination of node
    # and pod to find out feasibility of scheduling ovre the node.
    # Arguments:
    #   node - the node to evaluate of type V1Node.
    #   pod - the pod to evaluate, of type <>.
    # Returns:
    #   true or false boolean, whether the node is feasible or not.
    def predicates_apply(self, node, pod):
        for predicate in self.predicate_functions:
            if predicate(node, pod) == 0:
                return False
        return True

    # Prioritizes nodes on the basis of priority functions.
    # Arguments:
    #   nodes_lisst
    #   pod - pod to be scheduled
    # Returns:
    #   sorted list of nodes by priority
    def prioritize_nodes(self, nodes_list, pod):
        prioritized_nodes = {}
        for node in nodes_list:
            for priority_func in self.priority_functions:
                if node.metadata.name not in prioritized_nodes:
                    prioritized_nodes[node] = priority_func(node, pod)
                else:
                    prioritized_nodes[node] += priority_func(node, pod)
        return sorted(prioritized_nodes.iterkeys())

    # Binds the pod on the chosen best node.
    def bind_pod_on_best_node(self, name, node, namespace='default'):
        target = client.V1ObjectReference()
        target.kind = "Node"
        target.apiVersion = "v1"
        target.name = node

        meta = client.V1ObjectMeta()
        meta.name = name
        body = client.V1Binding(target=target, metadata=meta, kind='Node',
                api_version='v1')

        return v1.create_namespaced_binding(namespace=namespace, body=body)

    # Emit event on scheduling the pod.
    def emit_event_on_binding(self, pod):
        timestamp = datetime.utcnow()

        target = client.V1ObjectReference()
        target.kind = "Pod"
        target.apiVersion = "v1"
        target.name = pod.metadata.name

        meta = client.V1ObjectMeta()
        meta.name = pod.metadata.name

        event_body = client.V1Event(involved_object=target, metadata=meta)
        event_body.event_time = timestamp
        event_body.action = 'Pod was scheduled using our awesome scheduler'
        v1.create_namespaced_event(
                namespace=pod.metadata.namespace,
                body=event_body)
#                include_uninitialized=True)
        return


def run():
    # Create client to connect to kubernetes cluster
    # create broadcaster to create events on changing the system
    # create generic predicate scheduler from the k8s scheduler
    # get list of nodes, divide into on demand and spot instances
    # start watching events
    test_func()


if __name__ == '__main__':
  run()
