# python script for loading graph data
# To connect the ArangoDB database u ses python-arango driver https://github.com/Joowani/python-arango
# To download dataset uses ogb
################################################################################
## DISCLAIMER
##
## Copyright 2021 ArangoDB GmbH, Cologne, Germany
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
## Copyright holder is ArangoDB GmbH, Cologne, Germany
##
## @author Andrei Lobov
################################################################################
from ogb.nodeproppred import NodePropPredDataset
from arango import ArangoClient

import sys
import time

month_labels = {0:"January", 1:"February", 2:"March", 3:"April", 4:"May", 5:"June", 6:"July",
                7:"August", 8:"September", 9:"October", 10:"November", 11:"December",
                12: "Unknown", 13:"Happy", 14:"Sad", 15:"Good"}

os_labels = {0:"Windows", 1:"Linux", 2:"FeeBSD", 3:"MacOSX", 4:"DOS", 5:"SOLARIS", 6:"Unix", 7:"BolgenOS", 8:"Intros", 9:"MSVS"}

def populateVertex(collection, num_vertex, prefix = ""):
  batch_size = 10000
  data = []
  total = 0
  totaltimeNs = 0
  count = 0
  for node_id in range(0, num_vertex+1):
    data.append({'_key': prefix + str(node_id),
                 'month_label': month_labels[node_id % len(month_labels)],
                 'os_label':os_labels[node_id % len(os_labels)],
                 'count': node_id})
    if len(data) > batch_size:
      # start time
      start = time.perf_counter_ns()
      collection.insert_many(data)
      # stop time
      took = (time.perf_counter_ns() - start)
      totaltimeNs += took
      data.clear()
      print('Vertex ' + prefix + ': Loaded ' + str(total) + ' ' + str( round((total/num_vertex) * 100, 2)) +
            '%  in total ' + str(totaltimeNs / 1000000) + 'ms Batch:' + 
            str(took/1000000) + 'ms Avg:' + str( (totaltimeNs/ (total/batch_size))/1000000) + 'ms \n')
    total = total + 1
    count = count + 1
  if len(data) > 0:
    # start time
    start = time.perf_counter_ns()
    collection.insert_many(data)
    # stop time
    took = (time.perf_counter_ns() - start)
    totaltimeNs += took
    print('Vertex ' + prefix + ': Loaded ' + str(total) + ' ' + str( round((total/num_vertex) * 100, 2)) +
          '%  in total ' + str(totaltimeNs / 1000000) + 'ms Batch:' +
          str(took/1000000) + 'ms Avg:' + str( (totaltimeNs/ (total/batch_size))/1000000) + 'ms \n')

def populateEdges(vertex_name, collection, edges_index, from_prefix = "", to_prefix = "", label = "common"):
  batch_size = 10000
  data = []
  total = 0
  totaltimeNs = 0
  count = 0
  size = len(edges_index[0])
  for edge in range(0, size):
    data.append({'_key': label + str(edge),
                 'month_label': month_labels[edge % len(month_labels)],
                 'os_label':os_labels[edge % len(os_labels)],
                 'label': label,
                 'count': edge,
                 '_from': vertex_name + '/' + from_prefix + str(edges_index[0][edge]),
                 '_to': vertex_name + '/' + to_prefix + str(edges_index[1][edge])})
    if len(data) > batch_size:
      # start time
      start = time.perf_counter_ns()
      collection.insert_many(data)
      # stop time
      took = (time.perf_counter_ns() - start)
      totaltimeNs += took
      data.clear()
      print('Edge ' + label + ': Loaded ' + str(total) + ' ' + str( round((total/size) * 100, 2)) +
            '%  in total ' + str(totaltimeNs / 1000000) + 'ms Batch:' + 
            str(took/1000000) + 'ms Avg:' + str( (totaltimeNs/ (total/batch_size))/1000000) + 'ms \n')
    total = total + 1
    count = count + 1
  if len(data) > 0:
    # start time
    start = time.perf_counter_ns()
    collection.insert_many(data)
    # stop time
    took = (time.perf_counter_ns() - start)
    totaltimeNs += took
    print('Edge ' + label + ': Loaded ' + str(total) + ' ' + str( round((total/size) * 100, 2)) +
          '%  in total ' + str(totaltimeNs / 1000000) + 'ms Batch:' +
          str(took/1000000) + 'ms Avg:' + str( (totaltimeNs/ (total/batch_size))/1000000) + 'ms \n')

def main():
  if len(sys.argv) < 7:
    print("Usage: host database graph edge_collection vertex_collection dataset Example: python GraphOGBLoader.py http://localhost:8529 _system ogb ogb_edges ogb_vertex ogbn-arxiv")
    return

    # target database objects names
  database_name = sys.argv[2]
  graph_name = sys.argv[3]
  edge_name = sys.argv[4]
  vertex_name = sys.argv[5]

  client = ArangoClient(hosts=sys.argv[1])
  db = client.db(database_name)

  if (db.has_graph(graph_name)):
    graph = db.graph(graph_name)
  else:
    graph = db.create_graph(graph_name)
  
  # create vertex collection if needed
  if graph.has_vertex_collection(vertex_name):
    vertex = graph.vertex_collection(vertex_name)
  else:
    vertex = graph.create_vertex_collection(vertex_name)

  # create edge collection if needed
  if graph.has_edge_collection(edge_name):
    edge = graph.edge_collection(edge_name)
  else:
    edge = graph.create_edge_definition(
      edge_collection=edge_name,
      from_vertex_collections=[vertex_name],
      to_vertex_collections=[vertex_name]
    )


  dataset = NodePropPredDataset(name = sys.argv[6])
  graph, label = dataset[0]
  if "num_nodes" in graph:
    num_nodes = graph["num_nodes"]
    #populating nodes
    populateVertex(vertex, num_nodes)
    edges = graph["edge_index"]
    populateEdges(vertex_name, edge, edges)
  else:
    num_dict = graph["num_nodes_dict"]
    edges_dict = graph["edge_index_dict"]
    for num_nodes_key in num_dict.keys():
      populateVertex(vertex, num_dict[num_nodes_key], num_nodes_key)

    for edge_key in edges_dict.keys():
      populateEdges(vertex_name, edge, edges_dict[edge_key], edge_key[0], edge_key[2], edge_key[1])

  

if __name__== "__main__":
  main()
