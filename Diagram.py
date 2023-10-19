import networkx as nx
import matplotlib.pyplot as plt
import psycopg2
import requests
import json
from graphviz import Digraph

# read json file
with open('settings.json') as json_file:
    data = json.load(json_file)

# Postgresql connection
print("Connecting to database")
conn = psycopg2.connect(
    host=data['database']['host'],
    port=data['database']['port'],
    database=data['database']['database'],
    user=data['database']['user'],
    password=data['database']['password']
)
cursor = conn.cursor()
print("Connected to database")

# Beispiel-Daten: Liste von Beziehungen zwischen N-Teilen und Objekten
relationships = []

SelectQuery = "SELECT \"Tags_name\",\"News_sophoraId\" from \"Tags_News\";"

SelectQuery = """SELECT TagCount.Name,"Tags_News"."News_sophoraId"  from (
SELECT "Tags_name" as Name from "Tags_News"
group by "Tags_name"
having count("News_sophoraId") > 1) as TagCount
inner join "Tags_News" on TagCount.Name = "Tags_News"."Tags_name";
"""

cursor.execute(SelectQuery)

for tupel in cursor.fetchall():
    relationships.append((tupel[0], tupel[1]))


# Create a NetworkX graph (G) to represent the relationships.
G = nx.Graph()

# Create a set to store all the News_sophoraId nodes
news_nodes = set()

# Add nodes and edges to the graph
for relationship in relationships:
    tag, news_sophora_id = relationship
    G.add_node(tag)  # Add the tag node
    news_nodes.add(news_sophora_id)  # Add the News_sophoraId node to the set
    G.add_edge(tag, news_sophora_id)  # Connect the tag to the News_sophoraId

# Enlarge the plot size
plt.figure(figsize=(42*4, 18*4))

# Spring layout for positioning nodes
pos = nx.spring_layout(G, k=2, iterations=2000)
# Draw the graph without edges, only showing tag nodes without News_sophoraId nodes
node_labels = {node: '' if node in news_nodes else node for node in G.nodes()}
nx.draw(G, pos, nodelist=[node for node in G.nodes() if node not in news_nodes], edgelist=[], labels=node_labels, with_labels=True)

#save the graph
plt.savefig("graph.png")
# Display the graph
# plt.show()

