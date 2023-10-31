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

# SelectQuery = "SELECT \"Tags_name\",\"News_sophoraId\" from \"Tags_News\";"

SelectQuery = """SELECT TagCount.Name,"Tags_News"."News_sophoraId"  from (
SELECT "Tags_name" as Name from "Tags_News"
group by "Tags_name"
having count("News_sophoraId") > 0) as TagCount
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


# Calculate the counts of each node (tag) in relationships
node_counts = dict()
for tag, news_sophora_id in relationships:
    if tag in node_counts:
        node_counts[tag] += 1
    else:
        node_counts[tag] = 1

# Define a minimum node size
min_node_size = 100  # Adjust this to your desired minimum size

# Define a scaling factor for node sizes
scaling_factor = 100  # You can adjust this as needed

# Update the size of each node based on counts, ensuring a minimum size
node_sizes = {node: max(count * scaling_factor, min_node_size) for node, count in node_counts.items()}

# Enlarge the plot size
plt.figure(figsize=(21*8, 9*8))

# Spring layout for positioning nodes
pos = nx.spring_layout(G, k=1, iterations=10000)

# Draw node labels with custom font size, font family, and text positioning
node_labels = {node: '' if node in news_nodes else node for node in G.nodes()}
node_label_positions = nx.get_node_attributes(G, 'pos')
nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=8, font_family='sans-serif')

nx.draw_networkx_nodes(G, pos, nodelist=[node for node in G.nodes() if node not in news_nodes], node_color='r', node_size=[node_sizes[node] for node in G.nodes()if node not in news_nodes], alpha=0.8)

#save the graph
plt.savefig("graph.png")

