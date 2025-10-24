import json
from dataclasses import dataclass, asdict
from typing import Optional
from tqdm import tqdm

with open('data.json', 'r') as f:
    data = json.load(f)

@dataclass
class Node:
    id: int
    name: Optional[str]
    school: Optional[str]
    country: Optional[str]
    year: Optional[int]
    subject: Optional[str]

@dataclass
class Edge:
    advisor_id: int
    student_id: int

new_data = {
    "nodes": [],
    "edges": [],
} 

for node in tqdm(data['nodes']):
    new_data['nodes'].append(
        Node(
            id=node['id'],
            name=node['name'] if node['name'] else None,
            school=node['school'] if node['school'] else None,
            country=node['country'] if node['country'] else None,
            year=int(node['year']) if node['year'] else None,
            subject=node['subject'] if node['subject'] else None,
        )
    )
    for student in node['students']:
        new_data['edges'].append(
            Edge(
                advisor_id=node['id'],
                student_id=student,
            )
        )
    for advisor in node['advisors']:
        new_data['edges'].append(
            Edge(
                advisor_id=advisor,
                student_id=node['id'],
            )
        )

# Convert dataclasses to dictionaries for JSON serialization
serializable_data = {
    "nodes": [asdict(node) for node in new_data['nodes']],
    "edges": [asdict(edge) for edge in new_data['edges']],
}

with open('new_data.json', 'w') as f:
    json.dump(serializable_data, f, indent=2)