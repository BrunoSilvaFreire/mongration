from collections import deque
from enum import Enum
from typing import Generic, TypeVar, Callable, List, Set

from mongrations.misc.buffered_drawing import BufferedCanvas


def is_completed(needed: set, visited: set):
    return needed.issubset(visited)


class _Node:
    def __init__(self, data=None):
        self._map = {}
        self._vertex = data

    @property
    def connections(self):
        return self._map

    @property
    def data(self):
        return self._vertex

    @data.setter
    def data(self, value):
        self._vertex = value

    def edge(self, to):
        return self._map.get(to, None)

    def connect(self, index, edge_data):
        self._map[index] = edge_data

    def disconnect(self, index):
        if index in self._map:
            del self._map[index]
            return True
        return False

    def clear(self):
        self._map.clear()
        self._vertex = None


class EdgeView:
    def __init__(self, owner):
        self._elements = list(owner.connections.items())

    def __iter__(self):
        return iter(self._elements)


# Technically an adjacency list, but nobody cares
class Graph:
    def __init__(self):
        self._nodes = []
        self._free_indices = []
        self._free_indices_set = set()

    def clear(self):
        self._nodes.clear()

    def remove(self, index):
        if index < len(self._nodes):
            self._nodes[index].clear()
            self._free_indices.append(index)
            self._free_indices_set.add(index)

    def push(self, vertex):
        if self._free_indices:
            index = self._free_indices.pop(0)
            self._free_indices_set.remove(index)
            self._nodes[index].data = vertex
        else:
            index = len(self._nodes)
            self._nodes.append(_Node(vertex))
        return index

    def size(self):
        return len(self._nodes) - len(self._free_indices)

    def empty(self):
        return self.size() == 0

    def all_vertices_indices(self):
        return [i for i in range(len(self._nodes)) if i not in self._free_indices_set]

    def vertex(self, index):
        if index < len(self._nodes):
            return self._nodes[index].data
        return None

    def edge(self, from_index, to_index):
        if from_index < len(self._nodes):
            return self._nodes[from_index].edge(to_index)
        return None

    def connect(self, from_index, to_index, edge):
        if from_index < len(self._nodes):
            self._nodes[from_index].connect(to_index, edge)

    def edges_from(self, index):
        if index < len(self._nodes):
            return EdgeView(self._nodes[index])
        return None

    def node(self, index):
        if index < len(self._nodes):
            return self._nodes[index]
        return None

    def disconnect(self, from_index, to_index):
        if from_index < len(self._nodes):
            return self._nodes[from_index].disconnect(to_index)
        return False


# Define the dependency type using an Enum
class DependencyType(Enum):
    USES = 1
    USED = 2


# Convert the C++ `to_string` and operator overload for printing to a simple string representation
def dependency_type_to_string(dependency_type: DependencyType) -> str:
    if dependency_type == DependencyType.USES:
        return "uses"
    elif dependency_type == DependencyType.USED:
        return "used"


# Define the generic types for the vertex and index
TVertex = TypeVar('TVertex')


def clamp_string(s, max_size):
    if len(s) > max_size:
        return f"{s[:max_size - 3]}..."
    return s


class DependencyGraph(Generic[TVertex]):
    def __init__(self):
        self._graph = Graph()  # Use the provided Graph class
        self._independent = set()

    def add_dependency(self, from_: int, to: int):
        self._independent.discard(from_)
        self._graph.connect(from_, to, DependencyType.USES)
        self._graph.connect(to, from_, DependencyType.USED)

    def remove(self, index: int):
        self._graph.remove(index)

    def disconnect(self, from_: int, to: int) -> bool:
        success_from = self._graph.disconnect(from_, to)
        success_to = self._graph.disconnect(to, from_)
        return success_from and success_to

    def add(self, vertex: TVertex) -> int:
        index = self._graph.push(vertex)
        self._independent.add(index)
        return index

    def dependencies_of(self, index: int) -> List[int]:
        node = self._graph.node(index)
        if node is None:
            return []
        return [to for to, edge in node.connections.items() if edge == DependencyType.USES]

    def dependants_on(self, index: int) -> List[int]:
        node = self._graph.node(index)
        if node is None:
            return []
        return [to for to, edge in node.connections.items() if edge == DependencyType.USED]

    def get_size(self) -> int:
        return self._graph.size()

    def traverse(
            self,
            per_vertex: Callable[[int], None],
            per_edge: Callable[[int, int], None]
    ):
        visited = set()
        ready = deque(self._independent)

        while ready:
            vertex = ready.pop()
            if vertex in visited:
                continue
            visited.add(vertex)
            per_vertex(vertex)

            for edge in self._graph.edges_from(vertex):
                neighbor, edge_type = edge
                if neighbor not in visited:
                    needed = set(self.dependencies_of(neighbor))
                    if is_completed(needed, visited):
                        per_edge(vertex, neighbor)
                        if neighbor not in ready:
                            ready.appendleft(neighbor)

    def print_to_terminal(self, circle_radius=5, padding=2, name_selector=None):
        canvas = BufferedCanvas()
        vertices_to_level = {}

        def per_vertex(vertex):
            if vertex not in vertices_to_level:
                vertices_to_level[vertex] = 0

        def per_edge(src, dst):
            candidate = vertices_to_level[src] + 1
            if dst not in vertices_to_level or candidate < vertices_to_level[dst]:
                vertices_to_level[dst] = candidate

        self.traverse(per_vertex, per_edge)

        levels: dict[int, dict[int, (int, int)]] = {}
        circle_size = (circle_radius * 2) + padding
        offset = circle_size / 2 - padding / 2

        vertex_centers = dict()
        for index, level in vertices_to_level.items():
            vertex_map = levels.get(level)
            if vertex_map is None:
                levels[level] = vertex_map = dict()
            already_exists = len(vertex_map)

            center = (
                int(offset + level * circle_size),
                int(offset + already_exists * circle_size)
            )
            vertex_map[index] = center
            vertex_centers[index] = center
        for level, vertex_map in levels.items():
            for index, center in vertex_map.items():
                for dst, edge in self._graph.edges_from(index):
                    canvas.draw_line(center, vertex_centers[dst])
        max_msg_len = circle_radius * circle_radius
        for level, vertex_map in levels.items():
            for index, center in vertex_map.items():
                canvas.draw_circle(center, circle_radius)
                vertex = self._graph.vertex(index)
                if name_selector is not None:
                    msg = name_selector(vertex)
                else:
                    msg = str(vertex)
                canvas.draw_centered(center, clamp_string(msg, max_msg_len), circle_radius, circle_radius)

        print(canvas)

        with open("mongration.graph.txt", "w") as f:
            f.writelines(str(canvas))

    def is_completed(self, needed: Set[int], visited: Set[int]) -> bool:
        return needed.issubset(visited)

    def __getitem__(self, index: int):
        return self._graph.vertex(index)
