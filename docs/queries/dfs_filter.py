from dataclasses import dataclass
import enum
from typing import Any, Callable, Generator, Generic, Optional, TypeVar


VertexPropsT = TypeVar("VertexPropsT")
# На каждый тип вершин он свой
VertexHashIndex = dict["VertexID", "BaseVertex"]

SurogateID = str
# За пользовательские ID отвечает индекс
EdgeID = SurogateID
VertexID = SurogateID
EdgeBlockID = SurogateID


# =====================================================
#               Vertex Tables
# =====================================================


@dataclass
class BaseVertex:
    vertex_id: VertexID
    edge_block_id: EdgeBlockID


@dataclass
class SomeVertex(BaseVertex): ...


@dataclass
class OtherVertex(BaseVertex): ...


class VertexTypeTag(enum.IntEnum):
    SomeVertexTag = 0
    OtherVertexTag = 1
    ...


VERTEX_TABLE_RESOLVER = {
    VertexTypeTag.SomeVertexTag: VertexHashIndex({}),
    VertexTypeTag.OtherVertexTag: VertexHashIndex({}),
    # ...
}


# =====================================================
#               Vertex Tables [END]
# =====================================================


# =====================================================
#               Edge Tables
# =====================================================


EdgePropsT = TypeVar("EdgePropsT")
IncidentVertexPropsT = TypeVar("IncidentVertexPropsT")


@dataclass
class BaseEdge:
    id: EdgeID
    next_id: Optional[EdgeID]
    incident_v_id: VertexID


@dataclass
class SomeEdge(BaseEdge): ...


@dataclass
class OtherEdge(BaseEdge): ...


EdgeHashIndex = dict[EdgeID, BaseEdge]


class EdgeTypeTag(enum.IntEnum):
    SomeEdgeTag = 0
    OtherEdgeTag = 1
    ...


EDGE_TABLE_RESOLVER = {
    EdgeTypeTag.SomeEdgeTag: EdgeHashIndex({}),
    EdgeTypeTag.OtherEdgeTag: EdgeHashIndex({}),
    # ...
}

# =====================================================
#               Edge Tables [END]
# =====================================================


# директория у каждой вершины своя. Поэтому параметризируем соответствующим типом вершины
@dataclass
class DirectoryEntry:
    edge_type_tag: EdgeTypeTag
    edge_id: Optional[EdgeID]
    next_block_id: Optional[EdgeBlockID]


DirectoryHashIndex = dict[EdgeBlockID, DirectoryEntry]
TypedDirectoryHashIndex = dict[tuple[VertexID, EdgeTypeTag], DirectoryEntry]


DIRECTORY_INDEX_RESOLVER: dict[VertexTypeTag, DirectoryHashIndex] = {
    VertexTypeTag.SomeVertexTag: DirectoryHashIndex({}),
    VertexTypeTag.OtherVertexTag: DirectoryHashIndex({}),
}

TYPED_DIRECTORY_INDEX_RESOLVER: dict[VertexTypeTag, TypedDirectoryHashIndex] = {
    VertexTypeTag.SomeVertexTag: TypedDirectoryHashIndex({}),
    VertexTypeTag.OtherVertexTag: TypedDirectoryHashIndex({}),
}

EDGE_CHECKER = {
    VertexTypeTag.SomeVertexTag: [
        EdgeTypeTag.SomeEdgeTag,
        EdgeTypeTag.OtherEdgeTag,
    ],
    VertexTypeTag.OtherVertexTag: [],
}


# проверит, что заданное есть ребро у типа Vertex и то, что
# тип ребра назначения также равен Vertex. Будет механизм уведомления
# о нарушении
def assert_edge_T_T(v_tag: VertexTypeTag, e_tag: EdgeTypeTag):
    pass


def vertex_neighborhood(
    v_tag: VertexTypeTag, v_id: VertexID, e_tag: EdgeTypeTag
) -> Generator[BaseEdge]:
    d = TYPED_DIRECTORY_INDEX_RESOLVER[v_tag]
    edge_table = EDGE_TABLE_RESOLVER[e_tag]
    block = d[(v_id, e_tag)]
    next_edge_id = block.edge_id
    while next_edge_id is not None:
        edge = edge_table[next_edge_id]
        yield edge
        next_edge_id = edge.next_id


class Traverser:
    def __init__(
        self,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[Any], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[Any], bool],
        max_depth: int,
    ):
        self._seen_vs: set[VertexID] = set()
        self.v_tag = vertex_tag
        self.vertex_filter = vertex_filter
        self.e_tag = edge_tag
        self.edge_filter = edge_filter
        self.max_depth = max_depth
        self._result: list[VertexID] = []

    @staticmethod
    # ЗАПРОС 1
    def DFS(
        vertex_id: VertexID,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[BaseVertex], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[BaseEdge], bool],
        max_depth: int,
    ):
        t = Traverser(vertex_tag, vertex_filter, edge_tag, edge_filter, max_depth)

        assert_edge_T_T(t.v_tag, edge_tag)
        return t.dfs_traverse(vertex_id)

    def dfs_traverse(self, vertex_id: VertexID) -> list[VertexID]:
        self._seen_vs.clear()
        self._result.clear()
        self._dfs_traverse_helper(vertex_id, 1)
        return self._result

    def _dfs_traverse_helper(self, v_id: VertexID, cur_depth: int):
        if cur_depth == self.max_depth:
            self._result.append(v_id)
            return

        if v_id in self._seen_vs:
            return

        self._seen_vs.add(v_id)
        vert_table = VERTEX_TABLE_RESOLVER[self.v_tag]
        v = vert_table[v_id]
        if self.vertex_filter(v):
            return

        for edge in vertex_neighborhood(self.v_tag, v_id, self.e_tag):
            if not self.edge_filter(edge):
                continue
            self._dfs_traverse_helper(edge.incident_v_id, cur_depth + 1)


def count_triangles(v_tag: VertexTypeTag, e_tag: EdgeTypeTag):
    assert_edge_T_T(v_tag, e_tag)
    vertex_table = VERTEX_TABLE_RESOLVER[v_tag]

    c = 0
    for v_id in vertex_table:
        left_neighbourhood = set(e.id for e in vertex_neighborhood(v_tag, v_id, e_tag))
        for edge in vertex_neighborhood(v_tag, v_id, e_tag):
            right_neighbourhood = set(
                e.id for e in vertex_neighborhood(v_tag, edge.incident_v_id, e_tag)
            )

            c += len(left_neighbourhood.intersection(right_neighbourhood))

    return c / 3
