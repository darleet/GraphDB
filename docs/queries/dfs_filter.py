from dataclasses import dataclass
import enum
from typing import Any, Callable, Generic, Optional, TypeVar


VertexPropsT = TypeVar("VertexPropsT")
# На каждый тип вершин он свой
VertexHashIndex = dict["VertexID", "Vertex[VertexPropsT]"]

SurogateID = str
# За пользовательские ID отвечает индекс
EdgeID = SurogateID
VertexID = SurogateID
EdgeBlockID = SurogateID


# =====================================================
#               Vertex Tables
# =====================================================


VertexPropsT = TypeVar("VertexPropsT")


class Vertex(Generic[VertexPropsT]):
    vertex_id: VertexID
    edge_block_id: EdgeBlockID
    props: VertexPropsT


@dataclass
class SomeVertexProps: ...


@dataclass
class OtherVertexProps: ...


SomeVertex = Vertex[SomeVertexProps]
OtherVertex = Vertex[OtherVertexProps]


class VertexTypeTag(enum.IntEnum):
    SomeVertexTag = 0
    OtherVertexTag = 1
    ...


VERTEX_TABLE_RESOLVER = {
    VertexTypeTag.SomeVertexTag: VertexHashIndex[SomeVertexProps]({}),
    VertexTypeTag.OtherVertexTag: VertexHashIndex[OtherVertexProps]({}),
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


class Edge[EdgePropsT]:
    id: EdgeID
    next_id: Optional[EdgeID]
    incident_vert_id: VertexID
    props: EdgePropsT


@dataclass
class SomeEdgeProps: ...


@dataclass
class OtherEdgeProps: ...


SomeEdge = Edge[SomeEdgeProps]
OtherEdge = Edge[OtherEdgeProps]

EdgePropsT = TypeVar("EdgePropsT")

EdgeHashIndex = dict[EdgeID, Edge[EdgePropsT]]


class EdgeTypeTag(enum.IntEnum):
    SomeEdgeTag = 0
    OtherEdgeTag = 1
    ...


EDGE_TABLE_RESOLVER = {
    EdgeTypeTag.SomeEdgeTag: EdgeHashIndex[SomeEdgeProps]({}),
    EdgeTypeTag.OtherEdgeTag: EdgeHashIndex[OtherEdgeProps]({}),
    # ...
}

# =====================================================
#               Edge Tables [END]
# =====================================================


# директория у каждой вершины своя. Поэтому параметризируем соответствующим типом вершины
@dataclass
class DirectoryEntry[VertexPropsT]:
    edge_type_tag: EdgeTypeTag
    edge_id: EdgeID
    next_block_id: Optional[EdgeBlockID]


VertexPropsT = TypeVar("VertexPropsT")
DirectoryHashIndex = dict[EdgeBlockID, DirectoryEntry[VertexPropsT]]


DIRECTORY_INDEX_RESOLVER = {
    VertexTypeTag.SomeVertexTag: DirectoryHashIndex[SomeVertexProps]({}),
    VertexTypeTag.OtherVertexTag: DirectoryHashIndex[OtherVertexProps]({}),
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
def check_edge_tag_is_valid(v_tag: VertexTypeTag, e_tag: EdgeTypeTag): ...


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
        self.vertex_tag = vertex_tag
        self.vertex_filter = vertex_filter
        self.edge_tag = edge_tag
        self.edge_filter = edge_filter
        self.max_depth = max_depth

    @staticmethod
    # ЗАПРОС 1
    def DFS(
        vertex_id: VertexID,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[Any], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[Any], bool],
        max_depth: int,
    ):
        t = Traverser(vertex_tag, vertex_filter, edge_tag, edge_filter, max_depth)

        check_edge_tag_is_valid(t.vertex_tag, edge_tag)
        return t.dfs_traverse(vertex_id)

    def dfs_traverse(self, vertex_id: VertexID) -> list[VertexID]:
        self._seen_vs.clear()
        return self._dfs_traverse_helper(vertex_id, 0)

    def _dfs_traverse_helper(
        self, vertex_id: VertexID, cur_depth: int
    ) -> list[VertexID]:
        if cur_depth == self.max_depth:
            return []

        if vertex_id in self._seen_vs:
            return []

        self._seen_vs.add(vertex_id)
        vert_table = VERTEX_TABLE_RESOLVER[self.vertex_tag]
        v = vert_table[vertex_id]
        if self.vertex_filter(v):
            return []

        director_table = DIRECTORY_INDEX_RESOLVER[self.vertex_tag]
        block = director_table[v.edge_block_id]
        while block.edge_type_tag != self.edge_tag:
            if block.next_block_id is None:
                return []
            block = director_table[block.next_block_id]

        edges_table = EDGE_TABLE_RESOLVER[self.edge_tag]

        next_edge_id = block.edge_id
        res: list[VertexID] = [vertex_id]
        while next_edge_id is not None:
            edge = edges_table[next_edge_id]
            next_edge_id = edge.next_id
            if not self.edge_filter(edge):
                continue
            res.extend(self._dfs_traverse_helper(edge.incident_vert_id, cur_depth + 1))
        return res
