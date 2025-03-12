from dataclasses import dataclass
import enum
from typing import Any, Callable, Generic, Optional, TypeVar


VertexPropsT = TypeVar("VertexPropsT")
VertexHashIndex = dict["VertexID[VertexPropsT]", "Vertex[VertexPropsT]"]

SurogateID = str


class EdgeID[SrcVertexPropsT, EdgePropsT, DstVertexPropsT]: ...


class VertexID[VertexPropsT]: ...


class DirectoryID[VertexPropsT](str): ...


# =====================================================
#               Vertex Tables
# =====================================================


VertexPropsT = TypeVar("VertexPropsT")


class Vertex(Generic[VertexPropsT]):
    vertex_id: VertexID[VertexPropsT]
    edge_block_id: DirectoryID[VertexPropsT]
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
    VertexTypeTag.SomeVertexTag: VertexHashIndex[SomeVertexProps],
    VertexTypeTag.OtherVertexTag: VertexHashIndex[OtherVertexProps],
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


class Edge[SrcVertexPropsT, EdgePropsT, DstVertexPropsT]:
    id: EdgeID[SrcVertexPropsT, EdgePropsT, DstVertexPropsT]
    next_id: Optional[EdgeID[SrcVertexPropsT, EdgePropsT, DstVertexPropsT]]
    incident_vert_id: VertexID[DstVertexPropsT]
    props: EdgePropsT


@dataclass
class SomeEdgeProps: ...


@dataclass
class OtherEdgeProps: ...


SomeEdge = Edge[SomeVertexProps, SomeEdgeProps, SomeVertexProps]
OtherEdge = Edge[SomeVertexProps, OtherEdgeProps, OtherVertexProps]

SrcVertexPropsT = TypeVar("SrcVertexPropsT")
EdgePropsT = TypeVar("EdgePropsT")
DstVertexPropsT = TypeVar("DstVertexPropsT")

EdgeHashIndex = dict[
    EdgeID[SrcVertexPropsT, EdgePropsT, DstVertexPropsT],
    Edge[SrcVertexPropsT, EdgePropsT, DstVertexPropsT],
]


class EdgeTypeTag(enum.IntEnum):
    SomeEdgeTag = 0
    OtherEdgeTag = 1
    ...


EDGE_TABLE_RESOLVER = {
    EdgeTypeTag.SomeEdgeTag: EdgeHashIndex[
        SomeVertexProps, SomeEdgeProps, SomeVertexProps
    ],
    EdgeTypeTag.OtherEdgeTag: EdgeHashIndex[
        SomeVertexProps, OtherEdgeProps, OtherVertexProps
    ],
    # ...
}

# =====================================================
#               Edge Tables [END]
# =====================================================


# директория у каждой вершины своя. Поэтому параметризируем соответствующим типом вершины
@dataclass
class DirectoryEntry[VertexPropsT]:
    _DependentOn_edge_type_tag = Any

    edge_type_tag: EdgeTypeTag
    edge_id: EdgeID[VertexPropsT, _DependentOn_edge_type_tag, Any]  # зависимый тип :(
    next_block_id: Optional[DirectoryID[VertexPropsT]]


VertexPropsT = TypeVar("VertexPropsT")
DirectoryHashIndex = dict[DirectoryID[VertexPropsT], DirectoryEntry[VertexPropsT]]


DIRECTORY_INDEX_RESOLVER = {
    VertexTypeTag.SomeVertexTag: DirectoryHashIndex[SomeVertexProps]({}),
    VertexTypeTag.OtherVertexTag: DirectoryHashIndex[OtherVertexProps]({}),
}


class Traverser[VertexPropsT]:
    def __init__(self):
        self._seen_vs: dict[VertexID[VertexPropsT], bool] = {}

    def traverse(
        self,
        v: Vertex[VertexPropsT],
        filter: Callable[[VertexPropsT], bool],
        cur_depth: int,
        max_depth: int,
    ) -> list[Vertex[VertexPropsT]]: ...
