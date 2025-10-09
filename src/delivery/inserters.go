package delivery

import (
	"context"
	"github.com/Blackdeer1524/GraphDB/src/generated/api"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"go.uber.org/zap"
)

func (h *APIHandler) RaftInsertVertex(ctx context.Context, req *api.InsertVertexRequest) (api.RaftInsertVertexRes, error) {
	body := &proto.InsertVertexRequest{
		TableName: req.Table,
		Vertex:    h.toVertex(req.Record),
	}

	resp, err := h.Client.InsertVertex(ctx, body)
	if err != nil {
		h.Logger.Errorw("internal server error", zap.Error(err))
		return &api.RaftInsertVertexInternalServerError{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
		}, nil
	}

	return &api.VertexIDResponse{
		ID: resp.GetVertexId(),
	}, nil
}

func (h *APIHandler) RaftInsertVertices(ctx context.Context, req *api.InsertVerticesRequest) (api.RaftInsertVerticesRes, error) {
	body := &proto.InsertVerticesRequest{
		TableName: req.Table,
		Vertices:  h.toVertexSlice(req.Records),
	}

	resp, err := h.Client.InsertVertices(ctx, body)
	if err != nil {
		h.Logger.Errorw("internal server error", zap.Error(err))
		return &api.RaftInsertVerticesInternalServerError{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
		}, nil
	}

	return &api.VertexIDsResponse{
		Ids: resp.GetVertexIds(),
	}, nil
}

func (h *APIHandler) RaftInsertEdge(ctx context.Context, req *api.InsertEdgeRequest) (api.RaftInsertEdgeRes, error) {
	body := &proto.InsertEdgeRequest{
		TableName: req.Table,
		Edge:      h.toEdgeInfo(req.Edge),
	}

	resp, err := h.Client.InsertEdge(ctx, body)
	if err != nil {
		h.Logger.Errorw("internal server error", zap.Error(err))
		return &api.RaftInsertEdgeInternalServerError{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
		}, nil
	}

	return &api.EdgeIDResponse{
		ID: resp.GetEdgeId(),
	}, nil
}

func (h *APIHandler) RaftInsertEdges(ctx context.Context, req *api.InsertEdgesRequest) (api.RaftInsertEdgesRes, error) {
	body := &proto.InsertEdgesRequest{
		TableName: req.Table,
		Edges:     h.toEdgeInfoSlice(req.Edges),
	}

	resp, err := h.Client.InsertEdges(ctx, body)
	if err != nil {
		h.Logger.Errorw("internal server error", zap.Error(err))
		return &api.RaftInsertEdgesInternalServerError{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
		}, nil
	}

	return &api.EdgeIDsResponse{
		Ids: resp.GetEdgeIds(),
	}, nil
}

func (h *APIHandler) NewError(ctx context.Context, err error) *api.ErrorStatusCode {
	h.Logger.Errorw("internal server error", zap.Error(err))
	return &api.ErrorStatusCode{
		StatusCode: 500,
		Response: api.Error{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
			Details: api.OptErrorDetails{
				Set: false,
			},
		},
	}
}
