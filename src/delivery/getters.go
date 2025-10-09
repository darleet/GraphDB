package delivery

import (
	"context"
	"github.com/Blackdeer1524/GraphDB/src/generated/api"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"go.uber.org/zap"
)

func (h *APIHandler) RaftGetVertex(ctx context.Context, params api.RaftGetVertexParams) (api.RaftGetVertexRes, error) {
	body := &proto.GetVertexRequest{
		TableName: params.Table,
		VertexId:  params.ID,
	}

	resp, err := h.Client.GetVertex(ctx, body)
	if err != nil {
		h.Logger.Errorw("internal server error", zap.Error(err))
		return &api.RaftGetVertexInternalServerError{
			Code:    "INTERNAL_SERVER_ERROR",
			Message: "Internal Server Error",
		}, nil
	}

	return &api.VertexResponse{
		ID:   resp.GetVertexId(),
		Data: resp.Properties,
	}, nil
}
