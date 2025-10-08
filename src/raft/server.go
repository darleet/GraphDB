package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/query"
	"github.com/google/uuid"
	hraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	buf "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sync/atomic"
	"time"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type Server struct {
	proto.UnimplementedRaftServiceServer

	exec *query.Executor

	raft      *hraft.Raft
	ticker    *atomic.Uint64
	txnLogger common.ITxnLogger

	log src.Logger
}

func New(exec *query.Executor, raft *hraft.Raft, ticker *atomic.Uint64, txnLogger common.ITxnLogger, log src.Logger) *Server {
	return &Server{
		exec:      exec,
		raft:      raft,
		ticker:    ticker,
		txnLogger: txnLogger,
		log:       log,
	}
}

// GetVertex returns a single vertex from the graph
func (s *Server) GetVertex(ctx context.Context, req *proto.GetVertexRequest) (*proto.GetVertexResponse, error) {
	id, err := uuid.Parse(req.GetVertexId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse vertex id: %s", err)
	}

	txnID := common.TxnID(s.ticker.Add(1))

	var v storage.Vertex
	err = execute(
		txnID,
		s.exec,
		s.txnLogger,
		s.log,
		func(txnID common.TxnID, e *query.Executor, logger common.ITxnLoggerWithContext) error {
			var selectErr error
			v, selectErr = s.exec.SelectVertex(txnID, req.GetTableName(), storage.VertexSystemID(id), s.txnLogger.WithContext(txnID))
			if selectErr != nil {
				return selectErr
			}
			return nil
		},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to select vertex: %s", err)
	}

	props, err := utils.GoAnyMapToStringMap(v.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert properties: %s", err)
	}

	return &proto.GetVertexResponse{
		VertexId:   v.ID.String(),
		TableId:    uuid.UUID(v.DirItemID).String(),
		Properties: props,
	}, nil
}

// InsertVertex inserts a single vertex into the graph
func (s *Server) InsertVertex(ctx context.Context, req *proto.InsertVertexRequest) (*proto.InsertVertexResponse, error) {
	txnID := common.TxnID(s.ticker.Add(1))

	record := s.toVertex(req.GetVertex())

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal vertex record: %s", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertVertex.String(), txnID, req.GetTableName(), recordBytes)
	future := s.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply failed: %s", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, status.Errorf(codes.Internal, "no response from raft apply")
	}

	vID, ok := resp.(storage.VertexSystemID)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected response type: %T", resp)
	}

	return &proto.InsertVertexResponse{
		VertexId: vID.String(),
	}, nil
}

// InsertVertices inserts multiple vertices in bulk
func (s *Server) InsertVertices(ctx context.Context, req *proto.InsertVerticesRequest) (*proto.InsertVerticesResponse, error) {
	txnID := common.TxnID(s.ticker.Add(1))

	records := s.toVertexSlice(req.GetVertices())

	recordsBytes, err := json.Marshal(records)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal vertices records: %s", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertVertices.String(), txnID, req.GetTableName(), recordsBytes)
	future := s.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply failed: %s", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, status.Errorf(codes.Internal, "no response from raft apply")
	}

	vIDs, ok := resp.([]storage.VertexSystemID)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected response type: %T", resp)
	}

	res := make([]string, 0, len(vIDs))
	for _, vID := range vIDs {
		res = append(res, vID.String())
	}

	return &proto.InsertVerticesResponse{
		VertexIds: res,
	}, nil
}

// InsertEdge inserts a single edge into the graph
func (s *Server) InsertEdge(ctx context.Context, req *proto.InsertEdgeRequest) (*proto.InsertEdgeResponse, error) {
	txnID := common.TxnID(s.ticker.Add(1))

	record := s.toEdgeInfo(req.GetEdge())

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal edge record: %s", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertEdge.String(), txnID, req.GetTableName(), recordBytes)
	future := s.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply failed: %s", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, status.Errorf(codes.Internal, "no response from raft apply")
	}

	edgeID, ok := resp.(storage.EdgeSystemID)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected response type: %T", resp)
	}

	return &proto.InsertEdgeResponse{
		EdgeId: edgeID.String(),
	}, nil
}

// InsertEdges inserts multiple edges into the graph
func (s *Server) InsertEdges(ctx context.Context, req *proto.InsertEdgesRequest) (*proto.InsertEdgesResponse, error) {
	txnID := common.TxnID(s.ticker.Add(1))

	records := s.toEdgeInfoSlice(req.GetEdges())

	recordsBytes, err := json.Marshal(records)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal edges records: %s", err)
	}

	data := fmt.Sprintf("%s\n%d\n%s\n%s", InsertEdges.String(), txnID, req.GetTableName(), recordsBytes)
	future := s.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply failed: %s", err)
	}

	resp := future.Response()
	if resp == nil {
		return nil, status.Errorf(codes.Internal, "no response from raft apply")
	}

	edgeIDs, ok := resp.([]storage.EdgeSystemID)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected response type: %T", resp)
	}

	res := make([]string, 0, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		res = append(res, edgeID.String())
	}

	return &proto.InsertEdgesResponse{
		EdgeIds: res,
	}, nil
}

func (s *Server) toVertex(in *proto.VertexInfo) storage.VertexInfo {
	m := make(map[string]any)

	if in.Label != nil {
		m["label"] = in.GetLabel()
	}

	if in.Properties != nil && in.Properties.Data != nil {
		for k, v := range in.GetProperties().GetData() {
			el, err := anypb.UnmarshalNew(v, buf.UnmarshalOptions{})
			if err != nil {
				s.log.Errorw("failed to unmarshal property value", zap.Error(err))
				return storage.VertexInfo{}
			}
			m[k] = el
		}
	}

	return storage.VertexInfo{
		SystemID: storage.VertexSystemID(uuid.New()),
		Data:     m,
	}
}

func (s *Server) toVertexSlice(in []*proto.VertexInfo) []storage.VertexInfo {
	out := make([]storage.VertexInfo, len(in))
	for i := range in {
		out[i] = s.toVertex(in[i])
	}
	return out
}

func (s *Server) toEdgeInfo(in *proto.EdgeInfo) storage.EdgeInfo {
	m := make(map[string]any)
	if in.Properties != nil {
		for k, v := range in.GetProperties().GetData() {
			m[k] = v
		}
	}

	return storage.EdgeInfo{
		SystemID:    storage.EdgeSystemID(uuid.New()),
		SrcVertexID: storage.VertexSystemID([]byte(in.GetSourceVertexId())),
		DstVertexID: storage.VertexSystemID([]byte(in.GetTargetVertexId())),
		Data:        m,
	}
}

func (s *Server) toEdgeInfoSlice(in []*proto.EdgeInfo) (out []storage.EdgeInfo) {
	out = make([]storage.EdgeInfo, len(in))
	for i := range in {
		out[i] = s.toEdgeInfo(in[i])
	}
	return out
}
