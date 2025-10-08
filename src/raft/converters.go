package raft

import (
	"fmt"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

func toProperties(in map[string]any) (*proto.Properties, error) {
	a, err := utils.GoMapToAnyMap(in)
	if err != nil {
		return nil, fmt.Errorf("failed to convert map: %w", err)
	}
	return &proto.Properties{
		Data: a,
	}, nil
}
