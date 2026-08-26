package backend

import (
	"context"
	gatewayv1 "game-gateway/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	client gatewayv1.BackendServiceClient
}

func NewGRPCClient(conn grpc.ClientConnInterface) *GRPCClient {
	return &GRPCClient{
		client: gatewayv1.NewBackendServiceClient(conn),
	}
}

func (c *GRPCClient) Handle(ctx context.Context, req Request) (Response, error) {
	resp, err := c.client.Handle(ctx, &gatewayv1.BackendRequest{
		UserId:          req.UserID,
		SessionId:       req.SessionID,
		RoomId:          req.RoomID,
		MessageType:     req.MessageType,
		RequestId:       req.RequestID,
		Payload:         req.Payload,
		TimestampUnixMs: req.TimestampUnixMS,
	})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return Response{}, ErrTransportUnavailable
		}
		return Response{}, err
	}

	return Response{
		MessageType: resp.MessageType,
		Payload:     resp.Payload,
		ErrorCode:   resp.ErrorCode,
	}, nil
}
