package azure

import (
	"errors"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsNotFound returns true if err signals "resource does not exist". The error
// can flow back from either the gRPC plugin (NotFound) or directly from the
// Azure ARM SDK (HTTP 404).
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
		return true
	}
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) && rerr.StatusCode == 404 {
		return true
	}
	return false
}

// IsTypeMismatch returns true if err indicates the plugin returned an object of
// a different concrete protobuf type than the caller expected.
func IsTypeMismatch(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.InvalidArgument && strings.Contains(s.Message(), "type mismatch")
}

// IsQuotaError returns true if err signals an Azure quota / capacity exhaustion.
// We classify both HTTP 429 and the well-known Azure ARM error codes.
func IsQuotaError(err error) bool {
	if err == nil {
		return false
	}
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) {
		if rerr.StatusCode == 429 {
			return true
		}
		switch rerr.ErrorCode {
		case "QuotaExceeded",
			"OperationNotAllowed",
			"SkuNotAvailable",
			"AllocationFailed",
			"ZonalAllocationFailed",
			"OverconstrainedAllocationRequest":
			return true
		}
	}
	// Fallback substring match — covers gRPC-wrapped error strings and any
	// codes the SDK didn't surface structurally.
	msg := err.Error()
	return strings.Contains(msg, "QuotaExceeded") ||
		strings.Contains(msg, "OperationNotAllowed") ||
		strings.Contains(msg, "SkuNotAvailable") ||
		strings.Contains(msg, "AllocationFailed")
}
