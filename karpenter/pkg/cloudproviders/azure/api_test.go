package azure

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsTypeMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "exact type mismatch status",
			err:  status.Error(codes.InvalidArgument, "type mismatch"),
			want: true,
		},
		{
			name: "wrapped type mismatch status",
			err:  fmt.Errorf("wrap: %w", status.Error(codes.InvalidArgument, "type mismatch")),
			want: true,
		},
		{
			name: "other invalid argument",
			err:  status.Error(codes.InvalidArgument, "bad request"),
			want: false,
		},
		{
			name: "not found",
			err:  status.Error(codes.NotFound, "not found"),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsTypeMismatch(tc.err); got != tc.want {
				t.Fatalf("IsTypeMismatch(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
