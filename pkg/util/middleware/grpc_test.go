package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCode_NoError(t *testing.T) {
	a := ErrorCode(nil)
	assert.Equal(t, a, "2xx")
}

func TestErrorCode_Any5xx(t *testing.T) {
	err := httpgrpc.Errorf(http.StatusNotImplemented, "Fail")
	a := ErrorCode(err)
	assert.Equal(t, a, "5xx")
}

func TestErrorCode_Any4xx(t *testing.T) {
	err := httpgrpc.Errorf(http.StatusConflict, "Fail")
	a := ErrorCode(err)
	assert.Equal(t, a, "4xx")
}

func TestErrorCode_Canceled(t *testing.T) {
	err := status.Errorf(codes.Canceled, "Fail")
	a := ErrorCode(err)
	assert.Equal(t, a, "4xx")
}

func TestErrorCode_Unknown(t *testing.T) {
	err := status.Errorf(codes.Unknown, "Fail")
	a := ErrorCode(err)
	assert.Equal(t, a, "5xx")
}
