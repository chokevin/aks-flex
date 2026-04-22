package nodebootstrap

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func Test_parseNodeLabels(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    map[string]string
		wantErr bool
	}{
		{name: "empty", in: nil, want: nil},
		{name: "simple", in: []string{"a=1", "b=2"}, want: map[string]string{"a": "1", "b": "2"}},
		{name: "empty value", in: []string{"a="}, want: map[string]string{"a": ""}},
		{name: "domain key", in: []string{"nvidia.com/gpu.product=H200"}, want: map[string]string{"nvidia.com/gpu.product": "H200"}},
		{name: "missing equals", in: []string{"justkey"}, wantErr: true},
		{name: "empty key", in: []string{"=value"}, wantErr: true},
		{name: "duplicate", in: []string{"a=1", "a=2"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNodeLabels(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("key %q: got %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func Test_parseTaints(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    []corev1.Taint
		wantErr bool
	}{
		{name: "empty", in: nil, want: nil},
		{
			name: "key=value:NoSchedule",
			in:   []string{"nvidia.com/gpu=present:NoSchedule"},
			want: []corev1.Taint{{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}},
		},
		{
			name: "key:NoSchedule (no value)",
			in:   []string{"dedicated:NoSchedule"},
			want: []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}},
		},
		{
			name: "all effects",
			in:   []string{"a:NoSchedule", "b:PreferNoSchedule", "c:NoExecute"},
			want: []corev1.Taint{
				{Key: "a", Effect: corev1.TaintEffectNoSchedule},
				{Key: "b", Effect: corev1.TaintEffectPreferNoSchedule},
				{Key: "c", Effect: corev1.TaintEffectNoExecute},
			},
		},
		{name: "missing effect", in: []string{"key=value"}, wantErr: true},
		{name: "invalid effect", in: []string{"key=value:Bogus"}, wantErr: true},
		{name: "empty key", in: []string{":NoSchedule"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTaints(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("[%d]: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
