package nodebootstrap

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// parseNodeLabels parses a slice of "key=value" strings into a map. An empty
// slice returns a nil map. Duplicate keys cause an error.
func parseNodeLabels(in []string) (map[string]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(in))
	for _, s := range in {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid --node-label %q: expected key=value", s)
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("duplicate --node-label key %q", k)
		}
		out[k] = v
	}
	return out, nil
}

// parseTaints parses a slice of "key=value:Effect" or "key:Effect" strings into
// []corev1.Taint. Effect must be one of NoSchedule, PreferNoSchedule, NoExecute.
func parseTaints(in []string) ([]corev1.Taint, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]corev1.Taint, 0, len(in))
	for _, s := range in {
		// Effect is everything after the LAST ':'. This allows ':' in the value
		// portion when the input is in key=value:Effect form.
		idx := strings.LastIndex(s, ":")
		if idx < 0 {
			return nil, fmt.Errorf("invalid --taint %q: expected key[=value]:Effect", s)
		}
		head, effect := s[:idx], corev1.TaintEffect(s[idx+1:])
		switch effect {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
		default:
			return nil, fmt.Errorf("invalid --taint %q: effect must be NoSchedule, PreferNoSchedule, or NoExecute", s)
		}
		var key, value string
		if i := strings.Index(head, "="); i >= 0 {
			key, value = head[:i], head[i+1:]
		} else {
			key = head
		}
		if key == "" {
			return nil, fmt.Errorf("invalid --taint %q: empty key", s)
		}
		out = append(out, corev1.Taint{Key: key, Value: value, Effect: effect})
	}
	return out, nil
}
