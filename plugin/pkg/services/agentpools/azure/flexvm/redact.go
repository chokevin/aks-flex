// Package flexvm implements the cross-region Azure VM agent pool service.
// See agentpools.go for the package overview and authentication contract.
package flexvm

func (ap *AgentPool) Redact() {
	ap.GetSpec().GetKubeadm().Redact()
}

func (i *Instance) Redact() {
}
