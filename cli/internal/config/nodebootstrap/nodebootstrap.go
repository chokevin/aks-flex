package nodebootstrap

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api/features/kubeadm"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/userdata/flex"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/userdata/ubuntu"
	"github.com/Azure/aks-flex/plugin/pkg/util/cloudinit"

	"github.com/Azure/aks-flex/cli/internal/config/configcmd"
)

const (
	variantCloudInit = "cloud-init"
	variantScript    = "script"
)

var r = configcmd.NewRouter("node-bootstrap", "Generate a node bootstrap config for a remote cloud")
var Command *cobra.Command = r.Command()
var flagEnableNvidiaGPURuntime bool
var flagVariant string
var flagArch string
var flagKubeVersion string
var flagNodeLabels []string
var flagTaints []string

func init() {
	r.Handle("ubuntu", writeUbuntuUserData)
	r.Handle("flex", writeFlexUserData)

	Command.Flags().BoolVar(&flagEnableNvidiaGPURuntime, "nvidia-gpu", false, "Enable Nvidia GPU runtime in containerd configuration.")
	Command.Flags().StringVar(&flagArch, "arch", "amd64",
		"CPU architecture for the flex node binary (e.g. amd64, arm64).")
	Command.Flags().StringVar(&flagKubeVersion, "k8s-version", flex.DefaultKubeVer,
		"Kubernetes version for the downloaded binaries.")
	Command.Flags().StringVar(&flagVariant, "variant", variantCloudInit,
		fmt.Sprintf("Output variant: %q produces cloud-init YAML user data, %q produces an equivalent standalone bash script.", variantCloudInit, variantScript))
	Command.Flags().StringSliceVar(&flagNodeLabels, "node-label", nil,
		"Extra node label to register the node with, as key=value. Repeat for multiple labels. Merged with the labels derived from the AKS cluster (cluster name, managed=false, stretch-managed=true).")
	Command.Flags().StringSliceVar(&flagTaints, "taint", nil,
		"Taint to register the node with, as key[=value]:Effect (e.g. nvidia.com/gpu=present:NoSchedule). Repeat for multiple taints.")
}

// marshalUserData marshals the cloud-init UserData according to the selected
// --variant and writes it to w.
func marshalUserData(ud *cloudinit.UserData, w io.Writer) error {
	var data []byte
	var err error

	switch flagVariant {
	case variantCloudInit:
		data, err = ud.Marshal()
	case variantScript:
		data, err = marshalScript(ud)
	default:
		return fmt.Errorf("unsupported variant %q, supported: %s, %s", flagVariant, variantCloudInit, variantScript)
	}
	if err != nil {
		return fmt.Errorf("marshaling userdata as %s: %w", flagVariant, err)
	}

	_, err = w.Write(data)
	return err
}

func writeFlexUserData(ctx context.Context, w io.Writer) error {
	kc, err := kubeadmConfigFromFlags(ctx)
	if err != nil {
		return err
	}
	ud, err := flex.UserData(
		flex.WithEnableNvidiaGPURuntime(flagEnableNvidiaGPURuntime),
		flex.WithArch(flagArch),
		flex.WithKubeVersion(flagKubeVersion),
		flex.WithKubeadmConfig(kc),
	)
	if err != nil {
		return fmt.Errorf("generating flex userdata: %w", err)
	}
	return marshalUserData(ud, w)
}

func writeUbuntuUserData(ctx context.Context, w io.Writer) error {
	kc, err := kubeadmConfigFromFlags(ctx)
	if err != nil {
		return err
	}
	ud, err := ubuntu.UserData(kc)
	if err != nil {
		return fmt.Errorf("generating ubuntu userdata: %w", err)
	}
	return marshalUserData(ud, w)
}

// kubeadmConfigFromFlags returns the default kubeadm config (derived from the
// live AKS cluster when reachable) with extra --node-label and --taint flag
// values merged in.
func kubeadmConfigFromFlags(ctx context.Context) (*kubeadm.Config, error) {
	kc := configcmd.DefaultKubeadmConfig(ctx)

	extraLabels, err := parseNodeLabels(flagNodeLabels)
	if err != nil {
		return nil, err
	}
	if len(extraLabels) > 0 {
		kc.AddNodeLabels(extraLabels)
	}

	taints, err := parseTaints(flagTaints)
	if err != nil {
		return nil, err
	}
	if len(taints) > 0 {
		kc.AddK8SRegisterTaints(taints...)
	}

	return kc, nil
}
