package oneagent

import (
	"context"
	"fmt"
	dynatracev1alpha1 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha1"
	dynatracev1alpha2 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha2"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/utils"
	"github.com/Dynatrace/dynatrace-oneagent-operator/version"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"time"
)

// time between consecutive queries for a new pod to get ready
const splayTimeSeconds = uint16(10)
const annotationTemplateHash = "internal.oneagent.dynatrace.com/template-hash"

func UpdateCR(instance dynatracev1alpha1.BaseOneAgentDaemonSet, c client.Client) error {
	instance.GetOneAgentStatus().UpdatedTimestamp = metav1.Now()
	return c.Status().Update(context.TODO(), instance)
}

func DetermineOneAgentPhase(instance dynatracev1alpha1.BaseOneAgentDaemonSet, c client.Client) (bool, error) {
	var phaseChanged bool
	dsActual := &appsv1.DaemonSet{}
	err := c.Get(context.TODO(), types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()}, dsActual)

	if k8serrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		phaseChanged = instance.GetOneAgentStatus().Phase != dynatracev1alpha1.Error
		instance.GetOneAgentStatus().Phase = dynatracev1alpha1.Error
		return phaseChanged, err
	}

	if dsActual.Status.NumberReady == dsActual.Status.CurrentNumberScheduled {
		phaseChanged = instance.GetOneAgentStatus().Phase != dynatracev1alpha1.Running
		instance.GetOneAgentStatus().Phase = dynatracev1alpha1.Running
	} else {
		phaseChanged = instance.GetOneAgentStatus().Phase != dynatracev1alpha1.Deploying
		instance.GetOneAgentStatus().Phase = dynatracev1alpha1.Deploying
	}

	return phaseChanged, nil
}

func NewDaemonSetForCR(instance dynatracev1alpha1.BaseOneAgentDaemonSet, logger logr.Logger) (*appsv1.DaemonSet, error) {
	podSpec := newPodSpecForCR(instance, logger)
	selectorLabels := BuildLabels(instance.GetName())
	mergedLabels := mergeLabels(instance.GetOneAgentSpec().Labels, selectorLabels)

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        instance.GetName(),
			Namespace:   instance.GetNamespace(),
			Labels:      mergedLabels,
			Annotations: map[string]string{},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: mergedLabels},
				Spec:       podSpec,
			},
		},
	}

	dsHash, err := GenerateDaemonSetHash(ds)
	if err != nil {
		return nil, err
	}
	ds.Annotations[annotationTemplateHash] = dsHash

	return ds, nil
}

func newPodSpecForCR(instance dynatracev1alpha1.BaseOneAgentDaemonSet, logger logr.Logger) corev1.PodSpec {
	p := corev1.PodSpec{}
	trueVar := true
	img := "docker.io/dynatrace/oneagent:latest"

	sa := "dynatrace-oneagent"
	if instance.GetOneAgentSpec().ServiceAccountName != "" {
		sa = instance.GetOneAgentSpec().ServiceAccountName
	}

	args := instance.GetOneAgentSpec().Args
	if instance.GetOneAgentSpec().Proxy != nil && (instance.GetOneAgentSpec().Proxy.ValueFrom != "" || instance.GetOneAgentSpec().Proxy.Value != "") {
		args = append(args, "--set-proxy=$(https_proxy)")
	}

	if instance.GetOneAgentSpec().NetworkZone != "" {
		args = append(args, fmt.Sprintf("--set-network-zone=%s", instance.GetOneAgentSpec().NetworkZone))
	}

	if _, ok := instance.(*dynatracev1alpha1.OneAgentIM); ok {
		args = append(args, "--set-infra-only=true")
	}

	args = append(args, "--set-host-property=OperatorVersion="+version.Version)

	// K8s 1.18+ is expected to drop the "beta.kubernetes.io" labels in favor of "kubernetes.io" which was added on K8s 1.14.
	// To support both older and newer K8s versions we use node affinity.

	p = corev1.PodSpec{
		Containers: []corev1.Container{{
			Args:            args,
			Env:             nil,
			Image:           img,
			ImagePullPolicy: corev1.PullAlways,
			Name:            "dynatrace-oneagent",
			ReadinessProbe: &corev1.Probe{
				Handler: corev1.Handler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/sh", "-c", "grep -q oneagentwatchdo /proc/[0-9]*/stat",
						},
					},
				},
				InitialDelaySeconds: 30,
				PeriodSeconds:       30,
				TimeoutSeconds:      1,
			},
			Resources: instance.GetOneAgentSpec().Resources,
			SecurityContext: &corev1.SecurityContext{
				Privileged: &trueVar,
			},
			VolumeMounts: prepareVolumeMounts(instance),
		}},
		HostNetwork:        true,
		HostPID:            true,
		HostIPC:            true,
		NodeSelector:       instance.GetOneAgentSpec().NodeSelector,
		PriorityClassName:  instance.GetOneAgentSpec().PriorityClassName,
		ServiceAccountName: sa,
		Tolerations:        instance.GetOneAgentSpec().Tolerations,
		DNSPolicy:          instance.GetOneAgentSpec().DNSPolicy,
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "beta.kubernetes.io/arch",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"amd64", "arm64"},
								},
								{
									Key:      "beta.kubernetes.io/os",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"linux"},
								},
							},
						},
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/arch",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"amd64", "arm64"},
								},
								{
									Key:      "kubernetes.io/os",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"linux"},
								},
							},
						},
					},
				},
			},
		},
		Volumes: prepareVolumes(instance),
	}

	if instance.GetObjectKind().GroupVersionKind().Version == dynatracev1alpha1.SchemeGroupVersion.Version {
		ps, err := preparePodSpecV1(p, instance)
		if err != nil {
			logger.Error(err, "failed to prepare pod spec v1")
		}
		p = ps
	} else if instance.GetObjectKind().GroupVersionKind().Version == dynatracev1alpha2.SchemeGroupVersion.Version {
		ps, err := preparePodSpecV2(p, instance)
		if err != nil {
			logger.Error(err, "failed to prepare pod spec v2")
		}
		p = ps
	}

	return p
}

func preparePodSpecV1(p corev1.PodSpec, instance dynatracev1alpha1.BaseOneAgentDaemonSet) (corev1.PodSpec, error) {
	var img string
	envVarImg := os.Getenv("RELATED_IMAGE_DYNATRACE_ONEAGENT")

	if instance.GetOneAgentSpec().Image != "" {
		img = instance.GetOneAgentSpec().Image
	} else if envVarImg != "" {
		img = envVarImg
	}

	p.Containers[0].Image = img
	p.Containers[0].Env = prepareEnvVars(instance)
	return p, nil
}

func preparePodSpecV2(p corev1.PodSpec, instance dynatracev1alpha1.BaseOneAgentDaemonSet) (corev1.PodSpec, error) {
	p.ImagePullSecrets = append(p.ImagePullSecrets, corev1.LocalObjectReference{
		Name: instance.GetName() + "-pull-secret",
	},
	)

	i, err := utils.BuildOneAgentImage(instance.GetSpec().APIURL, instance.(*dynatracev1alpha2.OneAgent).Spec.AgentVersion)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	p.Containers[0].Image = i

	return p, nil
}

func prepareVolumes(instance dynatracev1alpha1.BaseOneAgentDaemonSet) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: "host-root",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/",
				},
			},
		},
	}

	if instance.GetOneAgentSpec().TrustedCAs != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "certs",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: instance.GetOneAgentSpec().TrustedCAs,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  "certs",
							Path: "certs.pem",
						},
					},
				},
			},
		})
	}

	return volumes
}

func prepareVolumeMounts(instance dynatracev1alpha1.BaseOneAgentDaemonSet) []corev1.VolumeMount {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "host-root",
			MountPath: "/mnt/root",
		},
	}

	if instance.GetOneAgentSpec().TrustedCAs != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "certs",
			MountPath: "/mnt/dynatrace/certs",
		})
	}

	return volumeMounts
}

func prepareEnvVars(instance dynatracev1alpha1.BaseOneAgentDaemonSet) []corev1.EnvVar {
	var token, installerURL, skipCert, proxy *corev1.EnvVar

	reserved := map[string]**corev1.EnvVar{
		"ONEAGENT_INSTALLER_TOKEN":           &token,
		"ONEAGENT_INSTALLER_SCRIPT_URL":      &installerURL,
		"ONEAGENT_INSTALLER_SKIP_CERT_CHECK": &skipCert,
		"https_proxy":                        &proxy,
	}

	var envVars []corev1.EnvVar

	for i := range instance.GetOneAgentSpec().Env {
		if p := reserved[instance.GetOneAgentSpec().Env[i].Name]; p != nil {
			*p = &instance.GetOneAgentSpec().Env[i]
			continue
		}
		envVars = append(envVars, instance.GetOneAgentSpec().Env[i])
	}

	if token == nil {
		token = &corev1.EnvVar{
			Name: "ONEAGENT_INSTALLER_TOKEN",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: utils.GetTokensName(instance)},
					Key:                  utils.DynatracePaasToken,
				},
			},
		}
	}

	if installerURL == nil {
		installerURL = &corev1.EnvVar{
			Name:  "ONEAGENT_INSTALLER_SCRIPT_URL",
			Value: fmt.Sprintf("%s/v1/deployment/installer/agent/unix/default/latest?Api-Token=$(ONEAGENT_INSTALLER_TOKEN)&arch=x86&flavor=default", instance.GetOneAgentSpec().APIURL),
		}
	}

	if skipCert == nil {
		skipCert = &corev1.EnvVar{
			Name:  "ONEAGENT_INSTALLER_SKIP_CERT_CHECK",
			Value: strconv.FormatBool(instance.GetOneAgentSpec().SkipCertCheck),
		}
	}

	env := []corev1.EnvVar{*token, *installerURL, *skipCert}

	if proxy == nil {
		if instance.GetOneAgentSpec().Proxy != nil {
			if instance.GetOneAgentSpec().Proxy.ValueFrom != "" {
				env = append(env, corev1.EnvVar{
					Name: "https_proxy",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: instance.GetOneAgentSpec().Proxy.ValueFrom},
							Key:                  "proxy",
						},
					},
				})
			} else if instance.GetOneAgentSpec().Proxy.Value != "" {
				env = append(env, corev1.EnvVar{
					Name:  "https_proxy",
					Value: instance.GetOneAgentSpec().Proxy.Value,
				})
			}
		}
	} else {
		env = append(env, *proxy)
	}

	return append(env, envVars...)
}

// deletePods deletes a list of pods
//
// Returns an error in the following conditions:
//  - failure on object deletion
//  - timeout on waiting for ready state
func DeletePods(logger logr.Logger, pods []corev1.Pod, labels map[string]string, waitSecs uint16, c client.Client) error {
	for _, pod := range pods {
		logger.Info("deleting pod", "pod", pod.Name, "node", pod.Spec.NodeName)

		// delete pod
		err := c.Delete(context.TODO(), &pod)
		if err != nil {
			return err
		}

		logger.Info("waiting until pod is ready on node", "node", pod.Spec.NodeName)

		// wait for pod on node to get "Running" again
		if err := waitPodReadyState(pod, labels, waitSecs, c); err != nil {
			return err
		}

		logger.Info("pod recreated successfully on node", "node", pod.Spec.NodeName)
	}

	return nil
}

func waitPodReadyState(pod corev1.Pod, labels map[string]string, waitSecs uint16, c client.Client) error {
	var status error

	listOps := []client.ListOption{
		client.InNamespace(pod.Namespace),
		client.MatchingLabels(labels),
	}

	for splay := uint16(0); splay < waitSecs; splay += splayTimeSeconds {
		time.Sleep(time.Duration(splayTimeSeconds) * time.Second)

		// The actual selector we need is,
		// "spec.nodeName=<pod.Spec.NodeName>,status.phase=Running,metadata.name!=<pod.Name>"
		//
		// However, the client falls back to a cached implementation for .List() after the first attempt, which
		// is not able to handle our query so the function fails. Because of this, we're getting all the pods and
		// filtering it ourselves.
		podList := &corev1.PodList{}
		status = c.List(context.TODO(), podList, listOps...)
		if status != nil {
			continue
		}

		var foundPods []*corev1.Pod
		for i := range podList.Items {
			p := &podList.Items[i]
			if p.Spec.NodeName != pod.Spec.NodeName || p.Status.Phase != corev1.PodRunning ||
				p.ObjectMeta.Name == pod.Name {
				continue
			}
			foundPods = append(foundPods, p)
		}

		if n := len(foundPods); n == 0 {
			status = fmt.Errorf("waiting for pod to be recreated on node: %s", pod.Spec.NodeName)
		} else if n == 1 && getPodReadyState(foundPods[0]) {
			break
		} else if n > 1 {
			status = fmt.Errorf("too many pods found: expected=1 actual=%d", n)
		}
	}

	return status
}
