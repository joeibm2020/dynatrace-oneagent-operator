package oneagent_v1alpha2

import (
	"context"
	dynatracev1alpha2 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha2"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/oneagent"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/parser"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/version"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const (
	UpdateInterval = 5 * time.Minute
)

func (r *ReconcileOneAgent) updatePods(instance *dynatracev1alpha2.OneAgent) (bool, error) {
	var waitSecs uint16 = 300
	if !instance.Spec.DisableAgentUpdate &&
		instance.Status.UpdatedTimestamp.Add(UpdateInterval).Before(time.Now()) {
		r.logger.Info("checking for outdated pods")
		// Check if pods have latest agent version
		outdatedPods, err := r.findOutdatedPods(r.logger, instance, isLatest)
		if err != nil {
			return false, err
		}

		err = oneagent.DeletePods(r.logger, outdatedPods, oneagent.BuildLabels(instance.GetName()), waitSecs, r.client)
		if err != nil {
			r.logger.Error(err, err.Error())
			return false, err
		}
		instance.Status.UpdatedTimestamp = metav1.Now()
		err = r.client.Status().Update(context.TODO(), instance)
		if err != nil {
			r.logger.Info("failed to updated instance status", "message", err.Error())
		}
	} else if instance.Spec.DisableAgentUpdate {
		r.logger.Info("Skipping updating pods because of configuration", "disableOneAgentUpdate", true)
	}
	return true, nil
}

func (r *ReconcileOneAgent) findOutdatedPods(
	logger logr.Logger,
	instance *dynatracev1alpha2.OneAgent,
	isLatestFn func(logr.Logger, *corev1.ContainerStatus, *corev1.Secret) (bool, error)) ([]corev1.Pod, error) {
	pods, err := r.findPods(instance)
	if err != nil {
		logger.Error(err, "failed to list pods")
		return nil, err
	}

	var outdatedPods []corev1.Pod
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Image == "" {
				// If image is not yet pulled skip check
				continue
			}
			logger.Info("pods container status", "pod", pod.Name, "container", status.Name, "image id", status.ImageID)

			imagePullSecret := &corev1.Secret{}
			err := r.client.Get(context.TODO(), client.ObjectKey{Namespace: pod.Namespace, Name: instance.Name + "-pull-secret"}, imagePullSecret)
			if err != nil {
				logger.Error(err, err.Error())
			}

			isLatest, err := isLatestFn(logger, &status, imagePullSecret)
			if err != nil {
				logger.Error(err, err.Error())
				//Error during image check, do nothing an continue with next status
				continue
			}

			if !isLatest {
				logger.Info("pod is outdated", "name", pod.Name)
				outdatedPods = append(outdatedPods, pod)
				// Pod is outdated, break loop
				break
			}
		}
	}

	return outdatedPods, nil
}

func isLatest(logger logr.Logger, status *corev1.ContainerStatus, imagePullSecret *corev1.Secret) (bool, error) {
	dockerConfig, err := parser.NewDockerConfig(imagePullSecret)
	if err != nil {
		logger.Info(err.Error())
	}

	dockerVersionChecker := version.NewDockerVersionChecker(status.Image, status.ImageID, dockerConfig)
	return dockerVersionChecker.IsLatest()
}

func (r *ReconcileOneAgent) findPods(instance *dynatracev1alpha2.OneAgent) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	listOptions := []client.ListOption{
		client.InNamespace(instance.GetNamespace()),
		client.MatchingLabels(oneagent.BuildLabels(instance.Name)),
	}
	err := r.client.List(context.TODO(), podList, listOptions...)
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}
