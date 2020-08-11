package oneagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	dynatracev1alpha1 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha1"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/istio"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/utils"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/dtclient"
	"github.com/go-logr/logr"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func (rec *reconciliation) Error(err error) bool {
	if err == nil {
		return false
	}
	rec.err = err
	return true
}

func (rec *reconciliation) Update(upd bool, d time.Duration, cause string) bool {
	if !upd {
		return false
	}
	rec.log.Info("Updating OneAgent CR", "cause", cause)
	rec.update = true
	rec.requeueAfter = d
	return true
}

// Add creates a new OneAgent Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, NewOneAgentReconciler(
		mgr.GetClient(),
		mgr.GetAPIReader(),
		mgr.GetScheme(),
		mgr.GetConfig(),
		log.Log.WithName("oneagent.controller"),
		utils.BuildDynatraceClient,
		&dynatracev1alpha1.OneAgent{}))
}

// NewOneAgentReconciler initialises a new ReconcileOneAgent instance
func NewOneAgentReconciler(client client.Client, apiReader client.Reader, scheme *runtime.Scheme, config *rest.Config, logger logr.Logger,
	dtcFunc utils.DynatraceClientFunc, instance dynatracev1alpha1.BaseOneAgentDaemonSet) *ReconcileOneAgent {
	return &ReconcileOneAgent{
		client:    client,
		apiReader: apiReader,
		scheme:    scheme,
		config:    config,
		logger:    log.Log.WithName("oneagent.controller"),
		dtcReconciler: &utils.DynatraceClientReconciler{
			DynatraceClientFunc: dtcFunc,
			Client:              client,
			UpdatePaaSToken:     true,
			UpdateAPIToken:      true,
		},
		istioController: istio.NewController(config, scheme),
		instance:        instance,
	}
}

// add adds a new OneAgentController to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r *ReconcileOneAgent) error {
	// Create a new controller
	c, err := controller.New("oneagent-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource OneAgent
	err = c.Watch(&source.Kind{Type: &dynatracev1alpha1.OneAgent{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource DaemonSets and requeue the owner OneAgent
	err = c.Watch(&source.Kind{Type: &appsv1.DaemonSet{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &dynatracev1alpha1.OneAgent{},
	})
	if err != nil {
		return err
	}

	return nil
}

// ReconcileOneAgent reconciles a OneAgent object
type ReconcileOneAgent struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client    client.Client
	apiReader client.Reader
	scheme    *runtime.Scheme
	config    *rest.Config
	logger    logr.Logger

	dtcReconciler   *utils.DynatraceClientReconciler
	istioController *istio.Controller
	instance        dynatracev1alpha1.BaseOneAgentDaemonSet
}

// Reconcile reads that state of the cluster for a OneAgent object and makes changes based on the state read
// and what is in the OneAgent.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileOneAgent) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logger := r.logger.WithValues("namespace", request.Namespace, "name", request.Name)
	logger.Info("Reconciling OneAgent")

	instance := r.instance.DeepCopyObject().(dynatracev1alpha1.BaseOneAgentDaemonSet)

	// Using the apiReader, which does not use caching to prevent a possible race condition where an old version of
	// the OneAgent object is returned from the cache, but it has already been modified on the cluster side
	if err := r.apiReader.Get(context.Background(), request.NamespacedName, instance); k8serrors.IsNotFound(err) {
		// Request object not dsActual, could have been deleted after reconcile request.
		// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
		// Return and don't requeue
		return reconcile.Result{}, nil
	} else if err != nil {
		return reconcile.Result{}, err
	}

	rec := reconciliation{log: logger, instance: instance, requeueAfter: 30 * time.Minute}
	r.reconcileImpl(&rec)

	if rec.err != nil {
		if rec.update || instance.GetOneAgentStatus().SetPhaseOnError(rec.err) {
			if errClient := UpdateCR(instance, r.client); errClient != nil {
				return reconcile.Result{}, fmt.Errorf("failed to update CR after failure, original, %s, then: %w", rec.err, errClient)
			}
		}

		var serr dtclient.ServerError
		if ok := errors.As(rec.err, &serr); ok && serr.Code == http.StatusTooManyRequests {
			logger.Info("Request limit for Dynatrace API reached! Next reconcile in one minute")
			return reconcile.Result{RequeueAfter: 1 * time.Minute}, nil
		}

		return reconcile.Result{}, rec.err
	}

	if rec.update {
		if err := UpdateCR(instance, r.client); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: rec.requeueAfter}, nil
}

type reconciliation struct {
	log      logr.Logger
	instance dynatracev1alpha1.BaseOneAgentDaemonSet

	// If update is true, then changes on instance will be sent to the Kubernetes API.
	//
	// Additionally, if err is not nil, then the Reconciliation will fail with its value. Unless it's a Too Many
	// Requests HTTP error from the Dynatrace API, on which case, a reconciliation is requeued after one minute delay.
	//
	// If err is nil, then a reconciliation is requeued after requeueAfter.
	err          error
	update       bool
	requeueAfter time.Duration
}

func (r *ReconcileOneAgent) reconcileImpl(rec *reconciliation) {
	if err := Validate(rec.instance); rec.Error(err) {
		return
	}

	dtc, upd, err := r.dtcReconciler.Reconcile(context.Background(), rec.instance)
	rec.Update(upd, 5*time.Minute, "Token conditions updated")
	if rec.Error(err) {
		return
	}

	if rec.instance.GetOneAgentSpec().EnableIstio {
		if upd, err := r.istioController.ReconcileIstio(rec.instance, dtc); err != nil {
			// If there are errors log them, but move on.
			rec.log.Info("Istio: failed to reconcile objects", "error", err)
		} else if upd {
			rec.log.Info("Istio: objects updated")
			rec.requeueAfter = 30 * time.Second
			return
		}
	}

	upd, err = r.reconcileRollout(rec.log, rec.instance, dtc)
	if rec.Error(err) || rec.Update(upd, 5*time.Minute, "Rollout reconciled") {
		return
	}

	if rec.instance.GetOneAgentSpec().DisableAgentUpdate {
		rec.log.Info("Automatic oneagent update is disabled")
		return
	}

	upd, err = r.reconcileVersion(rec.log, rec.instance, dtc)
	if rec.Error(err) || rec.Update(upd, 5*time.Minute, "Versions reconciled") {
		return
	}

	// Finally we have to determine the correct non error phase
	if upd, err = DetermineOneAgentPhase(rec.instance, r.client); !rec.Error(err) {
		rec.Update(upd, 5*time.Minute, "Phase change")
	}
}

func (r *ReconcileOneAgent) reconcileRollout(logger logr.Logger, instance dynatracev1alpha1.BaseOneAgentDaemonSet, dtc dtclient.Client) (bool, error) {
	updateCR := false

	// Define a new DaemonSet object
	dsDesired, err := NewDaemonSetForCR(instance)
	if err != nil {
		return false, err
	}

	// Set OneAgent instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, dsDesired, r.scheme); err != nil {
		return false, err
	}

	// Check if this DaemonSet already exists
	dsActual := &appsv1.DaemonSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: dsDesired.Name, Namespace: dsDesired.Namespace}, dsActual)
	if err != nil && k8serrors.IsNotFound(err) {
		logger.Info("Creating new daemonset")
		if err = r.client.Create(context.TODO(), dsDesired); err != nil {
			return false, err
		}
	} else if err != nil {
		return false, err
	} else if HasDaemonSetChanged(dsDesired, dsActual) {
		logger.Info("Updating existing daemonset")
		if err = r.client.Update(context.TODO(), dsDesired); err != nil {
			return false, err
		}
	}

	if instance.GetOneAgentStatus().Version == "" {
		desired, err := dtc.GetLatestAgentVersion(dtclient.OsUnix, dtclient.InstallerTypeDefault)
		if err != nil {
			return updateCR, fmt.Errorf("failed to get desired version: %w", err)
		}

		logger.Info("Updating version on OneAgent instance")
		instance.GetOneAgentStatus().Version = desired
		instance.GetOneAgentStatus().SetPhase(dynatracev1alpha1.Deploying)
		updateCR = true
	}

	return updateCR, nil
}

func (r *ReconcileOneAgent) reconcileVersion(logger logr.Logger, instance dynatracev1alpha1.BaseOneAgentDaemonSet, dtc dtclient.Client) (bool, error) {
	updateCR := false

	// get desired version
	desired, err := dtc.GetLatestAgentVersion(dtclient.OsUnix, dtclient.InstallerTypeDefault)
	if err != nil {
		return false, fmt.Errorf("failed to get desired version: %w", err)
	} else if desired != "" && instance.GetOneAgentStatus().Version != desired {
		logger.Info("new version available", "actual", instance.GetOneAgentStatus().Version, "desired", desired)
		instance.GetOneAgentStatus().Version = desired
		updateCR = true
	}

	// query oneagent pods
	podList := &corev1.PodList{}
	listOps := []client.ListOption{
		client.InNamespace(instance.GetNamespace()),
		client.MatchingLabels(BuildLabels(instance.GetName())),
	}
	err = r.client.List(context.TODO(), podList, listOps...)
	if err != nil {
		logger.Error(err, "failed to list pods", "listops", listOps)
		return updateCR, err
	}

	// determine pods to restart
	podsToDelete, instances, err := getPodsToRestart(podList.Items, dtc, instance)
	if err != nil {
		return updateCR, err
	}

	// Workaround: 'instances' can be null, making DeepEqual() return false when comparing against an empty map instance.
	// So, compare as long there is data.
	if (len(instances) > 0 || len(instance.GetOneAgentStatus().Instances) > 0) && !reflect.DeepEqual(instances, instance.GetOneAgentStatus().Instances) {
		logger.Info("oneagent pod instances changed", "status", instance.GetOneAgentStatus())
		updateCR = true
		instance.GetOneAgentStatus().Instances = instances
	}

	var waitSecs uint16 = 300
	if instance.GetOneAgentSpec().WaitReadySeconds != nil {
		waitSecs = *instance.GetOneAgentSpec().WaitReadySeconds
	}

	if len(podsToDelete) > 0 {
		if instance.GetOneAgentStatus().SetPhase(dynatracev1alpha1.Deploying) {
			err := UpdateCR(instance, r.client)
			if err != nil {
				logger.Error(err, fmt.Sprintf("failed to set phase to %s", dynatracev1alpha1.Deploying))
			}
		}
	}

	// restart daemonset
	err = DeletePods(logger, podsToDelete, BuildLabels(instance.GetName()), waitSecs, r.client)
	if err != nil {
		logger.Error(err, "failed to update version")
		return updateCR, err
	}

	return updateCR, nil
}
