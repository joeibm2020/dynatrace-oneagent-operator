package oneagent_v1alpha2

import (
	"context"
	"errors"
	"fmt"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/oneagent"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"time"

	dynatracev1alpha1 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha1"
	dynatracev1alpha2 "github.com/Dynatrace/dynatrace-oneagent-operator/pkg/apis/dynatrace/v1alpha2"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/istio"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/controller/utils"
	"github.com/Dynatrace/dynatrace-oneagent-operator/pkg/dtclient"
	"github.com/go-logr/logr"

	appsv1 "k8s.io/api/apps/v1"
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
		&dynatracev1alpha2.OneAgent{}))
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
	err = c.Watch(&source.Kind{Type: &dynatracev1alpha2.OneAgent{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource DaemonSets and requeue the owner OneAgent
	err = c.Watch(&source.Kind{Type: &appsv1.DaemonSet{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &dynatracev1alpha2.OneAgent{},
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

	instance := r.instance.DeepCopyObject().(*dynatracev1alpha2.OneAgent)

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
			if errClient := oneagent.UpdateCR(instance, r.client); errClient != nil {
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
		if err := oneagent.UpdateCR(instance, r.client); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: rec.requeueAfter}, nil
}

type reconciliation struct {
	log      logr.Logger
	instance *dynatracev1alpha2.OneAgent

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
	if err := oneagent.Validate(rec.instance); rec.Error(err) {
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

	err = r.ReconcilePullSecret(rec.instance, rec.log)
	if rec.Error(err) {
		return
	}

	upd, err = r.reconcileRollout(rec.log, *rec.instance)
	if rec.Error(err) || rec.Update(upd, 5*time.Minute, "Rollout reconciled") {
		return
	}

	if rec.instance.GetOneAgentSpec().DisableAgentUpdate {
		rec.log.Info("Automatic oneagent update is disabled")
		return
	}

	upd, err = r.updatePods(rec.instance)
	if rec.Error(err) || rec.Update(upd, 5*time.Minute, "Versions reconciled") {
		return
	}

	// Finally we have to determine the correct non error phase
	if upd, err = oneagent.DetermineOneAgentPhase(rec.instance, r.client); !rec.Error(err) {
		rec.Update(upd, 5*time.Minute, "Phase change")
	}
}

func (r *ReconcileOneAgent) reconcileRollout(logger logr.Logger, instance dynatracev1alpha2.OneAgent) (bool, error) {
	updateCR := false

	// Define a new DaemonSet object
	dsDesired, err := oneagent.NewDaemonSetForCR(&instance, logger)
	if err != nil {
		return false, err
	}

	// Set OneAgent instance as the owner and controller
	if err := controllerutil.SetControllerReference(&instance, dsDesired, r.scheme); err != nil {
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
	} else if oneagent.HasDaemonSetChanged(dsDesired, dsActual) {
		logger.Info("Updating existing daemonset")
		if err = r.client.Update(context.TODO(), dsDesired); err != nil {
			return false, err
		}
	}

	if instance.GetOneAgentStatus().Version == "" {
		logger.Info("Updating version on OneAgent instance")

		if instance.Spec.AgentVersion == "" {
			instance.GetOneAgentStatus().Version = "latest"
		} else {
			instance.GetOneAgentStatus().Version = instance.Spec.AgentVersion
		}

		instance.GetOneAgentStatus().SetPhase(dynatracev1alpha1.Deploying)
		updateCR = true
	}

	return updateCR, nil
}

func (r *ReconcileOneAgent) ReconcilePullSecret(instance *dynatracev1alpha2.OneAgent, log logr.Logger) error {
	var tkns corev1.Secret
	if err := r.client.Get(context.TODO(), client.ObjectKey{Name: utils.GetTokensName(instance), Namespace: instance.Namespace}, &tkns); err != nil {
		return fmt.Errorf("failed to query tokens: %w", err)
	}
	pullSecretData, err := utils.GeneratePullSecretData(r.client, instance, &tkns)
	if err != nil {
		return fmt.Errorf("failed to generate pull secret data: %w", err)
	}
	err = utils.CreateOrUpdateSecretIfNotExists(r.client, r.client, instance.Name+"-pull-secret", instance.Namespace, pullSecretData, corev1.SecretTypeDockerConfigJson, log)
	if err != nil {
		return fmt.Errorf("failed to create or update secret: %w", err)
	}

	return nil
}
