package controller

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretController implements a controller for secrets.
type SecretController struct {
	client.Client
	Scheme *runtime.Scheme
}

type origin struct {
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	Uid             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch

// Reconcile handles requests to reconcile secrets.
func (r *SecretController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var secret v1.Secret
	if err := r.Get(ctx, req.NamespacedName, &secret); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if _, found := secret.Annotations["kubed.appscode.com/origin"]; found {
		fmt.Printf("Skipping already managed secret: %s/%s\n", secret.Namespace, secret.Name)
		// This is already a managed secret, ignore
		return ctrl.Result{}, nil
	}

	annotationValue, found := secret.Annotations["kubed.appscode.com/sync"]
	if !found {
		return ctrl.Result{}, nil
	}

	parts := strings.SplitN(annotationValue, "=", 2)
	if len(parts) != 2 {
		return ctrl.Result{}, nil
	}

	labelSelector := map[string]string{
		parts[0]: parts[1],
	}

	namespaceList := &v1.NamespaceList{}
	err := r.Client.List(ctx, namespaceList, client.MatchingLabels(labelSelector))
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(namespaceList.Items) != 1 {
		fmt.Printf("Expected exactly one namespace to match label selector %v, but got %v\n", labelSelector, namespaceList.Items)

		return ctrl.Result{}, nil
	}

	// list namespaces which match the labelselector in annotationValue

	newNamespace := namespaceList.Items[0].Name
	newSecretName := secret.Name

	fmt.Printf("Triggering sync for secret %s/%s\n", secret.Namespace, secret.Name)

	if err := r.copySecret(ctx, &secret, newNamespace, newSecretName); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SecretController) copySecret(ctx context.Context, secret *v1.Secret, newNamespace string, newSecretName string) error {
	var newSecret v1.Secret
	newSecretKey := types.NamespacedName{Name: newSecretName, Namespace: newNamespace}
	err := r.Get(ctx, newSecretKey, &newSecret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	secretExists := !errors.IsNotFound(err)

	newSecretData := make(map[string][]byte)
	for key, value := range secret.Data {
		newSecretData[key] = value
	}

	if secretExists {
		newSecret.Data = newSecretData

		existingOrigin, exists := newSecret.Annotations["kubed.appscode.com/origin"]
		if exists {
			var originObj origin
			if err := json.Unmarshal([]byte(existingOrigin), &originObj); err != nil {
				return err
			}

			if secret.Namespace == newSecret.Namespace && secret.Name == newSecret.Name {
				fmt.Printf("Ignoring circular sync for secret %s/%s to %s/%s\n", secret.Namespace, secret.Name, newNamespace, newSecretName)
				// This is a circular sync, ignore
				return nil
			}

			originVersion, err := strconv.ParseInt(originObj.ResourceVersion, 10, 64)
			if nil != err {
				return err
			}
			secretVersion, err := strconv.ParseInt(originObj.ResourceVersion, 10, 64)
			if nil != err {
				return err
			}

			if originVersion >= secretVersion {
				fmt.Printf("Ignoring outdated sync for secret %s/%s to %s/%s. Copy version %d origin version %d\n", secret.Namespace, secret.Name, newNamespace, newSecretName, originVersion, secretVersion)
				// This has already been synced, ignore
				return nil
			}

		}
	} else {
		newSecret = v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      newSecretName,
				Namespace: newNamespace,
			},
			Type: secret.Type,
			Data: newSecretData,
		}
	}

	originObj := origin{
		Namespace:       secret.Namespace,
		Name:            secret.Name,
		Uid:             string(secret.UID),
		ResourceVersion: secret.ResourceVersion,
	}
	originBytes, err := json.Marshal(originObj)
	if nil == newSecret.ObjectMeta.Annotations {
		newSecret.ObjectMeta.Annotations = make(map[string]string)
	}
	if nil == newSecret.ObjectMeta.Labels {
		newSecret.ObjectMeta.Labels = make(map[string]string)
	}
	newSecret.ObjectMeta.Annotations["kubed.appscode.com/origin"] = string(originBytes)
	newSecret.ObjectMeta.Labels["kubed.appscode.com/origin.cluster"] = "unicorn"
	newSecret.ObjectMeta.Labels["kubed.appscode.com/origin.name"] = secret.Name
	newSecret.ObjectMeta.Labels["kubed.appscode.com/origin.namespace"] = secret.Namespace

	if secretExists {
		fmt.Printf("Updating existing secret: %s/%s\n", newSecret.Namespace, newSecret.Name)

		return r.Client.Update(ctx, &newSecret)
	}

	fmt.Printf("Creating new secret: %s/%s\n", newSecret.Namespace, newSecret.Name)

	return r.Client.Create(ctx, &newSecret)
}

// SetupWithManager sets up the controller with a manager.
func (r *SecretController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.Secret{}).
		Complete(r)
}
