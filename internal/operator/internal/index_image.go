// Copyright 2020 The Operator-SDK Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	internalregistry "github.com/operator-framework/operator-sdk/internal/olm/operator/internal"
	"github.com/operator-framework/operator-sdk/internal/operator"
	registryutil "github.com/operator-framework/operator-sdk/internal/registry"
)

const (
	defaultSourceType = "grpc"
)

type IndexImageCatalogCreator struct {
	IndexImage       string
	InjectBundles    []string
	InjectBundleMode string
	BundleImage      string

	cfg *operator.Configuration
}

func NewIndexImageCatalogCreator(cfg *operator.Configuration) *IndexImageCatalogCreator {
	return &IndexImageCatalogCreator{
		cfg: cfg,
	}
}

func (c IndexImageCatalogCreator) newCatalogSource(ctx context.Context, name string) *v1alpha1.CatalogSource {
	// Create barebones catalog source
	return &v1alpha1.CatalogSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.cfg.Namespace,
		},
		Spec: v1alpha1.CatalogSourceSpec{
			DisplayName: "catalogsource",
			Publisher:   "operator-sdk",
		},
	}
}

func (c IndexImageCatalogCreator) createRegistryPod(ctx context.Context, dbPath string) (*internalregistry.RegistryPod, error) {
	// Create registry pod, assigning its owner as the catalog source
	registryPod, err := internalregistry.NewRegistryPod(c.cfg.Client, dbPath, c.BundleImage, c.cfg.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error in initializing registry pod")
	}

	if err = registryPod.Create(ctx); err != nil {
		return nil, fmt.Errorf("error in creating registry pod")
	}

	return registryPod, nil
}

func (c IndexImageCatalogCreator) CreateCatalog(ctx context.Context, name string) (*v1alpha1.CatalogSource, error) {
	dbPath, err := c.getDBPath(ctx)
	if err != nil {
		return nil, fmt.Errorf("get database path: %v", err)
	}

	fmt.Printf("IndexImageCatalogCreator.IndexImage:        %q\n", c.IndexImage)
	fmt.Printf("IndexImageCatalogCreator.IndexImageDBPath:  %v\n", dbPath)
	fmt.Printf("IndexImageCatalogCreator.InjectBundles:     %q\n", strings.Join(c.InjectBundles, ","))
	fmt.Printf("IndexImageCatalogCreator.InjectBundleMode:  %q\n", c.InjectBundleMode)

	cs := c.newCatalogSource(ctx, name)

	registryPod, err := c.createRegistryPod(ctx, dbPath)

	pod, err := registryPod.GetPod()
	if err != nil {
		return nil, fmt.Errorf("unable to get registry pod: %v", err)
	}

	if err := controllerutil.SetOwnerReference(cs, pod, c.cfg.Scheme); err != nil {
		return nil, fmt.Errorf("unable to set registry pod owner reference: %v", err)
	}

	// Wait for registry pod to be ready
	if err := registryPod.VerifyPodRunning(ctx); err != nil {
		return nil, fmt.Errorf("registry pod is not running: %v", err)
	}

	// Update catalog source with `spec.Address = pod.status.podIP`
	cs.Spec.SourceType = defaultSourceType
	cs.Spec.Address = fmt.Sprintf("%s:%v", pod.Status.PodIP, internalregistry.DefaultGRPCPort)

	// Update catalog source with annotations for index image,
	// injected bundle, and registry add mode
	injectedBundlesJSON, err := json.Marshal(c.InjectBundles)
	if err != nil {
		return nil, fmt.Errorf("error in json marshal injected bundles: %v", err)
	}
	cs.ObjectMeta.Annotations = map[string]string{
		"operators.operatorframework.io/index-image":        c.IndexImage,
		"operators.operatorframework.io/inject-bundle-mode": c.InjectBundleMode,
		"operators.operatorframework.io/injected-bundles":   string(injectedBundlesJSON),
	}

	// Wait for catalog source status to indicate a successful
	// connection with the registry pod
	csKey := types.NamespacedName{
		Namespace: c.cfg.Namespace,
		Name:      name,
	}

	if err := wait.PollImmediateUntil(time.Millisecond*200, func() (bool, error) {
		if err := c.cfg.Client.Get(ctx, csKey, cs); err != nil {
			return false, err
		}
		if cs.Status.GRPCConnectionState != nil {
			if cs.Status.GRPCConnectionState.LastObservedState == "READY" {
				return true, nil
			}
		}
		return false, nil
	}, ctx.Done()); err != nil {
		return nil, fmt.Errorf("catalog source connection not ready: %v", err)
	}

	// Return the catalog source
	return cs, nil
}

const defaultDBPath = "/database/index.db"

func (c IndexImageCatalogCreator) getDBPath(ctx context.Context) (string, error) {
	labels, err := registryutil.GetImageLabels(ctx, nil, c.IndexImage, false)
	if err != nil {
		return "", fmt.Errorf("get index image labels: %v", err)
	}
	if dbPath, ok := labels["operators.operatorframework.io.index.database.v1"]; ok {
		return dbPath, nil
	}
	return defaultDBPath, nil
}
