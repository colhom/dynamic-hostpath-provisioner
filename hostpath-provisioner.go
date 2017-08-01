/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
)

const (
	resyncPeriod              = 15 * time.Second
	provisionerName           = "coreos.com/hostpath-provisioner"
	identityKey               = "hostPathProvisionerIdentity"
	exponentialBackOffOnError = false
	failedRetryThreshold      = 60
	leasePeriod               = controller.DefaultLeaseDuration
	retryPeriod               = 1 * time.Second
	renewDeadline             = controller.DefaultRenewDeadline
	termLimit                 = controller.DefaultTermLimit
	storageClassAnno          = "volume.beta.kubernetes.io/storage-class"
	pvMaxClassField           = "maxVolumesPerHost"
)

type hostPathProvisioner struct {
	// The directory to create PV-backing directories in.
	// The should be where the storage location appears on this container's filesystem
	pvDir string

	// The base directory from which PV-backing directories should be exposed to in PV metadata.
	// This should appear where the storage location appears on the host's filesystem
	pvHostpathDir string

	// Identity of this hostPathProvisioner, set to node's name. Used to identify
	// "this" provisioner's PVs.
	identity string

	// Either Recycle, Delete, or Retain
	// https://godoc.org/k8s.io/api/core/v1#PersistentVolumeReclaimPolicy
	reclaimPolicy v1.PersistentVolumeReclaimPolicy

	clientSet *kubernetes.Clientset
}

var _ controller.Provisioner = &hostPathProvisioner{}

// Provision creates a storage asset and returns a PV object representing it.
func (p *hostPathProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVName == "" {
		return nil, fmt.Errorf("VolumeOptions.PVName is not set")
	}

	labels := map[string]string{}
	if err := setSchedulerLabels(p.identity, labels); err != nil {
		return nil, err
	}

	var storageClass string
	if options.PVC.Spec.StorageClassName != nil && *options.PVC.Spec.StorageClassName != "" {
		storageClass = *options.PVC.Spec.StorageClassName
	} else {
		storageClass = options.PVC.Annotations[storageClassAnno]
	}

	if options.Parameters == nil || options.Parameters[pvMaxClassField] == "" {
		return nil, fmt.Errorf("storage class %s must include '%s' parameter", storageClass, pvMaxClassField)
	}

	pvMaxCount, err := strconv.Atoi(options.Parameters[pvMaxClassField])
	if err != nil || pvMaxCount <= 0 {
		return nil, fmt.Errorf("storage class %s parameter %s must be a positive integer: %v", storageClass, pvMaxClassField, err)
	}

	labels[storageClassAnno] = storageClass

	labelSelector := &metav1.LabelSelector{
		MatchLabels: labels,
	}

	pvList, err := p.clientSet.Core().PersistentVolumes().List(metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(labelSelector),
	})

	if err != nil {
		return nil, fmt.Errorf("error listing existing persistent volumes: %v", err)
	}

	pvCount := len(pvList.Items)
	glog.Infof("-> Found %d existing Persistent Volumes for storage class %s", pvCount, storageClass)

	if pvCount >= pvMaxCount {
		return nil, fmt.Errorf("provisioner %s has reached max PV count (%d of %d) for storage class %s",
			p.identity,
			pvCount,
			pvMaxCount,
			storageClass)
	}

	localPath := path.Join(p.pvDir, options.PVName)
	if err := os.MkdirAll(localPath, 0777); err != nil {
		return nil, err
	}
	pvPath := path.Join(p.pvHostpathDir, options.PVName)
	glog.Infof("-> Backing pv/%s with host directory %s", options.PVName, p.pvHostpathDir)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				identityKey:      p.identity,
				storageClassAnno: storageClass,
			},
			Labels: labels,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: p.reclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: pvPath,
				},
			},
			StorageClassName: storageClass,
		},
	}

	return pv, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *hostPathProvisioner) Delete(volume *v1.PersistentVolume) error {
	ann, ok := volume.Annotations[identityKey]
	if !ok {
		return fmt.Errorf("identity annotation key %s not found on PV", identityKey)
	}
	if ann != p.identity {
		return &controller.IgnoredError{Reason: "identity annotation on PV does not match ours"}
	}

	path := path.Join(p.pvDir, volume.Name)
	if err := os.RemoveAll(path); err != nil {
		return err
	}

	return nil
}

func setSchedulerLabels(identity string, labels map[string]string) error {
	regionLabel := "failure-domain.beta.kubernetes.io/region"
	regionVal := "hostpath"
	zoneLabel := "failure-domain.beta.kubernetes.io/zone"
	zoneVal := fmt.Sprintf("hostpath-%s", identity)
	if labels[regionLabel] == "" {
		labels[regionLabel] = regionVal
	}

	if labels[zoneLabel] == "" {
		labels[zoneLabel] = zoneVal
	}

	if labels[regionLabel] != regionVal {
		return fmt.Errorf("Conflicting node label: %s=%s", regionLabel, regionVal)
	}

	if labels[zoneLabel] != zoneVal {
		return fmt.Errorf("Conflicting node label: %s=%s", zoneLabel, zoneVal)
	}

	return nil
}

func setNodeLabels(identity string, cs *kubernetes.Clientset) error {
	nodes := cs.Core().Nodes()
	node, err := nodes.Get(identity, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	if err := setSchedulerLabels(identity, node.Labels); err != nil {
		return err
	}
	_, err = nodes.Update(node)
	return err
}

func main() {
	syscall.Umask(0)

	flag.Parse()
	flag.Set("logtostderr", "true")

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		glog.Fatal("env variable NODE_NAME must be set so that this provisioner can identify itself")
	}

	pvDir := os.Getenv("PV_DIR")
	if pvDir == "" {
		glog.Fatal("env variable PV_DIR must be set")
	}

	pvHostpathDir := os.Getenv("PV_HOSTPATH_DIR")
	if pvDir == "" {
		glog.Fatal("env variable PV_HOSTPATH_DIR must be set")
	}

	reclaimPolicy := v1.PersistentVolumeReclaimPolicy(os.Getenv("RECLAIM_POLICY"))
	if reclaimPolicy == "" {
		reclaimPolicy = v1.PersistentVolumeReclaimDelete
		glog.Warningf("env RECLAIM_POLICY not specified, defaulting to %s", reclaimPolicy)
	}
	reclaimValid := false
	validVals := []v1.PersistentVolumeReclaimPolicy{
		v1.PersistentVolumeReclaimDelete,
		v1.PersistentVolumeReclaimRecycle,
		v1.PersistentVolumeReclaimRetain,
	}

	for _, v := range validVals {
		if reclaimPolicy == v {
			reclaimValid = true
		}
	}

	if !reclaimValid {
		glog.Fatalf("RECLAIM_POLICY %s is not valid. Must be one of %v", reclaimPolicy, validVals)
	}

	if err := setNodeLabels(nodeName, clientset); err != nil {
		glog.Fatalf("Error setting node labels: %v", err)
	}

	hostPathProvisioner := &hostPathProvisioner{
		pvDir:         pvDir,
		pvHostpathDir: pvHostpathDir,
		identity:      nodeName,
		reclaimPolicy: reclaimPolicy,
		clientSet:     clientset,
	}

	// Start the provision controller which will dynamically provision hostPath
	// PVs
	pc := controller.NewProvisionController(clientset, resyncPeriod, provisionerName, hostPathProvisioner, serverVersion.GitVersion, exponentialBackOffOnError, failedRetryThreshold, leasePeriod, renewDeadline, retryPeriod, termLimit)
	pc.Run(wait.NeverStop)
}
