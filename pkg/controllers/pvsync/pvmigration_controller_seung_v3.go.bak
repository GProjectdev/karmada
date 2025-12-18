/*
Copyright 2025 Kwon MuSeong and Jeong SeungJun
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

package pvsync

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/controllers/ctrlutil"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
)

/* ============================ Constants & Types ============================ */

const (
	PVMigrationControllerName = "pv-migration-controller"

	// RB annotation contract (single source of truth)
	AnnPVMigration = "stwmig.karmada.io/pv-migration" // idle|processing|completed|error

	AnnFailedCluster   = "stwmig.karmada.io/failed-cluster"          // latched failed cluster name
	AnnDesiredHash     = "stwmig.karmada.io/pv-migration-desired-hash"     // hash of rb.spec.clusters at start
	AnnExpectedPVWorks = "stwmig.karmada.io/pv-migration-expected-pvworks" // expected pv-deployment work count

	// (레거시) 이전 키들 – 하위호환만 유지
	AnnRestore  = "stwmig.karmada.io/restore"   // (legacy) idle|processing|completed|error
	AnnOwnerSTM = "stwmig.karmada.io/owner-stm" // (legacy) <StatefulMigration .metadata.name>

	// ✅ MigrationRestore 컨트롤러가 쓰는 표준 키
	RestorePhaseAnno = "migration.dcnlab.com/restore-phase" // working|succeeded|failed

	// ✅ Restore 표식 대기용(레이스 방지)
	RestoreWaitStartAnno = "stwmig.karmada.io/restore-wait-start" // RFC3339
	RestoreWaitGrace     = 25 * time.Second                       // 8s 폴링 기준: ~3라운드
)

type PVMigrationController struct {
	client.Client
	EventRecorder record.EventRecorder
	RESTMapper    meta.RESTMapper
}

type pvTemplate struct {
	srcOrdinal int
	specYAML   string
}

// ---- helpers for SM existence/status based on annotations ----
func detectSMExistence(anns map[string]string) bool {
	if anns == nil {
		return false
	}
	// 새 플로우: restore-phase가 찍혀 있으면 SM 경로로 간주
	if _, ok := anns[RestorePhaseAnno]; ok {
		return true
	}
	// (레거시) owner-stm이 있거나 restore(completed 등) 키가 있으면 존재로 간주
	if anns[AnnOwnerSTM] != "" || anns[AnnRestore] != "" {
		return true
	}
	return false
}

func getRestorePhase(anns map[string]string) string {
	if anns == nil {
		return ""
	}
	// 새 키 우선
	if v := anns[RestorePhaseAnno]; v != "" {
		return v
	}
	// (레거시) restore=completed를 succeeded로 매핑
	if anns[AnnRestore] == "completed" {
		return "succeeded"
	}
	return ""
}

// desiredHash computes a stable hash of RB desired replicas per cluster.
// Used to detect "RB spec changed while pv-migration already completed" and restart pv-migration.
func desiredHash(rb *workv1alpha2.ResourceBinding) string {
	type item struct {
		Name     string
		Replicas int32
	}
	items := make([]item, 0, len(rb.Spec.Clusters))
	for _, c := range rb.Spec.Clusters {
		items = append(items, item{Name: c.Name, Replicas: c.Replicas})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	h := sha1.New()
	for _, it := range items {
		fmt.Fprintf(h, "%s=%d;", it.Name, it.Replicas)
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

/* ================================ Reconcile ================================ */

func (c *PVMigrationController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.Infof("[PVMigrationController] Reconciling ResourceBinding %s", req.NamespacedName)

	rb := &workv1alpha2.ResourceBinding{}
	if err := c.Client.Get(ctx, req.NamespacedName, rb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 대상: StatefulSet & 실패/페일오버 시 suspension on
	if rb.Spec.Resource.Kind != "StatefulSet" || rb.Spec.Resource.APIVersion != "apps/v1" {
		return ctrl.Result{}, nil
	}
	suspensionOn := rb.Spec.Suspension != nil && rb.Spec.Suspension.Dispatching != nil && *rb.Spec.Suspension.Dispatching
	if !suspensionOn {
		return ctrl.Result{}, nil
	}

	anns := rb.GetAnnotations()
	phase := ""
	if anns != nil {
		phase = anns[AnnPVMigration]
	}
	// restorePhase comes from MigrationRestore controller
	restorePhase := getRestorePhase(anns)

	// ✅ If pv-migration was completed for an older desired replica plan, restart pv-migration.
	currHash := desiredHash(rb)
	if phase == "completed" && anns != nil && anns[AnnDesiredHash] != "" && anns[AnnDesiredHash] != currHash {
		klog.Infof("RB desired replica plan changed (old=%s new=%s); restarting pv-migration for %s/%s",
			anns[AnnDesiredHash], currHash, rb.Namespace, rb.Name)
		_ = c.patchRBAnnotations(ctx, rb, map[string]string{
			AnnPVMigration: "processing",
			AnnDesiredHash: currHash,
		})
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// failed cluster selection: prefer latched annotation, otherwise pick first Unhealthy
	failedClusterName := ""
	if anns != nil {
		failedClusterName = anns[AnnFailedCluster]
	}
	if failedClusterName == "" {
		for _, s := range rb.Status.AggregatedStatus {
			if s.Health == "Unhealthy" {
				failedClusterName = s.ClusterName
				break
			}
		}
		if failedClusterName != "" {
			klog.Infof("[Matched RB] %s/%s: Unhealthy cluster detected: %s", rb.Namespace, rb.Name, failedClusterName)
			_ = c.patchRBAnnotations(ctx, rb, map[string]string{AnnFailedCluster: failedClusterName})
		}
	}

	// If we still don't know failed cluster:
	// - during processing: requeue (eviction might have already wiped Unhealthy)
	// - otherwise: skip
	if failedClusterName == "" {
		if phase == "processing" {
			klog.Infof("No failed cluster found for RB %s/%s (phase=processing); retrying", rb.Namespace, rb.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Mark pv-migration=processing (idempotent) and latch desired-hash
	if phase != "processing" {
		_ = c.patchRBAnnotations(ctx, rb, map[string]string{
			AnnPVMigration: "processing",
			AnnDesiredHash: currHash,
			AnnFailedCluster: failedClusterName,
		})
	}

	// If already completed and restore succeeded, unsuspend here (fast path).
	if phase == "completed" && restorePhase == "succeeded" {
		klog.Infof("Finalize fast-path: pv-migration=completed and restore-phase=succeeded; unsuspending RB %s/%s", rb.Namespace, rb.Name)
		fresh := &workv1alpha2.ResourceBinding{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(rb), fresh); err == nil {
			if err := c.unsuspendRB(ctx, fresh); err != nil {
				return ctrl.Result{RequeueAfter: 3 * time.Second}, err
			}
			_ = c.removeRBAnnotations(ctx, fresh, []string{AnnFailedCluster})
		}
		return ctrl.Result{}, nil
	}
// 5. PV 메타데이터 Work 수집 (원본/failure cluster 기준)
	stsKey := rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name
	workList := &workv1alpha1.WorkList{}
	if err := c.Client.List(ctx, workList, client.MatchingLabels{
		"pvsync.karmada.io/type":       "metadata",
		"pvsync.karmada.io/source-sts": stsKey,
		"pvsync.karmada.io/cluster":    failedClusterName,
	}); err != nil {
		klog.Errorf("Failed to list PV metadata Works for %s: %v", stsKey, err)
		_ = c.patchRBAnnotations(ctx, rb, map[string]string{AnnPVMigration: "error"})
		return ctrl.Result{}, err
	}

	// 6. 각 클러스터의 현재 PVC 수(existing)와 목표(desired) 비교
	desiredBy := map[string]int{}
	for _, tc := range rb.Spec.Clusters {
		desiredBy[tc.Name] = int(tc.Replicas)
	}
	existingBy := map[string]int{}
	prefixesAll := extractPrefixesFromMetaWork(workList, rb)
	if len(prefixesAll) == 0 {
		prefixesAll = []string{fmt.Sprintf("%s-%s-", "data", rb.Spec.Resource.Name)}
	}
	for name := range desiredBy {
		existSet, _, _ := c.scanPVCOrdinals(ctx, name, rb.Spec.Resource.Namespace, prefixesAll)
		existingBy[name] = len(existSet)
	}

	// surplus 계산
	surplus := map[string]int{}
	for name, d := range desiredBy {
		if existingBy[name] > d {
			surplus[name] = existingBy[name] - d
		}
	}

	needTotal := 0
	createdTotal := 0
	// 6-2. 대상 클러스터별 PV 생성
	for _, tc := range rb.Spec.Clusters {
		cluster := tc.Name
		desired := int(tc.Replicas)

		// allowedSources
		allowedSources := map[string]bool{}
		for src, n := range surplus {
			if src == cluster {
				continue
			}
			if n > 0 {
				allowedSources[src] = true
			}
		}
		if len(allowedSources) == 0 {
			for name := range desiredBy {
				if name != cluster {
					allowedSources[name] = true
				}
			}
		}

		// prefix/현재 PVC/부족분
		prefixes := extractPrefixesFromMetaWorkFiltered(workList, rb, allowedSources)
		if len(prefixes) == 0 {
			prefixes = prefixesAll
		}
		existSet, maxOrd, err := c.scanPVCOrdinals(ctx, cluster, rb.Spec.Resource.Namespace, prefixes)
		if err != nil {
			klog.Warningf("scanPVCOrdinals failed on %s: %v", cluster, err)
			continue
		}
		need := desired - len(existSet)
		if need > 0 {
			needTotal += need
		}
		if need <= 0 {
			klog.Infof("[Replica OK] cluster=%s desired=%d existing=%d -> skip PV creation", cluster, desired, len(existSet))
			continue
		}

		templates := collectSortedPVTemplatesFromMetaWorkFiltered(workList, rb.Spec.Resource.Namespace, rb.Spec.Resource.Name, allowedSources)
		if len(templates) == 0 {
			klog.Infof("No PV templates (metadata) for %s. Skip cluster %s.", stsKey, cluster)
			continue
		}

		start := maxOrd + 1
		created := 0
		tmplIdx := 0
		for created < need && tmplIdx < len(templates) {
			newPVC := fmt.Sprintf("%s%d", prefixes[tmplIdx%len(prefixes)], start+created)
			if err := c.createPVWorkWithPVCOverride(ctx, cluster, rb, newPVC, templates[tmplIdx].specYAML); err != nil {
				klog.Errorf("Failed to create PV for %s in %s: %v", newPVC, cluster, err)
			} else {
				klog.Infof("Ordinal map: src=%d -> dst=%d (PVC=%s)", templates[tmplIdx].srcOrdinal, start+created, newPVC)
				created++
				createdTotal++
			}
			tmplIdx++
		}

		klog.Infof("[PV creation] cluster=%s desired=%d existing=%d created=%d (start=%d)",
			cluster, desired, len(existSet), created, start)
	}

	// PV가 모두 준비되는지 확인하고, 준비되면 pv-migration=completed + (restore-phase 관찰) + unsuspend
	done, err := c.finalizeFailoverIfReady(ctx, rb, needTotal, createdTotal)
	if err != nil {
		klog.Errorf("❌ Failed to finalize failover for %s/%s: %v", rb.Namespace, rb.Name, err)
		_ = c.patchRBAnnotations(ctx, rb, map[string]string{AnnPVMigration: "error"})
		return ctrl.Result{}, err
	}
	if !done {
		// restore-phase(working/미설정) 등으로 대기 중 → 짧게 재큐
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

/* =========================== Helpers (cluster scan) =========================== */

// 대상 멤버 클러스터의 PVC를 조회해 prefix에 맞는 ordinal 집합과 최대값을 반환
func (c *PVMigrationController) scanPVCOrdinals(ctx context.Context, clusterName, ns string, prefixes []string) (map[int]bool, int, error) {
	dc, err := util.NewClusterDynamicClientSet(clusterName, c.Client)
	if err != nil {
		return nil, -1, err
	}
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	lst, err := dc.DynamicClientSet.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, -1, err
	}
	exist := map[int]bool{}
	maxOrd := -1
	for _, it := range lst.Items {
		name := it.GetName()
		matched := false
		for _, p := range prefixes {
			if strings.HasPrefix(name, p) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if n, ok := parseOrdinalFromClaimName(name); ok {
			exist[n] = true
			if n > maxOrd {
				maxOrd = n
			}
		}
	}
	return exist, maxOrd, nil
}

/* =========================== Work creation (PV) =========================== */

func (c *PVMigrationController) createPVWork(ctx context.Context, clusterName string, rb *workv1alpha2.ResourceBinding, pvName string, pvSpecYaml string) error {
	var fullpvSpec corev1.PersistentVolumeSpec
	if err := yaml.Unmarshal([]byte(pvSpecYaml), &fullpvSpec); err != nil {
		klog.Errorf("Failed to unmarshal PV spec YAML: %v", err)
		return err
	}
	var claimRef *corev1.ObjectReference
	if fullpvSpec.ClaimRef != nil {
		claimRef = &corev1.ObjectReference{
			APIVersion: fullpvSpec.ClaimRef.APIVersion,
			Kind:       fullpvSpec.ClaimRef.Kind,
			Name:       fullpvSpec.ClaimRef.Name,
			Namespace:  fullpvSpec.ClaimRef.Namespace,
		}
	}
	pvSpec := corev1.PersistentVolumeSpec{
		AccessModes:                   fullpvSpec.AccessModes,
		Capacity:                      fullpvSpec.Capacity,
		ClaimRef:                      claimRef,
		PersistentVolumeReclaimPolicy: fullpvSpec.PersistentVolumeReclaimPolicy,
		StorageClassName:              fullpvSpec.StorageClassName,
		VolumeMode:                    fullpvSpec.VolumeMode,
		PersistentVolumeSource:        corev1.PersistentVolumeSource{
			NFS: fullpvSpec.PersistentVolumeSource.NFS,
		},
	}
	workName := fmt.Sprintf("pv-work-%s-%s-%s", rb.Name, shortHash(pvName), clusterName)
	workNamespace := names.GenerateExecutionSpaceName(clusterName)

	// 이미 같은 Work가 있으면 skip
	existing := &workv1alpha1.Work{}
	if err := c.Client.Get(ctx, client.ObjectKey{Name: workName, Namespace: workNamespace}, existing); err == nil {
		klog.Infof("🔁 PV Work already exists for cluster %s. Skipping creation.", clusterName)
		return nil
	}

	// 이미 동일 PVC에 바인딩된 PV가 글로벌 클러스터에 존재하면 skip (best-effort)
	var existingPVs corev1.PersistentVolumeList
	if err := c.Client.List(ctx, &existingPVs); err == nil {
		for _, existingPV := range existingPVs.Items {
			if existingPV.Spec.ClaimRef != nil &&
				claimRef != nil &&
				existingPV.Spec.ClaimRef.Name == claimRef.Name &&
				existingPV.Spec.ClaimRef.Namespace == claimRef.Namespace {
				klog.Infof("🔁 PV for PVC %s/%s already exists. Skipping creation.", claimRef.Namespace, claimRef.Name)
				return nil
			}
		}
	}

	pvcName := ""
	if claimRef != nil {
		pvcName = claimRef.Name
	}
	generatedName := fmt.Sprintf("pv-%s-%s", pvcName, rb.Name)
	newPV := &corev1.PersistentVolume{
		TypeMeta:   metav1.TypeMeta{Kind: "PersistentVolume", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: generatedName},
		Spec:       pvSpec,
	}
	unstructuredPV, err := helper.ToUnstructured(newPV)
	if err != nil {
		klog.Errorf("Failed to convert PV to unstructured: %v", err)
		return err
	}
	workMeta := metav1.ObjectMeta{
		Name:      workName,
		Namespace: workNamespace,
		Labels: map[string]string{
			"pvsync.karmada.io/type":       "pv-deployment",
			"pvsync.karmada.io/source-sts": rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name,
			"pvsync.karmada.io/source-rb":  rb.Name,
			"pvsync.karmada.io/source-pv":  shortHash(pvName),
		},
	}
	if err := ctrlutil.CreateOrUpdateWork(ctx, c.Client, workMeta, unstructuredPV); err != nil {
		klog.Errorf("Failed to create PV Work for cluster %s: %v", clusterName, err)
		return err
	}
	klog.Infof("✅ Created PV Work %s for cluster %s using PV %s", workName, clusterName, newPV.Name)
	return nil
}

// ClaimRef.Name을 새 PVC로 치환하여 PV Work 생성
func (c *PVMigrationController) createPVWorkWithPVCOverride(ctx context.Context, clusterName string, rb *workv1alpha2.ResourceBinding, pvcName string, pvSpecYaml string) error {
	var full corev1.PersistentVolumeSpec
	if err := yaml.Unmarshal([]byte(pvSpecYaml), &full); err != nil {
		return err
	}
	if full.ClaimRef == nil {
		full.ClaimRef = &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim"}
	}
	full.ClaimRef.APIVersion = "v1"
	full.ClaimRef.Kind = "PersistentVolumeClaim"
	full.ClaimRef.Name = pvcName
	full.ClaimRef.Namespace = rb.Spec.Resource.Namespace
	full.ClaimRef.UID = ""
	full.ClaimRef.ResourceVersion = ""
	full.ClaimRef.FieldPath = ""

	pvSpec := corev1.PersistentVolumeSpec{
		AccessModes:                   full.AccessModes,
		Capacity:                      full.Capacity,
		ClaimRef:                      full.ClaimRef,
		PersistentVolumeReclaimPolicy: full.PersistentVolumeReclaimPolicy,
		StorageClassName:              full.StorageClassName,
		VolumeMode:                    full.VolumeMode,
		PersistentVolumeSource:        corev1.PersistentVolumeSource{NFS: full.PersistentVolumeSource.NFS},
	}
	pvObj := &corev1.PersistentVolume{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pv-%s-%s", pvcName, rb.Name)},
		Spec:       pvSpec,
	}
	unstr, err := helper.ToUnstructured(pvObj)
	if err != nil {
		return err
	}

	workName := fmt.Sprintf("pv-work-%s-%s-%s", rb.Name, shortHash(pvcName), clusterName)
	workNS := names.GenerateExecutionSpaceName(clusterName)
	meta := metav1.ObjectMeta{
		Name:      workName,
		Namespace: workNS,
		Labels: map[string]string{
			"pvsync.karmada.io/type":       "pv-deployment",
			"pvsync.karmada.io/source-sts": rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name,
			"pvsync.karmada.io/source-rb":  rb.Name,
			"pvsync.karmada.io/source-pv":  shortHash(pvcName),
		},
	}
	return ctrlutil.CreateOrUpdateWork(ctx, c.Client, meta, unstr)
}

/* ================= PV Ready → annotate completed → (maybe) unsuspend ================= */

// returns (done=true) when we actually unsuspended or no further waiting is needed
func (c *PVMigrationController) finalizeFailoverIfReady(ctx context.Context, rb *workv1alpha2.ResourceBinding, needTotal int, createdTotal int) (bool, error) {
	const retryInterval = 1 * time.Second

	// 1) PV Work들이 전부 Healthy(Available/Bound) 될 때까지 기다림
	for {
		pvWorkList := &workv1alpha1.WorkList{}
		if err := c.Client.List(ctx, pvWorkList, client.MatchingLabels{
			"pvsync.karmada.io/type":      "pv-deployment",
			"pvsync.karmada.io/source-rb": rb.Name,
		}); err != nil {
			return false, fmt.Errorf("failed to list PV Works: %w", err)
		}

		expectedCount := len(pvWorkList.Items)

		// ✅ If we *needed* PVs (needTotal>0) but PV works are not visible yet, do NOT mark completed.
		//    Requeue and wait until works appear / get applied.
		if expectedCount == 0 {
			if needTotal > 0 || createdTotal > 0 {
				klog.Infof("Waiting PV Works to appear for RB %s/%s (needTotal=%d createdTotal=%d)", rb.Namespace, rb.Name, needTotal, createdTotal)
				return false, nil
			}
			// nothing to create: allow completion path (no PV works expected)
			break
		}

		var availableCount int
		var mu sync.Mutex
		var wg sync.WaitGroup

		for _, pvWork := range pvWorkList.Items {
			wg.Add(1)
			go func(pvWork workv1alpha1.Work) {
				defer wg.Done()
				latest := &workv1alpha1.Work{}
				if err := c.Client.Get(ctx, client.ObjectKeyFromObject(&pvWork), latest); err != nil {
					klog.Warningf("⚠️ re-fetch PV Work %s failed: %v", pvWork.Name, err)
					return
				}
				for _, m := range latest.Status.ManifestStatuses {
					if m.Identifier.Kind != "PersistentVolume" || m.Health != "Healthy" {
						continue
					}
					var phaseStruct struct{ Phase string `json:"phase"` }
					if err := json.Unmarshal(m.Status.Raw, &phaseStruct); err != nil {
						continue
					}
					if phaseStruct.Phase == "Available" || phaseStruct.Phase == "Bound" {
						mu.Lock()
						availableCount++
						mu.Unlock()
						break
					}
				}
			}(pvWork)
		}
		wg.Wait()

		if availableCount == expectedCount {
			break
		}
		time.Sleep(retryInterval)
	}

	// 2) PV 완료 표식
	if err := c.patchRBAnnotations(ctx, rb, map[string]string{
		AnnPVMigration: "completed",
	}); err != nil {
		klog.Warningf("Failed to mark %s=completed: %v", AnnPVMigration, err)
	}

	// 3) 최신 RB 재조회 후 restore-phase에 따른 해제/대기 결정 (+그레이스)
	fresh := &workv1alpha2.ResourceBinding{}
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(rb), fresh); err != nil {
		return false, fmt.Errorf("get fresh RB: %w", err)
	}
	fAnns := fresh.GetAnnotations()
	pvDone := fAnns != nil && fAnns[AnnPVMigration] == "completed"
	restorePhase := getRestorePhase(fAnns) // "", working, succeeded, failed

	switch restorePhase {
	case "succeeded":
		if pvDone {
			if err := c.unsuspendRB(ctx, fresh); err != nil {
				return false, err
			}
			return true, nil
			_ = c.removeRBAnnotations(ctx, fresh, []string{AnnFailedCluster})
			return true, nil
		}
		return false, nil
	case "failed":
		klog.Infof("Hold suspension: restore-phase=failed (pv-migration=%q)", fAnns[AnnPVMigration])
		return false, nil
	case "working":
		klog.Infof("Hold suspension: restore-phase=working (pv-migration=%q)", fAnns[AnnPVMigration])
		return false, nil
	case "":
		// 🔒 레이스 방지: restore-phase 미설정이라도 바로 풀지 말고 잠깐 대기
		if !pvDone {
			return false, nil
		}
		waitStart := ""
		if fAnns != nil {
			waitStart = fAnns[RestoreWaitStartAnno]
		}
		if waitStart == "" {
			_ = c.patchRBAnnotations(ctx, fresh, map[string]string{
				RestoreWaitStartAnno: time.Now().UTC().Format(time.RFC3339),
			})
			klog.Infof("Hold suspension: restore-phase empty; starting grace wait")
			return false, nil
		}
		if t, err := time.Parse(time.RFC3339, waitStart); err == nil {
			if time.Since(t) > RestoreWaitGrace {
				klog.Infof("Grace wait elapsed (>%v) with no restore-phase; unsuspending", RestoreWaitGrace)
				if err := c.unsuspendRB(ctx, fresh); err != nil {
					return false, err
				}
				return true, nil
			}
		} else {
			_ = c.patchRBAnnotations(ctx, fresh, map[string]string{
				RestoreWaitStartAnno: time.Now().UTC().Format(time.RFC3339),
			})
		}
		return false, nil
	default:
		klog.Infof("Hold suspension: unknown restore-phase=%q", restorePhase)
		return false, nil
	}
}

/* ================================ Utilities ================================ */

func (c *PVMigrationController) patchRBAnnotations(ctx context.Context, rb *workv1alpha2.ResourceBinding, add map[string]string) error {
	key := client.ObjectKeyFromObject(rb)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &workv1alpha2.ResourceBinding{}
		if err := c.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		anns := fresh.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		need := false
		for k, v := range add {
			if curr, ok := anns[k]; !ok || curr != v {
				anns[k] = v
				need = true
			}
		}
		if !need {
			return nil
		}
		fresh.SetAnnotations(anns)
		return c.Client.Update(ctx, fresh)
	})
}

// removeRBAnnotations removes given annotation keys from the RB (idempotent).
func (c *PVMigrationController) removeRBAnnotations(
	ctx context.Context,
	rb *workv1alpha2.ResourceBinding,
	keys []string,
) error {
	objKey := client.ObjectKeyFromObject(rb)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &workv1alpha2.ResourceBinding{}
		if err := c.Client.Get(ctx, objKey, fresh); err != nil {
			return err
		}

		anns := fresh.GetAnnotations()
		if len(anns) == 0 {
			return nil
		}

		changed := false
		for _, k := range keys {
			if _, ok := anns[k]; ok {
				delete(anns, k)
				changed = true
			}
		}
		if !changed {
			return nil
		}

		// Merge patch to avoid clobbering other concurrent changes
		base := fresh.DeepCopy()
		fresh.SetAnnotations(anns)
		return c.Client.Patch(ctx, fresh, client.MergeFrom(base))
	})
}

func (c *PVMigrationController) unsuspendRB(ctx context.Context, rb *workv1alpha2.ResourceBinding) error {
	// spec.suspension.dispatching=false
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &workv1alpha2.ResourceBinding{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(rb), fresh); err != nil {
			return err
		}
		if fresh.Spec.Suspension == nil {
			fresh.Spec.Suspension = &workv1alpha2.Suspension{}
		}
		falseVal := false
		fresh.Spec.Suspension.Dispatching = &falseVal
		if err := c.Client.Update(ctx, fresh); err != nil {
			return err
		}
		klog.Infof("🎯 Unsuspended RB %s/%s (dispatching=false)", fresh.Namespace, fresh.Name)
		return nil
	})
}

func (c *PVMigrationController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(PVMigrationControllerName).
		For(&workv1alpha2.ResourceBinding{}). // no predicate, triggers on create/update
		Complete(c)
}

func parseOrdinalFromClaimName(name string) (int, bool) {
	parts := strings.Split(name, "-")
	if len(parts) < 3 {
		return 0, false
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, false
	}
	return n, true
}

// 메타워크(ConfigMap)의 PV Spec에서 ClaimRef.Name을 읽어 prefix 목록을 추출
// 예: "data-web-0" → "data-web-"
func extractPrefixesFromMetaWork(workList *workv1alpha1.WorkList, rb *workv1alpha2.ResourceBinding) []string {
	expected := rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name
	dedup := map[string]struct{}{}
	var prefixes []string
	for _, w := range workList.Items {
		if w.Labels["pvsync.karmada.io/source-sts"] != expected {
			continue
		}
		for _, manifest := range w.Spec.Workload.Manifests {
			var cm corev1.ConfigMap
			if err := yaml.Unmarshal(manifest.Raw, &cm); err != nil {
				continue
			}
			for _, pvSpecYaml := range cm.Data {
				var full corev1.PersistentVolumeSpec
				if err := yaml.Unmarshal([]byte(pvSpecYaml), &full); err != nil {
					continue
				}
				if full.ClaimRef == nil {
					continue
				}
				name := full.ClaimRef.Name
				parts := strings.Split(name, "-")
				if len(parts) < 3 {
					continue
				}
				prefix := strings.Join(parts[:len(parts)-1], "-") + "-"
				if _, ok := dedup[prefix]; !ok {
					dedup[prefix] = struct{}{}
					prefixes = append(prefixes, prefix)
				}
			}
		}
	}
	return prefixes
}

// 소스 템플릿을 src ordinal 오름차순으로 수집
func collectSortedPVTemplatesFromMetaWork(workList *workv1alpha1.WorkList, stsNS, stsName string) []pvTemplate {
	expected := stsNS + "." + stsName
	var out []pvTemplate
	for _, w := range workList.Items {
		if w.Labels["pvsync.karmada.io/source-sts"] != expected {
			continue
		}
		for _, manifest := range w.Spec.Workload.Manifests {
			var cm corev1.ConfigMap
			if err := yaml.Unmarshal(manifest.Raw, &cm); err != nil {
				continue
			}
			for _, pvSpecYaml := range cm.Data {
				var full corev1.PersistentVolumeSpec
				if err := yaml.Unmarshal([]byte(pvSpecYaml), &full); err != nil {
					continue
				}
				if full.ClaimRef == nil {
					continue
				}
				if n, ok := parseOrdinalFromClaimName(full.ClaimRef.Name); ok {
					out = append(out, pvTemplate{srcOrdinal: n, specYAML: pvSpecYaml})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].srcOrdinal < out[j].srcOrdinal })
	return out
}

// allowed 소스 클러스터 필터 버전(prefix)
func extractPrefixesFromMetaWorkFiltered(workList *workv1alpha1.WorkList, rb *workv1alpha2.ResourceBinding, allowed map[string]bool) []string {
	expected := rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name
	dedup := map[string]struct{}{}
	var prefixes []string
	for _, w := range workList.Items { // ✅ FIX: 구조체 리터럴에 .Items 붙이던 문법 오류 수정
		if w.Labels["pvsync.karmada.io/source-sts"] != expected {
			continue
		}
		cn := w.Labels["pvsync.karmada.io/cluster"]
		if cn == "" {
			var err error
			cn, err = names.GetClusterName(w.Namespace)
			if err != nil {
				continue
			}
		}
		if len(allowed) > 0 && !allowed[cn] {
			continue
		}
		for _, manifest := range w.Spec.Workload.Manifests {
			var cm corev1.ConfigMap
			if err := yaml.Unmarshal(manifest.Raw, &cm); err != nil {
				continue
			}
			for _, pvSpecYaml := range cm.Data {
				var full corev1.PersistentVolumeSpec
				if err := yaml.Unmarshal([]byte(pvSpecYaml), &full); err != nil {
					continue
				}
				if full.ClaimRef == nil {
					continue
				}
				name := full.ClaimRef.Name
				parts := strings.Split(name, "-")
				if len(parts) < 3 {
					continue
				}
				prefix := strings.Join(parts[:len(parts)-1], "-") + "-"
				if _, ok := dedup[prefix]; !ok {
					dedup[prefix] = struct{}{}
					prefixes = append(prefixes, prefix)
				}
			}
		}
	}
	return prefixes
}

// allowed 소스 클러스터 필터 버전(템플릿)
func collectSortedPVTemplatesFromMetaWorkFiltered(workList *workv1alpha1.WorkList, stsNS, stsName string, allowed map[string]bool) []pvTemplate {
	expected := stsNS + "." + stsName
	var out []pvTemplate
	for _, w := range workList.Items {
		if w.Labels["pvsync.karmada.io/source-sts"] != expected {
			continue
		}
		cn := w.Labels["pvsync.karmada.io/cluster"]
		if cn == "" {
			var err error
			cn, err = names.GetClusterName(w.Namespace)
			if err != nil {
				continue
			}
		}
		if len(allowed) > 0 && !allowed[cn] {
			continue
		}
		for _, manifest := range w.Spec.Workload.Manifests {
			var cm corev1.ConfigMap
			if err := yaml.Unmarshal(manifest.Raw, &cm); err != nil {
				continue
			}
			for _, pvSpecYaml := range cm.Data {
				var full corev1.PersistentVolumeSpec
				if err := yaml.Unmarshal([]byte(pvSpecYaml), &full); err != nil {
					continue
				}
				if full.ClaimRef == nil {
					continue
				}
				if n, ok := parseOrdinalFromClaimName(full.ClaimRef.Name); ok {
					out = append(out, pvTemplate{srcOrdinal: n, specYAML: pvSpecYaml})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].srcOrdinal < out[j].srcOrdinal })
	return out
}

func shortHash(input string) string {
	h := sha1.New()
	h.Write([]byte(input))
	return hex.EncodeToString(h.Sum(nil))[:10]
}