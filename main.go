package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// ===== Config =====

type Config struct {
	Namespace      string
	OrchBaseURL    string // how pods reach this service, e.g. http://job-orchestrator:8080
	StorageDir     string // where uploaded files live
	ManagedBy      string // label marker
	DefaultTTL     int32  // seconds
	SharedHostPath string
}

func loadConfig() Config {
	cfg := Config{
		Namespace:      os.Getenv("NAMESPACE"),
		OrchBaseURL:    os.Getenv("ORCH_BASE_URL"),
		StorageDir:     getenv("STORAGE_DIR", "/data"),
		ManagedBy:      "go-job-orchestrator",
		DefaultTTL:     60,
		SharedHostPath: os.Getenv("DATA_HOST_PATH"),
	}
	if cfg.Namespace == "" {
		if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			cfg.Namespace = strings.TrimSpace(string(ns))
		}
		if cfg.Namespace == "" {
			cfg.Namespace = "default"
		}
	}
	if cfg.OrchBaseURL == "" {
		cfg.OrchBaseURL = "http://job-orchestrator:8080"
	}
	return cfg
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ===== In-memory token index (persisted alongside artifacts) =====

type artifactIndex struct {
	Token string   `json:"token"`
	Files []string `json:"files"`
}

func writeIndex(dir string, idx artifactIndex) error {
	b, _ := json.Marshal(idx)
	return os.WriteFile(filepath.Join(dir, "index.json"), b, 0o600)
}

func readIndex(dir string) (artifactIndex, error) {
	b, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return artifactIndex{}, err
	}
	var idx artifactIndex
	return idx, json.Unmarshal(b, &idx)
}

func randomToken(n int) (string, error) {
	buf := make([]byte, n)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// ===== Kube client =====

func kubeClient() (*kubernetes.Clientset, error) {
	// 1. Try in-cluster config (when running inside Kubernetes pod)
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}

	// 2. If KUBECONFIG env is set, use that
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		if cfg, err := clientcmd.BuildConfigFromFlags("", kc); err == nil {
			return kubernetes.NewForConfig(cfg)
		} else {
			return nil, fmt.Errorf("failed to load kubeconfig from KUBECONFIG=%s: %w", kc, err)
		}
	}

	// 3. Fall back to ~/.kube/config (cross-platform safe)
	home := homedir.HomeDir()
	if home == "" {
		return nil, fmt.Errorf("cannot determine home directory; set KUBECONFIG")
	}
	kc := filepath.Join(home, ".kube", "config")

	cfg, err := clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig from %s: %w", kc, err)
	}
	return kubernetes.NewForConfig(cfg)
}

// ===== HTTP / API =====

type jobRequest struct {
	Runtime               string `form:"runtime" json:"runtime"`
	Entrypoint            string `form:"entrypoint" json:"entrypoint"`
	ArgsJSON              string `form:"args" json:"args"` // JSON array (optional)
	EnvJSON               string `form:"env" json:"env"`   // JSON object (optional)
	CPU                   string `form:"cpu" json:"cpu"`
	Memory                string `form:"memory" json:"memory"`
	TTLSecondsAfterFinish int32  `form:"ttlSecondsAfterFinished" json:"ttlSecondsAfterFinished"`
}

type jobResponse struct {
	JobID     string `json:"jobID"`
	JobName   string `json:"jobName"`
	Namespace string `json:"namespace"`
}

func main() {
	cfg := loadConfig()
	if err := os.MkdirAll(cfg.StorageDir, 0o755); err != nil {
		log.Fatalf("create storage dir: %v", err)
	}
	client, err := kubeClient()
	if err != nil {
		log.Fatalf("kube client: %v", err)
	}

	r := gin.Default()

	// Graceful shutdown context for background goroutines
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start background controllers
	go watchJobs(ctx, client, cfg)      // writes /data/<jobID>/status.json when a job completes/fails
	go artifactReaper(ctx, client, cfg) // periodically removes old /data/<jobID> folders

	// healthz
	r.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })

	/* 	// Upload + create job
	   	r.POST("/api/v1/jobs", func(c *gin.Context) {
	   		var req jobRequest
	   		if err := c.Bind(&req); err != nil {
	   			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	   			return
	   		}
	   		if req.Runtime == "" || req.Entrypoint == "" {
	   			c.JSON(http.StatusBadRequest, gin.H{"error": "runtime and entrypoint are required"})
	   			return
	   		}
	   		// Collect args from JSON or repeated form fields
	   		var args []string
	   		if strings.TrimSpace(req.ArgsJSON) != "" {
	   			if err := json.Unmarshal([]byte(req.ArgsJSON), &args); err != nil {
	   				c.JSON(400, gin.H{"error": "args must be JSON array"})
	   				return
	   			}
	   		}
	   		args = append(args, c.PostFormArray("arg")...)

	   		// Env (optional JSON)
	   		envMap := map[string]string{}
	   		if strings.TrimSpace(req.EnvJSON) != "" {
	   			if err := json.Unmarshal([]byte(req.EnvJSON), &envMap); err != nil {
	   				c.JSON(400, gin.H{"error": "env must be JSON object"})
	   				return
	   			}
	   		}

	   		// Save files
	   		jobID := uuid.NewString()
	   		jobDir := filepath.Join(cfg.StorageDir, jobID)
	   		if err := os.MkdirAll(jobDir, 0o755); err != nil {
	   			c.JSON(500, gin.H{"error": err.Error()})
	   			return
	   		}

	   		form, err := c.MultipartForm()
	   		if err != nil {
	   			c.JSON(400, gin.H{"error": "multipart form required"})
	   			return
	   		}
	   		files := form.File["files"]
	   		if len(files) == 0 {
	   			c.JSON(400, gin.H{"error": "at least one file is required (files=...)"})
	   			return
	   		}

	   		stored := make([]string, 0, len(files))
	   		for _, fh := range files {
	   			name := filepath.Base(fh.Filename)
	   			out := filepath.Join(jobDir, name)
	   			f, err := fh.Open()
	   			if err != nil {
	   				c.JSON(500, gin.H{"error": err.Error()})
	   				return
	   			}
	   			defer f.Close()
	   			w, err := os.Create(out)
	   			if err != nil {
	   				c.JSON(500, gin.H{"error": err.Error()})
	   				return
	   			}
	   			if _, err := io.Copy(w, f); err != nil {
	   				_ = w.Close()
	   				c.JSON(500, gin.H{"error": err.Error()})
	   				return
	   			}
	   			_ = w.Close()
	   			stored = append(stored, name)
	   		}

	   		// Token for artifact access
	   		tok, _ := randomToken(16)
	   		_ = writeIndex(jobDir, artifactIndex{Token: tok, Files: stored})

	   		// Create k8s Job
	   		jobName := fmt.Sprintf("run-%s", strings.ToLower(uuid.NewString()[:8]))
	   		ttl := req.TTLSecondsAfterFinish
	   		if ttl <= 0 {
	   			ttl = cfg.DefaultTTL
	   		}

	   		image, cmd, cmdArgs, initScript, err := buildContainers(cfg, req.Runtime, req.Entrypoint, args, tok, jobID, stored)
	   		if err != nil {
	   			c.JSON(400, gin.H{"error": err.Error()})
	   			return
	   		}

	   		// Resources (optional)
	   		resources := corev1.ResourceRequirements{}
	   		if req.CPU != "" || req.Memory != "" {
	   			resources.Requests = corev1.ResourceList{}
	   			resources.Limits = corev1.ResourceList{}
	   			if req.CPU != "" {
	   				resources.Requests[corev1.ResourceCPU] = resourceQuantity(req.CPU)
	   				resources.Limits[corev1.ResourceCPU] = resourceQuantity(req.CPU)
	   			}
	   			if req.Memory != "" {
	   				resources.Requests[corev1.ResourceMemory] = resourceQuantity(req.Memory)
	   				resources.Limits[corev1.ResourceMemory] = resourceQuantity(req.Memory)
	   			}
	   		}

	   		envVars := []corev1.EnvVar{}
	   		for k, v := range envMap {
	   			envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	   		}
	   		sort.Slice(envVars, func(i, j int) bool { return envVars[i].Name < envVars[j].Name })

	   		job := &batchv1.Job{
	   			ObjectMeta: metav1.ObjectMeta{
	   				Name:      jobName,
	   				Namespace: cfg.Namespace,
	   				Labels: map[string]string{
	   					"managed-by": cfg.ManagedBy,
	   					"job-id":     jobID,
	   				},
	   			},
	   			Spec: batchv1.JobSpec{
	   				TTLSecondsAfterFinished: ptr.To(ttl),
	   				BackoffLimit:            ptr.To[int32](0),
	   				Template: corev1.PodTemplateSpec{
	   					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"job-name": jobName}},
	   					Spec: corev1.PodSpec{
	   						RestartPolicy: corev1.RestartPolicyNever,
	   						Volumes: []corev1.Volume{{
	   							Name:         "workspace",
	   							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	   						}},
	   						InitContainers: []corev1.Container{{
	   							Name:         "fetch-artifacts",
	   							Image:        "curlimages/curl:8.8.0",
	   							Command:      []string{"/bin/sh", "-lc", initScript},
	   							VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
	   						}},
	   						Containers: []corev1.Container{{
	   							Name:         "runner",
	   							Image:        image,
	   							Command:      cmd,
	   							Args:         cmdArgs,
	   							Env:          envVars,
	   							Resources:    resources,
	   							VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
	   						}},
	   					},
	   				},
	   			},
	   		}

	   		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	   		defer cancel()
	   		if _, err := client.BatchV1().Jobs(cfg.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
	   			c.JSON(500, gin.H{"error": err.Error()})
	   			return
	   		}

	   		c.JSON(200, jobResponse{JobID: jobID, JobName: jobName, Namespace: cfg.Namespace})
	   	}) */
	// POST /api/v1/jobs
	r.POST("/api/v1/jobs", func(c *gin.Context) {
		type jobRequest struct {
			Runtime               string `form:"runtime" json:"runtime"`
			Entrypoint            string `form:"entrypoint" json:"entrypoint"`
			ArgsJSON              string `form:"args" json:"args"` // JSON array (optional)
			EnvJSON               string `form:"env" json:"env"`   // JSON object (optional)
			CPU                   string `form:"cpu" json:"cpu"`
			Memory                string `form:"memory" json:"memory"`
			TTLSecondsAfterFinish int32  `form:"ttlSecondsAfterFinished" json:"ttlSecondsAfterFinished"`
		}

		var req jobRequest
		if err := c.Bind(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if strings.TrimSpace(req.Runtime) == "" || strings.TrimSpace(req.Entrypoint) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "runtime and entrypoint are required"})
			return
		}

		// Collect args from JSON and/or repeated -F arg=<value>
		var args []string
		if s := strings.TrimSpace(req.ArgsJSON); s != "" {
			if err := json.Unmarshal([]byte(s), &args); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "args must be a JSON array of strings"})
				return
			}
		}
		args = append(args, c.PostFormArray("arg")...)

		// Optional env as JSON object
		envMap := map[string]string{}
		if s := strings.TrimSpace(req.EnvJSON); s != "" {
			if err := json.Unmarshal([]byte(s), &envMap); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "env must be a JSON object"})
				return
			}
		}

		// Persist uploaded files under /data/<jobID>
		cfg := loadConfig()
		jobID := uuid.NewString()
		jobDir := filepath.Join(cfg.StorageDir, jobID)
		if err := os.MkdirAll(jobDir, 0o755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "mkdir job dir: " + err.Error()})
			return
		}
		form, err := c.MultipartForm()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "multipart form required"})
			return
		}
		files := form.File["files"]
		if len(files) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "at least one file required (files=...)"})
			return
		}
		stored := make([]string, 0, len(files))
		for _, fh := range files {
			name := filepath.Base(fh.Filename)
			dst := filepath.Join(jobDir, name)
			src, err := fh.Open()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "open upload: " + err.Error()})
				return
			}
			out, err := os.Create(dst)
			if err != nil {
				_ = src.Close()
				c.JSON(http.StatusInternalServerError, gin.H{"error": "create dst: " + err.Error()})
				return
			}
			if _, err := io.Copy(out, src); err != nil {
				_ = src.Close()
				_ = out.Close()
				c.JSON(http.StatusInternalServerError, gin.H{"error": "write dst: " + err.Error()})
				return
			}
			_ = src.Close()
			_ = out.Close()
			stored = append(stored, name)
		}

		// Per-job token for artifact fetching (used by initContainer)
		tok, _ := randomToken(16)
		if err := writeIndex(jobDir, artifactIndex{Token: tok, Files: stored}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "write index: " + err.Error()})
			return
		}

		// Build runtime containers + init script (this version should redirect logs when SharedHostPath is set)
		image, cmd, cmdArgs, initScript, err := buildContainers(cfg, req.Runtime, req.Entrypoint, args, tok, jobID, stored)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Resources (optional)
		resources := corev1.ResourceRequirements{}
		if req.CPU != "" || req.Memory != "" {
			resources.Requests = corev1.ResourceList{}
			resources.Limits = corev1.ResourceList{}
			if strings.TrimSpace(req.CPU) != "" {
				q := resource.MustParse(req.CPU)
				resources.Requests[corev1.ResourceCPU] = q
				resources.Limits[corev1.ResourceCPU] = q
			}
			if strings.TrimSpace(req.Memory) != "" {
				q := resource.MustParse(req.Memory)
				resources.Requests[corev1.ResourceMemory] = q
				resources.Limits[corev1.ResourceMemory] = q
			}
		}

		// Env vars for the runner
		envVars := make([]corev1.EnvVar, 0, len(envMap))
		for k, v := range envMap {
			envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
		}
		sort.Slice(envVars, func(i, j int) bool { return envVars[i].Name < envVars[j].Name })

		// TTL
		ttl := req.TTLSecondsAfterFinish
		if ttl <= 0 {
			ttl = cfg.DefaultTTL
		}

		// Job name
		jobName := fmt.Sprintf("run-%s", strings.ToLower(uuid.NewString()[:8]))

		// ---- Volumes & mounts ----
		vols := []corev1.Volume{
			{
				Name: "workspace",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
		if strings.TrimSpace(cfg.SharedHostPath) != "" {
			hpType := corev1.HostPathDirectoryOrCreate
			vols = append(vols, corev1.Volume{
				Name: "shared-data",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: cfg.SharedHostPath, // e.g. /run/desktop/mnt/host/c/rd-share/job-engine-data
						Type: &hpType,
					},
				},
			})
		}

		vmRunner := []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}}
		if strings.TrimSpace(cfg.SharedHostPath) != "" {
			// Mount the SAME hostPath at /data in the job pod, so logs go under /data/<jobID>/results
			vmRunner = append(vmRunner, corev1.VolumeMount{Name: "shared-data", MountPath: "/data"})
		}
		ttlCopy := ttl // int32
		backoff := int32(0)
		fsGroupID := int64(10001)

		// ---- Build Job object ----
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: cfg.Namespace,
				Labels: map[string]string{
					"managed-by": cfg.ManagedBy,
					"job-id":     jobID,
				},
			},
			Spec: batchv1.JobSpec{
				TTLSecondsAfterFinished: &ttlCopy,
				BackoffLimit:            &backoff,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"job-name": jobName},
					},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Volumes:       vols,

						InitContainers: []corev1.Container{{
							Name:    "fetch-artifacts",
							Image:   "curlimages/curl:8.8.0",
							Command: []string{"/bin/sh", "-lc", initScript},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
							},
						}},

						Containers: []corev1.Container{{
							Name:         "runner",
							Image:        image,
							Command:      cmd,
							Args:         cmdArgs,
							Env:          envVars,
							Resources:    resources,
							VolumeMounts: vmRunner,
						}},

						// Make writes work on stricter storage backends
						SecurityContext: &corev1.PodSecurityContext{
							FSGroup: &fsGroupID,
						},
					},
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.BatchV1().Jobs(cfg.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"jobID":     jobID,
			"jobName":   jobName,
			"namespace": cfg.Namespace,
		})
	})

	// list jobs
	r.GET("/api/v1/jobs", func(c *gin.Context) {
		ls := labels.Set{"managed-by": loadConfig().ManagedBy}.AsSelector().String()
		jobs, err := client.BatchV1().Jobs(loadConfig().Namespace).List(c, metav1.ListOptions{LabelSelector: ls})
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, jobs)
	})

	// job status
	r.GET("/api/v1/jobs/:id", func(c *gin.Context) {
		jobID := c.Param("id")
		// find by label job-id
		ls := labels.Set{"job-id": jobID}.AsSelector().String()
		jobs, err := client.BatchV1().Jobs(loadConfig().Namespace).List(c, metav1.ListOptions{LabelSelector: ls})
		if err != nil || len(jobs.Items) == 0 {
			c.JSON(404, gin.H{"error": "job not found"})
			return
		}
		c.JSON(200, jobs.Items[0])
	})

	// logs (first pod, first container)
	r.GET("/api/v1/jobs/:id/logs", func(c *gin.Context) {
		jobID := c.Param("id")
		ns := loadConfig().Namespace
		ls := labels.Set{"job-id": jobID}.AsSelector().String()
		jobs, err := client.BatchV1().Jobs(ns).List(c, metav1.ListOptions{LabelSelector: ls})
		if err != nil || len(jobs.Items) == 0 {
			c.JSON(404, gin.H{"error": "job not found"})
			return
		}
		job := jobs.Items[0]
		podList, err := client.CoreV1().Pods(ns).List(c, metav1.ListOptions{LabelSelector: labels.Set{"job-name": job.Name}.AsSelector().String()})
		if err != nil || len(podList.Items) == 0 {
			c.JSON(404, gin.H{"error": "pod not found"})
			return
		}
		pod := podList.Items[0]
		tail := int64(200)
		if v := c.Query("tail"); v != "" {
			if vv, e := parseInt64(v); e == nil {
				tail = vv
			}
		}
		req := client.CoreV1().Pods(ns).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "runner", TailLines: &tail})
		rc, err := req.Stream(c)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		defer rc.Close()
		c.Status(200)
		c.Header("Content-Type", "text/plain")
		io.Copy(c.Writer, rc)
	})

	// artifacts: served to initContainers (Authorization: Bearer <token>)
	r.GET("/artifacts/:id/:filename", func(c *gin.Context) {
		id := c.Param("id")
		fname := filepath.Base(c.Param("filename"))
		dir := filepath.Join(loadConfig().StorageDir, id)
		idx, err := readIndex(dir)
		if err != nil {
			c.JSON(404, gin.H{"error": "unknown job"})
			return
		}
		// auth
		ah := c.GetHeader("Authorization")
		if !strings.HasPrefix(ah, "Bearer ") || strings.TrimPrefix(ah, "Bearer ") != idx.Token {
			c.JSON(401, gin.H{"error": "unauthorized"})
			return
		}
		// allow only registered files
		allowed := false
		for _, f := range idx.Files {
			if f == fname {
				allowed = true
				break
			}
		}
		if !allowed {
			c.JSON(404, gin.H{"error": "file not found"})
			return
		}
		c.FileAttachment(filepath.Join(dir, fname), fname)
	})

	// Upload results: PUT /artifacts/:id/upload/:filename
	// Auth: Authorization: Bearer <token>
	// Saves into /data/<jobID>/results/<filename>
	r.PUT("/artifacts/:id/upload/:filename", func(c *gin.Context) {
		id := c.Param("id")
		fname := filepath.Base(c.Param("filename"))
		dir := filepath.Join(loadConfig().StorageDir, id)
		idx, err := readIndex(dir)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "unknown job"})
			return
		}
		// auth
		ah := c.GetHeader("Authorization")
		if !strings.HasPrefix(ah, "Bearer ") || strings.TrimPrefix(ah, "Bearer ") != idx.Token {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		// ensure results dir
		outDir := filepath.Join(dir, "results")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		dst := filepath.Join(outDir, fname)
		f, err := os.Create(dst)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer f.Close()
		if _, err := io.Copy(f, c.Request.Body); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Status(http.StatusCreated)
	})
	// GET saved outputs: /outputs/:id/:filename
	r.GET("/outputs/:id/:filename", func(c *gin.Context) {
		id := c.Param("id")
		fname := filepath.Base(c.Param("filename"))
		dir := filepath.Join(loadConfig().StorageDir, id)
		idx, err := readIndex(dir)
		if err != nil {
			c.JSON(404, gin.H{"error": "unknown job"})
			return
		}
		ah := c.GetHeader("Authorization")
		if !strings.HasPrefix(ah, "Bearer ") || strings.TrimPrefix(ah, "Bearer ") != idx.Token {
			c.JSON(401, gin.H{"error": "unauthorized"})
			return
		}
		p := filepath.Join(dir, "results", fname)
		if _, err := os.Stat(p); err != nil {
			c.JSON(404, gin.H{"error": "file not found"})
			return
		}
		c.FileAttachment(p, fname)
	})

	addr := ":8080"
	log.Printf("listening on %s (namespace=%s)", addr, cfg.Namespace)
	r.Run(addr)
}

// watchJobs writes a tiny status.json to /data/<jobID>/ when a managed Job completes or fails.
func watchJobs(ctx context.Context, client *kubernetes.Clientset, cfg Config) {
	selector := labels.Set{"managed-by": cfg.ManagedBy}.AsSelector().String()
	for ctx.Err() == nil {
		w, err := client.BatchV1().Jobs(cfg.Namespace).Watch(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			log.Printf("watchJobs: watch error: %v; retrying...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		ch := w.ResultChan()
		for {
			select {
			case <-ctx.Done():
				w.Stop()
				return
			case ev, ok := <-ch:
				if !ok {
					w.Stop()
					time.Sleep(500 * time.Millisecond)
					break
				}
				j, ok := ev.Object.(*batchv1.Job)
				if !ok || j == nil {
					continue
				}
				jobID := j.Labels["job-id"]
				if jobID == "" {
					continue
				}
				if j.Status.Succeeded > 0 || j.Status.Failed > 0 {
					status := map[string]any{
						"jobName":        j.Name,
						"succeeded":      j.Status.Succeeded > 0,
						"failed":         j.Status.Failed > 0,
						"startTime":      j.Status.StartTime,
						"completionTime": j.Status.CompletionTime,
						"conditions":     j.Status.Conditions,
					}
					b, _ := json.MarshalIndent(status, "", "  ")
					out := filepath.Join(cfg.StorageDir, jobID, "status.json")
					if err := os.WriteFile(out, b, 0o644); err != nil {
						log.Printf("watchJobs: write %s: %v", out, err)
					} else {
						log.Printf("watchJobs: wrote %s", out)
					}
				}
			}
		}
	}
}

// artifactReaper removes /data/<jobID> if the job no longer exists and the folder is older than TTL+buffer.
func artifactReaper(ctx context.Context, client *kubernetes.Clientset, cfg Config) {
	//ticker := time.NewTicker(10 * time.Minute) // adjust if you want quicker testing
	ticker := time.NewTicker(10 * time.Second) // adjust if you want quicker testing
	defer ticker.Stop()
	//buffer := int32(600) // extra seconds after TTL
	buffer := int32(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := os.ReadDir(cfg.StorageDir)
			if err != nil {
				log.Printf("reaper: read dir: %v", err)
				continue
			}
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				jobID := e.Name()
				// if job still exists, skip
				ls := labels.Set{"job-id": jobID}.AsSelector().String()
				jobs, _ := client.BatchV1().Jobs(cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: ls})
				if len(jobs.Items) > 0 {
					continue
				}
				// age check
				fi, err := os.Stat(filepath.Join(cfg.StorageDir, jobID))
				if err != nil {
					continue
				}
				maxAge := time.Duration(cfg.DefaultTTL+buffer) * time.Second
				if time.Since(fi.ModTime()) > maxAge {
					target := filepath.Join(cfg.StorageDir, jobID)
					if err := os.RemoveAll(target); err != nil {
						log.Printf("reaper: remove %s: %v", target, err)
					} else {
						log.Printf("reaper: removed %s", target)
					}
				}
			}
		}
	}
}

// buildContainers returns image/command/args for main container and init script to fetch artifacts.
/* func buildContainers(cfg Config, runtime, entrypoint string, args []string, token, jobID string, files []string) (image string, cmd []string, cmdArgs []string, initScript string, err error) {
	entry := "/workspace/" + strings.TrimPrefix(entrypoint, "/")
	switch strings.ToLower(runtime) {
	case "python":
		image = "python:3.11-slim"
		cmd = []string{"python", entry}
	case "node":
		image = "node:20-alpine"
		cmd = []string{"node", entry}
	case "java":
		image = "eclipse-temurin:21-jre"
		if strings.HasSuffix(strings.ToLower(entry), ".jar") {
			cmd = []string{"java", "-jar", entry}
		} else {
			cmd = []string{"java", entry}
		}
	case "binary":
		image = "ubuntu:24.04"
		cmd = []string{"/bin/sh", "-lc", "chmod +x '" + entry + "' && '" + entry + "'"}
	default:
		return "", nil, nil, "", fmt.Errorf("unsupported runtime: %s", runtime)
	}
	cmdArgs = args

	// init script: download all artifacts into /workspace
	var b strings.Builder
	b.WriteString("set -euo pipefail; mkdir -p /workspace; ")
	for _, f := range files {
		url := fmt.Sprintf("%s/artifacts/%s/%s", strings.TrimRight(cfg.OrchBaseURL, "/"), jobID, f)
		b.WriteString(fmt.Sprintf("curl -fsSL -H 'Authorization: Bearer %s' %s -o /workspace/%s; ", token, shellQuote(url), shellEscapePath(f)))
	}
	initScript = b.String()
	return
}
*/
func buildContainers(
	cfg Config,
	runtime, entrypoint string,
	args []string,
	token, jobID string,
	files []string,
) (image string, cmd []string, cmdArgs []string, initScript string, err error) {

	// Normalize entrypoint path inside the pod
	entry := "/workspace/" + strings.TrimPrefix(entrypoint, "/")

	// Quote all args for safe shell execution
	qargs := make([]string, 0, len(args))
	for _, a := range args {
		qargs = append(qargs, shellQuote(a))
	}
	argStr := strings.Join(qargs, " ")

	// Build the runtime-specific command line
	var runCmd string
	switch strings.ToLower(runtime) {
	case "python":
		image = "python:3.11-slim"
		runCmd = "python " + shellQuote(entry)
		if argStr != "" {
			runCmd += " " + argStr
		}
	case "node":
		image = "node:20-alpine"
		runCmd = "node " + shellQuote(entry)
		if argStr != "" {
			runCmd += " " + argStr
		}
	case "java":
		image = "eclipse-temurin:21-jre"
		if strings.HasSuffix(strings.ToLower(entry), ".jar") {
			runCmd = "java -jar " + shellQuote(entry)
		} else {
			runCmd = "java " + shellQuote(entry)
		}
		if argStr != "" {
			runCmd += " " + argStr
		}
	case "binary":
		image = "ubuntu:24.04"
		// Make it executable, then run
		runCmd = "chmod +x " + shellQuote(entry) + " && " + shellQuote(entry)
		if argStr != "" {
			runCmd += " " + argStr
		}
	default:
		return "", nil, nil, "", fmt.Errorf("unsupported runtime: %s", runtime)
	}

	// Wrap in a shell to redirect logs and mark completion
	// -> stdout: /workspace/stdout.log
	// -> stderr: /workspace/stderr.log
	// -> completion marker: /workspace/.done
	/* 	runCmd = runCmd + " > /workspace/stdout.log 2> /workspace/stderr.log; touch /workspace/.done"
	   	cmd = []string{"/bin/sh", "-lc"}
	   	cmdArgs = []string{runCmd} */
	// Decide where to write logs
	resultsDir := "/workspace" // default
	if strings.TrimSpace(cfg.SharedHostPath) != "" {
		resultsDir = "/data/" + jobID + "/results" // <— SAME /data path as orchestrator PVC/hostPath
	}

	// Wrap command to create the dir, run, and redirect
	runCmd = "mkdir -p " + shellQuote(resultsDir) + " ; " +
		runCmd + " > " + shellQuote(resultsDir+"/stdout.log") +
		" 2> " + shellQuote(resultsDir+"/stderr.log") +
		" ; touch /workspace/.done"

	cmd = []string{"/bin/sh", "-lc"}
	cmdArgs = []string{runCmd}

	// Init script: download all artifacts to /workspace using bearer token
	var sb strings.Builder
	sb.WriteString("set -euo pipefail; mkdir -p /workspace; ")
	base := strings.TrimRight(cfg.OrchBaseURL, "/")
	for _, f := range files {
		url := fmt.Sprintf("%s/artifacts/%s/%s", base, jobID, f)
		sb.WriteString(fmt.Sprintf(
			"curl -fsSL -H 'Authorization: Bearer %s' %s -o /workspace/%s; ",
			token, shellQuote(url), shellEscapePath(f),
		))
	}
	initScript = sb.String()

	return
}

func shellQuote(s string) string { // naive
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "'\\''"))
}

func joinShellArgs(args []string) string {
	if len(args) == 0 {
		return ""
	}
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, shellQuote(a))
	}
	return strings.Join(out, " ")
}

func shellEscapePath(p string) string {
	return strings.ReplaceAll(p, "'", "'\\''")
}

func resourceQuantity(q string) resource.Quantity {
	return resource.MustParse(q)
}
func parseInt64(s string) (int64, error) {
	var x int64
	_, err := fmt.Sscan(s, &x)
	return x, err
}

// Wait helper (unused here but handy for extensions)
func waitForJobComplete(ctx context.Context, client *kubernetes.Clientset, ns, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, false, func(ctx context.Context) (bool, error) {
		j, err := client.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if j.Status.Succeeded > 0 {
			return true, nil
		}
		if j.Status.Failed > 0 {
			return false, errors.New("job failed")
		}
		return false, nil
	})
}
