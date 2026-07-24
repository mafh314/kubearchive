// Copyright KubeArchive Authors
// SPDX-License-Identifier: Apache-2.0
//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/avast/retry-go/v5"
	"github.com/kubearchive/kubearchive/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// TestPasswordRotation verifies that database password rotation works correctly.
//
// Prerequisite (external to KubeArchive):
//   - The database administrator changes the kubearchive user password in PostgreSQL.
//
// KubeArchive user procedure:
//  1. Update the kubearchive-database-credentials Secret with the new password.
//  2. Rollout restart kubearchive-sink and kubearchive-api-server.
//  3. Verify the components reconnect successfully with the new credentials.
func TestPasswordRotation(t *testing.T) {
	clientset, _ := test.GetKubernetesClient(t)
	ctx := context.Background()

	secret, err := clientset.CoreV1().Secrets("kubearchive").Get(ctx, "kubearchive-database-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get kubearchive-database-credentials secret: %v", err)
	}
	originalPassword := string(secret.Data["DATABASE_PASSWORD"])
	t.Logf("Retrieved original database password from Secret")

	newPassword := "Rotated-P@ss-" + test.RandomString()

	t.Cleanup(func() {
		t.Log("Restoring original database password")
		alterDBPassword(t, clientset, originalPassword)
		patchDatabaseSecret(t, clientset, originalPassword)
		rolloutRestartDeployment(t, clientset, "kubearchive-sink")
		rolloutRestartDeployment(t, clientset, "kubearchive-api-server")
		waitForDBConnection(t, "kubearchive-sink")
		waitForDBConnection(t, "kubearchive-api-server")
	})

	// Prerequisite: database administrator changes the password in PostgreSQL
	t.Log("Prerequisite: changing database password in PostgreSQL")
	alterDBPassword(t, clientset, newPassword)

	// Step 1: update the kubearchive-database-credentials Secret
	t.Log("Step 1: updating kubearchive-database-credentials Secret")
	patchDatabaseSecret(t, clientset, newPassword)

	// Step 2: rollout restart the KubeArchive deployments
	t.Log("Step 2: restarting kubearchive-sink")
	rolloutRestartDeployment(t, clientset, "kubearchive-sink")
	t.Log("Step 2: restarting kubearchive-api-server")
	rolloutRestartDeployment(t, clientset, "kubearchive-api-server")

	// Step 3: verify the components reconnect with the new credentials
	t.Log("Step 3: waiting for sink to connect to the database with the new password")
	waitForDBConnection(t, "kubearchive-sink")
	t.Log("Step 3: waiting for api-server to connect to the database with the new password")
	waitForDBConnection(t, "kubearchive-api-server")
}

// alterDBPassword changes the kubearchive database user password by
// executing ALTER USER via psql in the PostgreSQL pod.
func alterDBPassword(t testing.TB, clientset *kubernetes.Clientset, password string) {
	t.Helper()

	config, err := test.GetKubernetesConfig()
	if err != nil {
		t.Fatalf("Failed to get Kubernetes config: %v", err)
	}

	podName := test.GetPodName(t, clientset, "postgresql", "kubearchive-")
	if podName == "" {
		t.Fatal("Could not find PostgreSQL pod in namespace 'postgresql'")
	}

	cmd := []string{"psql", "-U", "postgres", "-c", fmt.Sprintf("ALTER USER kubearchive WITH PASSWORD '%s'", password)}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace("postgresql").
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: cmd,
			Stdout:  true,
			Stderr:  true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		t.Fatalf("Failed to create SPDY executor: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("Failed to execute ALTER USER: %v, stderr: %s", err, stderr.String())
	}

	t.Logf("ALTER USER result: %s", strings.TrimSpace(stdout.String()))
}

// patchDatabaseSecret updates the DATABASE_PASSWORD in the kubearchive-database-credentials Secret.
func patchDatabaseSecret(t testing.TB, clientset *kubernetes.Clientset, password string) {
	t.Helper()
	ctx := context.Background()

	secret, err := clientset.CoreV1().Secrets("kubearchive").Get(ctx, "kubearchive-database-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get secret: %v", err)
	}

	secret.Data["DATABASE_PASSWORD"] = []byte(password)

	_, err = clientset.CoreV1().Secrets("kubearchive").Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update secret: %v", err)
	}

	// Verify the Secret was updated correctly
	updated, err := clientset.CoreV1().Secrets("kubearchive").Get(ctx, "kubearchive-database-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to verify updated secret: %v", err)
	}
	if string(updated.Data["DATABASE_PASSWORD"]) != password {
		t.Fatalf("Secret password mismatch after update: expected %q, got %q",
			password, string(updated.Data["DATABASE_PASSWORD"]))
	}
	t.Logf("Secret updated, DATABASE_PASSWORD is now: %s", base64.StdEncoding.EncodeToString([]byte(password)))
}

// rolloutRestartDeployment simulates `kubectl rollout restart` by scaling
// the deployment to 0 and back to 1, waiting for the old pod to terminate
// and the new pod to be created.
func rolloutRestartDeployment(t testing.TB, clientset *kubernetes.Clientset, deploymentName string) {
	t.Helper()
	ctx := context.Background()

	labelSelector := fmt.Sprintf("app=%s", deploymentName)

	// Scale to 0
	scale, err := clientset.AppsV1().Deployments("kubearchive").GetScale(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get scale for %s: %v", deploymentName, err)
	}
	scaleCopy := *scale
	scaleCopy.Spec.Replicas = 0
	_, err = clientset.AppsV1().Deployments("kubearchive").UpdateScale(ctx, deploymentName, &scaleCopy, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to scale %s to 0: %v", deploymentName, err)
	}
	t.Logf("Scaled %s to 0 replicas", deploymentName)

	// Wait for all pods to disappear
	err = retry.New(retry.Attempts(30), retry.MaxDelay(2*time.Second)).Do(func() error {
		pods, listErr := clientset.CoreV1().Pods("kubearchive").List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if listErr != nil {
			return listErr
		}
		if len(pods.Items) > 0 {
			return fmt.Errorf("%s still has %d pod(s)", deploymentName, len(pods.Items))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for %s pods to terminate: %v", deploymentName, err)
	}

	// Scale back to 1
	scale, err = clientset.AppsV1().Deployments("kubearchive").GetScale(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get scale for %s: %v", deploymentName, err)
	}
	scaleCopy = *scale
	scaleCopy.Spec.Replicas = 1
	_, err = clientset.AppsV1().Deployments("kubearchive").UpdateScale(ctx, deploymentName, &scaleCopy, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to scale %s to 1: %v", deploymentName, err)
	}
	t.Logf("Scaled %s to 1 replica", deploymentName)
}

// waitForDBConnection waits for a component's pod to log a successful database connection.
func waitForDBConnection(t testing.TB, podPrefix string) {
	t.Helper()

	retryErr := retry.New(retry.Attempts(30), retry.MaxDelay(2*time.Second)).Do(func() error {
		logs, getErr := test.GetPodLogs(t, "kubearchive", podPrefix)
		if getErr != nil {
			return getErr
		}

		if strings.Contains(logs, "Successfully connected to the database") {
			return nil
		}

		return fmt.Errorf("%s has not connected to the database yet", podPrefix)
	})

	if retryErr != nil {
		t.Fatalf("Timed out waiting for %s to connect to the database: %v", podPrefix, retryErr)
	}
	t.Logf("%s successfully connected to the database", podPrefix)
}
