package chunk

// Chunk functions used only in tests

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/util"
)

// BenchmarkLabels is a real example from Kubernetes' embedded cAdvisor metrics, lightly obfuscated
var BenchmarkLabels = labels.NewBuilder(labels.EmptyLabels()).
	Set(model.MetricNameLabel, "container_cpu_usage_seconds_total").
	Set("beta_kubernetes_io_arch", "amd64").
	Set("beta_kubernetes_io_instance_type", "c3.somesize").
	Set("beta_kubernetes_io_os", "linux").
	Set("container_name", "some-name").
	Set("cpu", "cpu01").
	Set("failure_domain_beta_kubernetes_io_region", "somewhere-1").
	Set("failure_domain_beta_kubernetes_io_zone", "somewhere-1b").
	Set("id", "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28").
	Set("image", "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506").
	Set("instance", "ip-111-11-1-11.ec2.internal").
	Set("job", "kubernetes-cadvisor").
	Set("kubernetes_io_hostname", "ip-111-11-1-11").
	Set("monitor", "prod").
	Set("name", "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0").
	Set("namespace", "kube-system").
	Set("pod_name", "some-other-name-5j8s8").
	Labels()

// ChunksToMatrix converts a set of chunks to a model.Matrix.
func ChunksToMatrix(ctx context.Context, chunks []Chunk, from, through model.Time) (model.Matrix, error) {
	// Group chunks by series, sort and dedupe samples.
	metrics := map[model.Fingerprint]model.Metric{}
	samplesBySeries := map[model.Fingerprint][][]model.SamplePair{}
	for _, c := range chunks {
		ss, err := c.Samples(from, through)
		if err != nil {
			return nil, err
		}

		metric := util.LabelsToMetric(c.Metric)
		fingerprint := metric.Fingerprint()
		metrics[fingerprint] = metric
		samplesBySeries[fingerprint] = append(samplesBySeries[fingerprint], ss)
	}

	matrix := make(model.Matrix, 0, len(samplesBySeries))
	for fp, ss := range samplesBySeries {
		matrix = append(matrix, &model.SampleStream{
			Metric: metrics[fp],
			Values: util.MergeNSampleSets(ss...),
		})
	}

	return matrix, nil
}
