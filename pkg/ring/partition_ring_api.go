package ring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// RegisterPartitionRingAPI registers HTTP endpoints for managing the partition ring.
func (r *PartitionRing) RegisterPartitionRingAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/partition/", r.handlePartitionAPI)
}

func (r *PartitionRing) handlePartitionAPI(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/api/v1/partition/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		r.listPartitions(w, req)
		return
	}

	partitionID := parts[0]
	if len(parts) == 1 {
		switch req.Method {
		case http.MethodGet:
			r.getPartition(w, req, partitionID)
		case http.MethodDelete:
			r.deletePartition(w, req, partitionID)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	action := parts[1]
	switch action {
	case "readonly":
		r.setPartitionReadOnly(w, req, partitionID)
	case "active":
		r.setPartitionActive(w, req, partitionID)
	case "status":
		r.getPartitionStatus(w, req, partitionID)
	default:
		http.Error(w, "Unknown action", http.StatusBadRequest)
	}
}

func (r *PartitionRing) listPartitions(w http.ResponseWriter, req *http.Request) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		http.Error(w, "Ring not initialized", http.StatusInternalServerError)
		return
	}

	type partitionInstanceInfo struct {
		ID    string `json:"id"`
		Addr  string `json:"addr"`
		Zone  string `json:"zone"`
		State string `json:"state"`
	}

	type partitionInfo struct {
		ID        string                  `json:"id"`
		State     string                  `json:"state"`
		Instances []partitionInstanceInfo `json:"instances"`
		NumTokens int                     `json:"num_tokens"`
	}

	partitions := make([]partitionInfo, 0, len(r.desc.Partitions))
	for id, p := range r.desc.Partitions {
		instances := make([]partitionInstanceInfo, 0, len(p.Instances))
		for iid, inst := range p.Instances {
			instances = append(instances, partitionInstanceInfo{
				ID:    iid,
				Addr:  inst.Addr,
				Zone:  inst.Zone,
				State: inst.State.String(), // Using the original String() method for instance state is fine
			})
		}

		partitions = append(partitions, partitionInfo{
			ID:        id,
			State:     FormatPartitionState(p.State),
			Instances: instances,
			NumTokens: len(p.Tokens),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(partitions); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (r *PartitionRing) getPartition(w http.ResponseWriter, req *http.Request, partitionID string) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		http.Error(w, "Ring not initialized", http.StatusInternalServerError)
		return
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		http.Error(w, fmt.Sprintf("Partition %s not found", partitionID), http.StatusNotFound)
		return
	}

	type instanceInfo struct {
		ID    string `json:"id"`
		Addr  string `json:"addr"`
		Zone  string `json:"zone"`
		State string `json:"state"`
	}

	type partitionInfo struct {
		ID        string         `json:"id"`
		State     string         `json:"state"`
		Instances []instanceInfo `json:"instances"`
		NumTokens int            `json:"num_tokens"`
	}

	instances := make([]instanceInfo, 0, len(partition.Instances))
	for iid, inst := range partition.Instances {
		instances = append(instances, instanceInfo{
			ID:    iid,
			Addr:  inst.Addr,
			Zone:  inst.Zone,
			State: inst.State.String(), // Using the original String() method for instance state is fine
		})
	}

	info := partitionInfo{
		ID:        partitionID,
		State:     FormatPartitionState(partition.State),
		Instances: instances,
		NumTokens: len(partition.Tokens),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (r *PartitionRing) deletePartition(w http.ResponseWriter, req *http.Request, partitionID string) {
	if req.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.RemovePartition(partitionID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Partition %s removed", partitionID)))
}

func (r *PartitionRing) setPartitionReadOnly(w http.ResponseWriter, req *http.Request, partitionID string) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.SetPartitionState(partitionID, P_READONLY); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Partition %s set to READONLY", partitionID)))
}

func (r *PartitionRing) setPartitionActive(w http.ResponseWriter, req *http.Request, partitionID string) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if the partition has enough healthy instances
	state, err := r.CheckPartitionHealth(partitionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if state == P_NON_READY {
		http.Error(w, fmt.Sprintf("Partition %s is not healthy enough to be set to ACTIVE", partitionID), http.StatusBadRequest)
		return
	}

	if err := r.SetPartitionState(partitionID, P_ACTIVE); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Partition %s set to ACTIVE", partitionID)))
}

func (r *PartitionRing) getPartitionStatus(w http.ResponseWriter, req *http.Request, partitionID string) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		http.Error(w, "Ring not initialized", http.StatusInternalServerError)
		return
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		http.Error(w, fmt.Sprintf("Partition %s not found", partitionID), http.StatusNotFound)
		return
	}

	// Count healthy instances per zone
	healthyInstancesPerZone := make(map[string]int)
	for _, inst := range partition.Instances {
		if r.IsHealthy(&inst, Read, r.KVClient.LastUpdateTime(r.key)) {
			healthyInstancesPerZone[inst.Zone]++
		}
	}

	// Check if we have enough healthy instances across zones
	healthy := len(healthyInstancesPerZone) >= int(r.cfg.ReplicationFactor)

	// Check if we have at most one instance per zone
	zoneBalanced := true
	for _, count := range healthyInstancesPerZone {
		if count > 1 {
			zoneBalanced = false
			break
		}
	}

	type statusInfo struct {
		ID                string         `json:"id"`
		State             string         `json:"state"`
		Healthy           bool           `json:"healthy"`
		ZoneBalanced      bool           `json:"zone_balanced"`
		InstancesPerZone  map[string]int `json:"instances_per_zone"`
		ReplicationFactor int            `json:"replication_factor"`
	}

	status := statusInfo{
		ID:                partitionID,
		State:             FormatPartitionState(partition.State),
		Healthy:           healthy,
		ZoneBalanced:      zoneBalanced,
		InstancesPerZone:  healthyInstancesPerZone,
		ReplicationFactor: int(r.cfg.ReplicationFactor),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
