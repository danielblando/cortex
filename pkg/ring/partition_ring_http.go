package ring

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log/level"
)

const partitionPageContent = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Partition Ring Status</title>
	</head>
	<body>
		<h1>Partition Ring Status</h1>
		<p>Current time: {{ .Now }}</p>
		<p>Storage updated: {{ .StorageLastUpdated }}</p>
		<form action="" method="POST">
			<input type="hidden" name="csrf_token" value="$__CSRF_TOKEN_PLACEHOLDER__">
			{{ range $i, $p := .Partitions }}
			<h2>Partition {{ .ID }}</h2>
			<p><strong>State:</strong> {{ .State }} | <strong>Tokens:</strong> {{ .NumTokens }} | <strong>Timestamp:</strong> {{ .Timestamp }}</p>
			{{ if .Ingesters }}
			<table width="100%" border="1" style="margin-bottom: 20px;">
				<thead>
					<tr>
						<th>Ingester ID</th>
						<th>Availability Zone</th>
						<th>State</th>
						<th>Address</th>
						<th>Last Heartbeat</th>
						<th>Actions</th>
					</tr>
				</thead>
				<tbody>
					{{ range $j, $ing := .Ingesters }}
					{{ if mod $j 2 }}
					<tr>
					{{ else }}
					<tr bgcolor="#BEBEBE">
					{{ end }}
						<td>{{ .ID }}</td>
						<td>{{ .Zone }}</td>
						<td>{{ .State }}</td>
						<td>{{ .Address }}</td>
						<td>{{ .HeartbeatTimestamp }}</td>
						<td><button name="remove" value="{{ $p.ID }}:{{ .ID }}" type="submit">Remove</button></td>
					</tr>
					{{ end }}
				</tbody>
			</table>
			{{ else }}
			<p><em>No ingesters in this partition</em></p>
			{{ end }}
			{{ end }}
		</form>
	</body>
</html>`

var partitionHttpPageTemplate *template.Template

func init() {
	t := template.New("webpage")
	t.Funcs(template.FuncMap{"mod": func(i, j int) bool { return i%j == 0 }})
	partitionHttpPageTemplate = template.Must(t.Parse(partitionPageContent))
}

type partitionHttpResponse struct {
	Partitions         []partitionDesc `json:"partitions"`
	Now                time.Time       `json:"now"`
	StorageLastUpdated time.Time       `json:"storageLastUpdated"`
}

type partitionDesc struct {
	ID        string         `json:"id"`
	State     string         `json:"state"`
	Timestamp string         `json:"timestamp"`
	NumTokens int            `json:"numTokens"`
	Ingesters []ingesterInfo `json:"ingesters"`
}

type ingesterInfo struct {
	ID                 string `json:"id"`
	Zone               string `json:"zone"`
	State              string `json:"state"`
	Address            string `json:"address"`
	HeartbeatTimestamp string `json:"heartbeatTimestamp"`
}

func (r *PartitionRing) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		removeValue := req.FormValue("remove")
		if removeValue != "" {
			parts := strings.Split(removeValue, ":")
			if len(parts) == 2 {
				partitionID := parts[0]
				ingesterID := parts[1]
				if err := r.removeIngester(req.Context(), partitionID, ingesterID); err != nil {
					http.Error(w, fmt.Sprintf("Error removing ingester: %v", err), http.StatusInternalServerError)
					return
				}
			}
		}

		// Implement PRG pattern to prevent double-POST
		w.Header().Set("Location", "#")
		w.WriteHeader(http.StatusFound)
		return
	}

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	partitionIds := []string{}
	if r.desc == nil || r.desc.Partitions == nil {
		w.Write([]byte("notfound"))
		w.WriteHeader(404)
		return
	}
	for id := range r.desc.Partitions {
		partitionIds = append(partitionIds, id)
	}
	sort.Strings(partitionIds)

	storageLastUpdate := r.KVClient.LastUpdateTime(r.key)
	partitions := []partitionDesc{}

	for _, id := range partitionIds {
		p := r.desc.Partitions[id]

		// Get ingester details for this partition
		ingesters := []ingesterInfo{}
		ingesterIDs := []string{}
		for ingesterID := range p.Instances {
			ingesterIDs = append(ingesterIDs, ingesterID)
		}
		sort.Strings(ingesterIDs)

		for _, ingesterID := range ingesterIDs {
			ing := p.Instances[ingesterID]
			heartbeatTimestamp := time.Unix(ing.Timestamp, 0)
			state := ing.State.String()

			// Check if ingester is healthy
			if !r.IsHealthy(&ing, Reporting, storageLastUpdate) {
				state = "UNHEALTHY"
			}

			ingesters = append(ingesters, ingesterInfo{
				ID:                 ingesterID,
				Zone:               ing.Zone,
				State:              state,
				Address:            ing.Addr,
				HeartbeatTimestamp: heartbeatTimestamp.String(),
			})
		}

		partitions = append(partitions, partitionDesc{
			ID:        id,
			State:     FormatPartitionState(p.State),
			Timestamp: time.Unix(p.Timestamp, 0).String(),
			NumTokens: len(p.Tokens),
			Ingesters: ingesters,
		})
	}

	renderPartitionHTTPResponse(w, partitionHttpResponse{
		Partitions:         partitions,
		Now:                time.Now(),
		StorageLastUpdated: storageLastUpdate,
	}, partitionHttpPageTemplate, req)
}

func renderPartitionHTTPResponse(w http.ResponseWriter, v partitionHttpResponse, t *template.Template, r *http.Request) {
	err := t.Execute(w, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func (r *PartitionRing) removeIngester(ctx context.Context, partitionID, ingesterID string) error {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*PartitionRingDesc)
		ringDesc.RemoveIngester(partitionID, ingesterID)
		return ringDesc, true, nil
	}

	err := r.KVClient.CAS(ctx, r.key, unregister)
	if err != nil {
		level.Error(r.logger).Log("msg", "error removing ingester", "partition", partitionID, "ingester", ingesterID, "err", err)
	}
	return err
}
