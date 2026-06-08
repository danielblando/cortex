package ruler

import (
	"sort"
	"time"

	promRules "github.com/prometheus/prometheus/rules"
)

// mergeGroupStateDesc removes duplicates from the provided []*GroupStateDesc by keeping the GroupStateDesc with the
// latest information. It uses the EvaluationTimestamp of the GroupStateDesc and the EvaluationTimestamp of the
// ActiveRules in a GroupStateDesc to determine the which GroupStateDesc has the latest information.
// It also truncates rule groups if maxRuleGroups > 0
func mergeGroupStateDesc(ruleResponses []*RulesResponse, maxRuleGroups int32, dedup bool) *RulesResponse {

	// Pre-calculate total group count to avoid repeated slice growth.
	totalGroups := 0
	for _, resp := range ruleResponses {
		totalGroups += len(resp.Groups)
	}

	if !dedup {
		groups := make([]*GroupStateDesc, 0, totalGroups)
		for _, resp := range ruleResponses {
			groups = append(groups, resp.Groups...)
		}
		if maxRuleGroups > 0 {
			sort.Sort(PaginatedGroupStates(groups))
			result, nextToken := generatePage(groups, int(maxRuleGroups))
			return &RulesResponse{Groups: result, NextToken: nextToken}
		}
		return &RulesResponse{Groups: groups, NextToken: ""}
	}

	// Dedup path: keep the GroupStateDesc with the latest evaluation timestamp.
	// Use the group-level EvaluationTimestamp first (O(1) per group), and only
	// scan individual rules if two groups have identical group-level timestamps.
	states := make(map[string]*GroupStateDesc, totalGroups/2)
	rgTime := make(map[string]time.Time, totalGroups/2)

	for _, resp := range ruleResponses {
		for _, state := range resp.Groups {
			latestTs := state.EvaluationTimestamp
			key := promRules.GroupKey(state.Group.Namespace, state.Group.Name)
			ts, ok := rgTime[key]
			if !ok {
				states[key] = state
				rgTime[key] = latestTs
				continue
			}
			if ts.Before(latestTs) {
				states[key] = state
				rgTime[key] = latestTs
			} else if ts.Equal(latestTs) {
				// Only scan rules when group-level timestamps are identical.
				existingMax := findMaxRuleTimestamp(states[key])
				newMax := findMaxRuleTimestamp(state)
				if newMax.After(existingMax) {
					states[key] = state
					rgTime[key] = newMax
				}
			}
		}
	}

	groups := make([]*GroupStateDesc, 0, len(states))
	for _, state := range states {
		groups = append(groups, state)
	}

	if maxRuleGroups > 0 {
		sort.Sort(PaginatedGroupStates(groups))
		result, nextToken := generatePage(groups, int(maxRuleGroups))
		return &RulesResponse{Groups: result, NextToken: nextToken}
	}
	return &RulesResponse{Groups: groups, NextToken: ""}
}

// findMaxRuleTimestamp finds the latest EvaluationTimestamp among a group's active rules.
func findMaxRuleTimestamp(state *GroupStateDesc) time.Time {
	maxTs := state.EvaluationTimestamp
	for _, r := range state.ActiveRules {
		if r.EvaluationTimestamp.After(maxTs) {
			maxTs = r.EvaluationTimestamp
		}
	}
	return maxTs
}
