package fake

import (
	"context"
	"strings"
	"testing"

	"github.com/kubearchive/kubearchive/pkg/models"
	"github.com/stretchr/testify/assert"
)

var testResources = []models.Resource{
	{Kind: "Crontab", ApiVersion: "stable.example.com/v1", Status: nil, Spec: nil, Metadata: map[string]interface{}{"namespace": "test", "name": "test"}},
}

func TestNewFakeDatabase(t *testing.T) {
	tests := []struct {
		name      string
		resources []models.Resource
	}{
		{
			name:      "the database is created with no resources",
			resources: nil,
		},
		{
			name:      "the database is created with test resources",
			resources: testResources,
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.resources, NewFakeDatabase(tt.resources).resources)
	}
}

func TestQueryResources(t *testing.T) {

	existingKind := testResources[0].Kind
	existingGroup := strings.Split(testResources[0].ApiVersion, "/")[0]
	existingVersion := strings.Split(testResources[0].ApiVersion, "/")[1]

	tests := []struct {
		name     string
		kind     string
		group    string
		version  string
		expected []models.Resource
	}{
		{
			name:     "No matching resources by kind",
			kind:     "NotFound",
			group:    existingGroup,
			version:  existingVersion,
			expected: nil,
		},
		{
			name:     "No matching resources by group",
			kind:     existingKind,
			group:    "not.found",
			version:  existingVersion,
			expected: nil,
		},
		{
			name:     "No matching resources by version",
			kind:     existingKind,
			group:    existingGroup,
			version:  "v2",
			expected: nil,
		},
		{
			name:     "Matching resources",
			kind:     existingKind,
			group:    existingGroup,
			version:  existingVersion,
			expected: testResources,
		},
	}

	db := NewFakeDatabase(testResources)

	for _, tt := range tests {
		filteredResources, _ := db.QueryResources(context.TODO(), tt.kind, tt.group, tt.version)
		assert.Equal(t, tt.expected, filteredResources)
	}
}

func TestQueryNamespacedResources(t *testing.T) {

	existingKind := testResources[0].Kind
	existingGroup := strings.Split(testResources[0].ApiVersion, "/")[0]
	existingVersion := strings.Split(testResources[0].ApiVersion, "/")[1]
	existingNamespace := (testResources[0].Metadata["namespace"]).(string)

	tests := []struct {
		name      string
		kind      string
		group     string
		version   string
		namespace string
		expected  []models.Resource
	}{
		{
			name:      "No matching resources by kind",
			kind:      "NotFound",
			group:     existingGroup,
			version:   existingVersion,
			namespace: existingNamespace,
			expected:  nil,
		},
		{
			name:      "No matching resources by group",
			kind:      existingKind,
			group:     "not.found",
			version:   existingVersion,
			namespace: existingNamespace,
			expected:  nil,
		},
		{
			name:      "No matching resources by version",
			kind:      existingKind,
			group:     existingGroup,
			version:   "v2",
			namespace: existingNamespace,
			expected:  nil,
		},
		{
			name:      "No matching resources by namespace",
			kind:      existingKind,
			group:     existingGroup,
			version:   existingVersion,
			namespace: "notfound",
			expected:  nil,
		},
		{
			name:      "Matching resources",
			kind:      existingKind,
			group:     existingGroup,
			version:   existingVersion,
			namespace: existingNamespace,
			expected:  testResources,
		},
	}
	db := NewFakeDatabase(testResources)

	for _, tt := range tests {
		filteredResources, _ := db.QueryNamespacedResources(context.TODO(), tt.kind, tt.group, tt.version, tt.namespace)
		assert.Equal(t, tt.expected, filteredResources)
	}
}
