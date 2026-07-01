package matviewquery

import (
	"sort"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/openshift/sippy/pkg/util/sets"
)

// mergeVariants returns a copy of base with overrides merged in.
func mergeVariants(base, overrides map[string][]string) map[string][]string {
	merged := maps.Clone(base)
	maps.Copy(merged, overrides)
	return merged
}

// excludeKeys returns a copy of keys with any entries in exclude removed.
func excludeKeys(keys, exclude []string) []string {
	if len(exclude) == 0 {
		return keys
	}
	excludeSet := sets.NewString(exclude...)
	var result []string
	for _, k := range keys {
		if !excludeSet.Has(k) {
			result = append(result, k)
		}
	}
	return result
}

func parseVariantKey(variantKey string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(variantKey, ",") {
		if k, v, ok := strings.Cut(pair, ":"); ok {
			result[k] = v
		}
	}
	return result
}

func variantMapToSortedSlice(m map[string]string) []string {
	result := make([]string, 0, len(m))
	for k, v := range m {
		result = append(result, k+":"+v)
	}
	sort.Strings(result)
	return result
}
