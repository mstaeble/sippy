package matviewquery

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lib/pq"
)

// buildMatchingVCIDsCTE generates a CTE that selects variant_combination_ids
// matching the include_variants filter, and returns the SQL fragment and params.
func buildMatchingVCIDsCTE(cteName string, includeVariants map[string][]string) (string, []any) {
	var params []any
	var clauses []string
	var keys []string
	for k := range includeVariants {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := includeVariants[key]
		formatted := make([]string, len(values))
		for i, v := range values {
			formatted[i] = key + ":" + v
		}
		if len(formatted) == 1 {
			clauses = append(clauses, "variants @> ?::text[]")
		} else {
			clauses = append(clauses, "variants && ?::text[]")
		}
		params = append(params, pq.StringArray(formatted))
	}

	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	cte := fmt.Sprintf("%s AS (SELECT id FROM variant_combinations%s)", cteName, where)
	return cte, params
}

// buildVCParsedCTE generates a CTE that parses variant_combinations.variants
// into individual columns for the given dbGroupBy keys. Only processes
// variant combinations that are in the specified matching vcids CTE.
func buildVCParsedCTE(cteName, vcidCTEName string, dbGroupByKeys []string) string {
	var cols []string
	for _, k := range dbGroupByKeys {
		cols = append(cols, fmt.Sprintf(
			"MAX(CASE WHEN v LIKE '%s:%%' THEN split_part(v, ':', 2) END) AS %s",
			k, quoteIdent(k)))
	}
	return fmt.Sprintf(`%s AS (
		SELECT vc.id, %s
		FROM variant_combinations vc
		JOIN %s mv ON vc.id = mv.id, unnest(vc.variants) v
		GROUP BY vc.id
	)`, cteName, strings.Join(cols, ", "), vcidCTEName)
}

// quoteIdent wraps a string in double quotes for use as a SQL identifier.
func quoteIdent(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

// variantKeyExpr generates a SQL expression that builds a comma-separated
// variant key string from the dbGroupBy columns.
func variantKeyExpr(prefix string, dbGroupByKeys []string) string {
	var parts []string
	for _, k := range dbGroupByKeys {
		parts = append(parts, fmt.Sprintf("'%s:' || COALESCE(%s.%s, '')", k, prefix, quoteIdent(k)))
	}
	return "concat_ws(',', " + strings.Join(parts, ", ") + ")"
}

// dbGroupByColumns returns a comma-separated list of prefixed column references.
func dbGroupByColumns(prefix string, dbGroupByKeys []string) string {
	var cols []string
	for _, k := range dbGroupByKeys {
		cols = append(cols, prefix+"."+quoteIdent(k))
	}
	return strings.Join(cols, ", ")
}

// dbGroupByMatchClauses generates IS NOT DISTINCT FROM clauses for a LATERAL JOIN.
func dbGroupByMatchClauses(leftPrefix, rightPrefix string, dbGroupByKeys []string) string {
	var clauses []string
	for _, k := range dbGroupByKeys {
		clauses = append(clauses, fmt.Sprintf("%s.%s IS NOT DISTINCT FROM %s.%s",
			leftPrefix, quoteIdent(k), rightPrefix, quoteIdent(k)))
	}
	return strings.Join(clauses, " AND ")
}
