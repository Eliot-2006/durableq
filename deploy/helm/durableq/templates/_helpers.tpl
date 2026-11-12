{{- define "durableq.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}{{ .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- end -}}

{{- define "durableq.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* selector labels; call with (dict "ctx" . "component" "api") */}}
{{- define "durableq.selectorLabels" -}}
app.kubernetes.io/name: {{ .ctx.Chart.Name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "durableq.labels" -}}
{{ include "durableq.selectorLabels" . }}
app.kubernetes.io/version: {{ .ctx.Values.image.tag | default .ctx.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .ctx.Chart.Name .ctx.Chart.Version | replace "+" "_" }}
{{- end -}}

{{/* Secret holding DATABASE_URL (and POSTGRES_PASSWORD when generated). */}}
{{- define "durableq.secretName" -}}
{{ .Values.database.existingSecret | default (printf "%s-db" (include "durableq.fullname" .)) }}
{{- end -}}

{{/* Password: explicit value, else the one already in the cluster, else random. */}}
{{- define "durableq.pgPassword" -}}
{{- if .Values.postgresql.auth.password -}}
{{- .Values.postgresql.auth.password -}}
{{- else -}}
{{- $s := lookup "v1" "Secret" .Release.Namespace (include "durableq.secretName" .) -}}
{{- if and $s $s.data (index $s.data "POSTGRES_PASSWORD") -}}
{{- index $s.data "POSTGRES_PASSWORD" | b64dec -}}
{{- else -}}
{{- randAlphaNum 24 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "durableq.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "durableq.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{- define "durableq.dbEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "durableq.secretName" . }}
      key: {{ .Values.database.secretKey }}
{{- end -}}

{{/* D17: migrations run as an init container (advisory-locked, idempotent). */}}
{{- define "durableq.migrateInit" -}}
- name: migrate
  image: {{ include "durableq.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  args: ["migrate"]
  env:
    {{- include "durableq.dbEnv" . | nindent 4 }}
  securityContext:
    {{- include "durableq.containerSecurityContext" . | nindent 4 }}
  resources:
    requests: {cpu: 10m, memory: 16Mi}
    limits: {memory: 64Mi}
{{- end -}}

{{/* Render-time validation. */}}
{{- define "durableq.validate" -}}
{{- if not (or .Values.database.existingSecret .Values.postgresql.enabled) -}}
{{- fail "set database.existingSecret or enable postgresql" -}}
{{- end -}}
{{- if and .Values.database.existingSecret .Values.postgresql.enabled -}}
{{- fail "database.existingSecret and postgresql.enabled are mutually exclusive; the in-chart Postgres needs the chart-generated Secret" -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+s$" .Values.worker.grace) -}}
{{- fail "worker.grace must be whole seconds, e.g. 10s" -}}
{{- end -}}
{{- $grace := .Values.worker.grace | trimSuffix "s" | int -}}
{{- if le (.Values.worker.terminationGracePeriodSeconds | int) (add $grace 5) -}}
{{- fail (printf "worker.terminationGracePeriodSeconds (%v) must exceed worker.grace (%s) by at least 5s" .Values.worker.terminationGracePeriodSeconds .Values.worker.grace) -}}
{{- end -}}
{{- if .Values.postgresql.enabled -}}
{{- $workers := ternary .Values.autoscaling.maxReplicas .Values.worker.replicas .Values.autoscaling.enabled | int -}}
{{- $need := add (mul $workers (.Values.worker.maxConns | int)) (mul (.Values.api.replicas | int) (.Values.api.maxConns | int)) 10 -}}
{{- if gt $need (.Values.postgresql.maxConnections | int) -}}
{{- fail (printf "connection budget exceeded: %d workers x %v + %v api x %v + 10 = %d > postgresql.maxConnections (%v)" $workers .Values.worker.maxConns .Values.api.replicas .Values.api.maxConns $need .Values.postgresql.maxConnections) -}}
{{- end -}}
{{- end -}}
{{- end -}}
