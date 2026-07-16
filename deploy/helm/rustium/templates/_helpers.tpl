{{- define "rustium.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rustium.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "rustium.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rustium.labels" -}}
helm.sh/chart: {{ include "rustium.chart" . }}
{{ include "rustium.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "rustium.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rustium.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rustium.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rustium.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "rustium.configSecretName" -}}
{{- default (printf "%s-config" (include "rustium.fullname" .)) .Values.config.existingSecret }}
{{- end }}

{{- define "rustium.persistentVolumeClaimName" -}}
{{- default (printf "%s-state" (include "rustium.fullname" .)) .Values.persistence.existingClaim }}
{{- end }}
