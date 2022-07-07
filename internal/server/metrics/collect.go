package metrics

import (
	"github.com/VictoriaMetrics/metrics"
	"github.com/nevian427/yaacc/internal/model"
)

// собираем метрики во время работы
func MetricsCDR(metricsCh <-chan model.CDR) error {
	for {
		cdr, ok := <-metricsCh
		// канал закрыт делать больше нечего
		if !ok {
			return nil
		}
		metrics.GetOrCreateCounter("yaacc_cdr_count{source=\"" + cdr.Ip + "\"}").Add(1)
		if cdr.Vdn != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_vdn_count{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.InTrk + "\", vdn=\"" + cdr.Vdn + "\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_vdn_duration{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.InTrk + "\", vdn=\"" + cdr.Vdn + "\"}").Add(cdr.Duration)
		}
		if cdr.CodeUsed != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_count{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.CodeUsed + "\", direction=\"outbound\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_duration{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.CodeUsed + "\", direction=\"outbound\"}").Add(cdr.Duration)
		}
		if cdr.InTrk != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_count{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.InTrk + "\", direction=\"inbound\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_duration{source=\"" + cdr.Ip + "\", trunk=\"" + cdr.InTrk + "\", direction=\"inbound\"}").Add(cdr.Duration)
		}
	}
}
