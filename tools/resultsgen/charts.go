package main

import (
	"fmt"
	"html"
	"strings"
)

type chart struct {
	strings.Builder
	fg, muted, grid, blue, orange string
}

func newChart(title, description string, height int, dark bool) *chart {
	c := &chart{fg: "#24292f", muted: "#57606a", grid: "#d0d7de", blue: "#0065a8", orange: "#a34500"}
	bg := "#ffffff"
	if dark {
		bg = "#0d1117"
		c.fg = "#e6edf3"
		c.muted = "#aeb9c5"
		c.grid = "#303b48"
		c.blue = "#56b4e9"
		c.orange = "#f3ab63"
	}
	fmt.Fprintf(c, `<svg xmlns="http://www.w3.org/2000/svg" width="720" height="%d" viewBox="0 0 720 %d" role="img" aria-labelledby="title desc">
<title id="title">%s</title><desc id="desc">%s</desc>
<rect width="720" height="%d" fill="%s"/>
<g font-family="system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="22" fill="%s">
`, height, height, html.EscapeString(title), html.EscapeString(description), height, bg, c.fg)
	c.text(24, 34, title, "font-size=\"26\" font-weight=\"650\"")
	return c
}

func (c *chart) text(x, y float64, value, attrs string) {
	fmt.Fprintf(c, "<text x=\"%.2f\" y=\"%.2f\" %s>%s</text>\n", x, y, attrs, html.EscapeString(value))
}
func (c *chart) line(x1, y1, x2, y2 float64, color, dash string) {
	fmt.Fprintf(c, "<line x1=\"%.2f\" y1=\"%.2f\" x2=\"%.2f\" y2=\"%.2f\" stroke=\"%s\" stroke-width=\"3\" stroke-dasharray=\"%s\"/>\n", x1, y1, x2, y2, color, dash)
}
func (c *chart) mark(x, y float64, color string, square bool) {
	if square {
		fmt.Fprintf(c, "<rect x=\"%.2f\" y=\"%.2f\" width=\"12\" height=\"12\" fill=\"%s\"/>\n", x-6, y-6, color)
	} else {
		fmt.Fprintf(c, "<circle cx=\"%.2f\" cy=\"%.2f\" r=\"6\" fill=\"%s\"/>\n", x, y, color)
	}
}
func (c *chart) legend(x, y float64, label, color, dash string, square, marker bool) {
	c.line(x, y-5, x+30, y-5, color, dash)
	if marker {
		c.mark(x+15, y-5, color, square)
	}
	c.text(x+40, y, label, "")
}
func (c *chart) finish(captions []string, source string, y float64) string {
	for _, caption := range captions {
		c.text(24, y, caption, `font-size="16"`)
		y += 24
	}
	c.text(24, y, "source: "+source, `font-size="16"`)
	c.WriteString("</g>\n</svg>\n")
	return c.String()
}

const plotLeft, plotRight, plotTop, plotBottom = 80.0, 660.0, 290.0, 650.0

func plotX(n float64) float64      { return plotLeft + (n-1)/9*(plotRight-plotLeft) }
func plotY(n, max float64) float64 { return plotBottom - n/max*(plotBottom-plotTop) }
func (c *chart) axes(max, step float64, label string) {
	c.text(24, 254, label, `font-weight="600"`)
	for n := 0.0; n <= max; n += step {
		y := plotY(n, max)
		c.line(plotLeft, y, plotRight, y, c.grid, "")
		c.text(plotLeft-12, y+7, fmt.Sprintf("%.0f", n), `text-anchor="end"`)
	}
	c.line(plotLeft, plotTop, plotLeft, plotBottom, c.muted, "")
	c.line(plotLeft, plotBottom, plotRight, plotBottom, c.muted, "")
	for _, n := range []float64{1, 2, 5, 10} {
		x := plotX(n)
		c.line(x, plotBottom, x, plotBottom+8, c.muted, "")
		c.text(x, plotBottom+36, fmt.Sprintf("%.0f", n), `text-anchor="middle"`)
	}
	c.text((plotLeft+plotRight)/2, 730, "Worker replicas (pods)", `text-anchor="middle"`)
	fmt.Fprintf(c, `<defs><clipPath id="plot-frame"><rect x="%.0f" y="%.0f" width="%.0f" height="%.0f"/></clipPath></defs>
`, plotLeft, plotTop, plotRight-plotLeft, plotBottom-plotTop)
}

func (c *chart) series(rows []row, sleep int, field, color, dash string, max float64, square bool) {
	var previous row
	for _, r := range rows {
		if number(r, "sleep_ms") != float64(sleep) {
			continue
		}
		n := number(r, field)
		if field != "jobs_per_second" {
			n /= 1000
		}
		x, y := plotX(number(r, "replicas")), plotY(n, max)
		if previous != nil {
			prev := number(previous, field)
			if field != "jobs_per_second" {
				prev /= 1000
			}
			c.line(plotX(number(previous, "replicas")), plotY(prev, max), x, y, color, dash)
		}
		c.mark(x, y, color, square)
		previous = r
	}
}

func throughputChart(rows []row, dark bool) string {
	c := newChart("Kind cluster throughput", "Eight measured cells at 1, 2, 5 and 10 worker replicas. Solid lines join measured points. Dashed linear references multiply the one-replica rate by replica count, not additional measurements. The 0ms reference is clipped at 2,200 jobs/s; at ten replicas it reaches 4,948 jobs/s. Speed-ups compare ten replicas to one.", 930, dark)
	c.legend(24, 86, "0ms measured", c.blue, "", false, true)
	c.legend(24, 124, "50ms measured", c.orange, "", true, true)
	c.legend(24, 162, "0ms linear reference (1-pod rate × n)", c.blue, "8 6", false, false)
	c.legend(24, 200, "50ms linear reference (1-pod rate × n)", c.orange, "8 6", true, false)
	const max = 2200.0
	c.axes(max, 500, "Throughput (jobs/s)")
	c.line(plotLeft, plotTop, plotRight, plotTop, c.grid, "")
	c.text(plotLeft-12, plotTop+7, fmt.Sprintf("%.0f", max), `text-anchor="end"`)
	for _, sleep := range []int{0, 50} {
		color := c.blue
		if sleep == 50 {
			color = c.orange
		}
		var first, last row
		for _, r := range rows {
			if number(r, "sleep_ms") == float64(sleep) {
				if first == nil {
					first = r
				}
				last = r
			}
		}
		rate := number(first, "jobs_per_second")
		c.WriteString(`<g clip-path="url(#plot-frame)">` + "\n")
		c.line(plotX(1), plotY(rate, max), plotX(10), plotY(rate*10, max), color, "8 6")
		c.WriteString("</g>\n")
		if sleep == 0 {
			label := fmt.Sprintf("%.0f", rate*10)
			label = label[:len(label)-3] + "," + label[len(label)-3:]
			c.text(348, 252, "0ms reference at 10:", `font-size="20"`)
			c.text(348, 278, label+" jobs/s (off scale)", `font-size="20"`)
		}
		c.series(rows, sleep, "jobs_per_second", color, "", max, sleep == 50)
		y := 782.0
		if sleep == 50 {
			y = 816
		}
		c.text(24, y, fmt.Sprintf("%dms: %.1f× throughput (1 → 10 pods)", sleep, number(last, "jobs_per_second")/rate), `font-weight="650"`)
	}
	return c.finish([]string{
		"kind cluster · M3 Pro · Docker: 11 CPUs",
		"demo.effect · 10 slots, 6 DB connections per pod · one run/cell",
	}, matrixSource, 855)
}

func cpuChart(rows []row, dark bool) string {
	c := newChart("Kind cluster CPU", "Postgres and total worker CPU for the same eight measured cells. Values are sampled maxima of metrics-server window averages, not instantaneous CPU peaks. Circles identify 0ms; squares identify 50ms. Solid lines are Postgres; dashed lines are all workers summed.", 904, dark)
	c.legend(24, 86, "0ms Postgres", c.blue, "", false, true)
	c.legend(24, 124, "50ms Postgres", c.orange, "", true, true)
	c.legend(24, 162, "0ms total workers", c.blue, "8 6", false, true)
	c.legend(24, 200, "50ms total workers", c.orange, "8 6", true, true)
	c.axes(8, 2, "CPU (cores)")
	for _, sleep := range []int{0, 50} {
		color := c.blue
		if sleep == 50 {
			color = c.orange
		}
		c.series(rows, sleep, "postgres_cpu_millicores", color, "", 8, sleep == 50)
		c.series(rows, sleep, "worker_cpu_millicores", color, "8 6", 8, sleep == 50)
	}
	return c.finish([]string{
		"kind cluster · M3 Pro · Docker: 11 CPUs",
		"demo.effect · 10 slots, 6 DB connections per pod",
		"Sampled metrics-server window averages; eight measured cells.",
	}, matrixSource, 790)
}

func chaosChart(rows []row, dark bool) string {
	c := newChart("Kind cluster chaos", "Seed 44, nine fault scenarios with different workloads and job counts. Each scenario shows drain seconds and jobs re-executed under at-least-once delivery on separate scales. Every invariant I1 through I7 has zero violations. Drain times are not a controlled comparison between scenarios.", 1220, dark)
	c.text(24, 76, "At-least-once redelivery; I1–I7 hold in every scenario.", "")
	c.text(24, 116, "Drain time (s)", `font-weight="600"`)
	c.text(384, 116, "Re-executed (jobs)", `font-weight="600"`)
	for _, n := range []float64{0, 180, 360} {
		c.text(24+n/360*236, 148, fmt.Sprintf("%.0f", n), `font-size="20"`)
	}
	for _, n := range []float64{0, 50, 100} {
		c.text(384+n/100*236, 148, fmt.Sprintf("%.0f", n), `font-size="20"`)
	}
	for i, r := range rows {
		y := 184 + float64(i)*106
		c.text(24, y, r["scenario"].(string), `font-size="24" font-weight="600"`)
		c.text(696, y, "I1–I7: 0 each", `text-anchor="end" font-size="20"`)
		c.line(24, y+30, 260, y+30, c.grid, "")
		c.line(384, y+30, 620, y+30, c.grid, "")
		drain := number(r, "drain_s")
		fmt.Fprintf(c, "<rect x=\"24\" y=\"%.2f\" width=\"%.2f\" height=\"16\" fill=\"%s\"/>\n", y+22, drain/360*236, c.blue)
		c.text(342, y+37, fmt.Sprintf("%.2f", drain), `text-anchor="end"`)
		n := r["reexecuted_jobs"].(int)
		if n > 0 {
			fmt.Fprintf(c, "<rect x=\"384\" y=\"%.2f\" width=\"%.2f\" height=\"16\" fill=\"%s\"/>\n", y+22, float64(n)/100*236, c.orange)
		}
		c.text(696, y+37, fmt.Sprint(n), `text-anchor="end"`)
	}
	return c.finish([]string{
		"Different workloads; drain times are not directly comparable.",
		"Zero re-executions have no bar. I1–I7 checked after drain.",
		"kind cluster · M3 Pro · demo.effect · seed 44",
		"Lease: 30s · heartbeat: 10s",
	}, chaosSource, 1096)
}
