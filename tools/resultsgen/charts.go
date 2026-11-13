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
	fmt.Fprintf(c, `<svg xmlns="http://www.w3.org/2000/svg" width="880" height="%d" viewBox="0 0 880 %d" role="img" aria-labelledby="title desc">
<title id="title">%s</title><desc id="desc">%s</desc>
<rect width="880" height="%d" fill="%s"/>
<g font-family="system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="15" fill="%s">
`, height, height, html.EscapeString(title), html.EscapeString(description), height, bg, c.fg)
	c.text(24, 34, title, "font-size=\"23\" font-weight=\"650\"")
	return c
}

func (c *chart) text(x, y float64, value, attrs string) {
	fmt.Fprintf(c, "<text x=\"%.2f\" y=\"%.2f\" %s>%s</text>\n", x, y, attrs, html.EscapeString(value))
}
func (c *chart) line(x1, y1, x2, y2 float64, color, dash string) {
	fmt.Fprintf(c, "<line x1=\"%.2f\" y1=\"%.2f\" x2=\"%.2f\" y2=\"%.2f\" stroke=\"%s\" stroke-width=\"2\" stroke-dasharray=\"%s\"/>\n", x1, y1, x2, y2, color, dash)
}
func (c *chart) mark(x, y float64, color string, square bool) {
	if square {
		fmt.Fprintf(c, "<rect x=\"%.2f\" y=\"%.2f\" width=\"8\" height=\"8\" fill=\"%s\"/>\n", x-4, y-4, color)
	} else {
		fmt.Fprintf(c, "<circle cx=\"%.2f\" cy=\"%.2f\" r=\"4.5\" fill=\"%s\"/>\n", x, y, color)
	}
}
func (c *chart) legend(x, y float64, label, color, dash string, square, marker bool) {
	c.line(x, y-5, x+30, y-5, color, dash)
	if marker {
		c.mark(x+15, y-5, color, square)
	}
	c.text(x+40, y, label, "")
}
func (c *chart) finish(caption, source string, y float64) string {
	c.text(24, y, caption, `font-size="13"`)
	c.text(24, y+22, "source: "+source, `font-size="12"`)
	c.WriteString("</g>\n</svg>\n")
	return c.String()
}

func plotX(n float64) float64      { return 80 + (n-1)/9*600 }
func plotY(n, max float64) float64 { return 440 - n/max*260 }
func (c *chart) axes(max, step float64, label string) {
	c.text(24, 157, label, `font-weight="600"`)
	for n := 0.0; n <= max; n += step {
		y := plotY(n, max)
		c.line(80, y, 680, y, c.grid, "")
		c.text(68, y+5, fmt.Sprintf("%.0f", n), `text-anchor="end"`)
	}
	c.line(80, 180, 80, 440, c.muted, "")
	c.line(80, 440, 680, 440, c.muted, "")
	for _, n := range []float64{1, 2, 5, 10} {
		x := plotX(n)
		c.line(x, 440, x, 446, c.muted, "")
		c.text(x, 468, fmt.Sprintf("%.0f", n), `text-anchor="middle"`)
	}
	c.text(380, 498, "Worker replicas (pods)", `text-anchor="middle"`)
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
	c := newChart("Kubernetes throughput: work per job changes the scaling limit", "Eight measured cells at 1, 2, 5 and 10 worker replicas. Solid lines join measured points. Dashed linear references are one-replica throughput multiplied by replica count; they are not measurements. Speed-ups are measured at ten replicas relative to one.", 570, dark)
	c.legend(24, 76, "0ms measured", c.blue, "", false, true)
	c.legend(340, 76, "0ms linear reference (1-replica rate × n)", c.blue, "7 5", false, false)
	c.legend(24, 110, "50ms measured", c.orange, "", true, true)
	c.legend(340, 110, "50ms linear reference (1-replica rate × n)", c.orange, "7 5", true, false)
	c.axes(5500, 1000, "Throughput (jobs/s)")
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
		c.line(plotX(1), plotY(rate, 5500), plotX(10), plotY(rate*10, 5500), color, "7 5")
		c.series(rows, sleep, "jobs_per_second", color, "", 5500, sleep == 50)
		y := plotY(number(last, "jobs_per_second"), 5500)
		c.text(700, y-3, fmt.Sprintf("%dms · %.1f×", sleep, number(last, "jobs_per_second")/rate), `font-weight="650"`)
	}
	return c.finish("demo.effect · kind / M3 Pro / Docker 11 CPUs · 10 slots, 6 DB conns/pod; one run/cell.", matrixSource, 532)
}

func cpuChart(rows []row, dark bool) string {
	c := newChart("Kubernetes CPU: Postgres carries most of the queue work", "Postgres and total worker CPU for the same eight measured workload/replica cells. Values are sampled maxima of metrics-server window averages, not instantaneous CPU peaks. Circles identify 0ms; squares identify 50ms. Solid lines are Postgres; dashed lines are all workers summed.", 570, dark)
	c.legend(24, 76, "0ms Postgres", c.blue, "", false, true)
	c.legend(390, 76, "0ms total workers", c.blue, "7 5", false, true)
	c.legend(24, 110, "50ms Postgres", c.orange, "", true, true)
	c.legend(390, 110, "50ms total workers", c.orange, "7 5", true, true)
	c.axes(8, 2, "CPU (cores)")
	for _, sleep := range []int{0, 50} {
		color := c.blue
		if sleep == 50 {
			color = c.orange
		}
		c.series(rows, sleep, "postgres_cpu_millicores", color, "", 8, sleep == 50)
		c.series(rows, sleep, "worker_cpu_millicores", color, "7 5", 8, sleep == 50)
	}
	return c.finish("demo.effect · kind / M3 Pro / Docker 11 CPUs · sampled metrics-server window averages.", matrixSource, 532)
}

func chaosChart(rows []row, dark bool) string {
	c := newChart("Kubernetes chaos: redelivery occurs; invariants hold", "Seed 44, nine fault scenarios with different workloads and job counts. Each row shows drain seconds, jobs re-executed under at-least-once delivery, and zero violations of each invariant I1 through I7. Drain times are not a controlled comparison between scenarios.", 730, dark)
	c.legend(24, 76, "Drain time", c.blue, "", false, false)
	c.legend(230, 76, "At-least-once redelivery", c.orange, "", true, false)
	c.text(24, 120, "Scenario", `font-weight="650"`)
	c.text(260, 120, "Drain time (s)", `font-weight="650"`)
	c.text(555, 120, "Re-executed (jobs)", `font-weight="650"`)
	c.text(778, 114, "I1–I7", `font-weight="650" text-anchor="middle"`)
	c.text(778, 134, "violations each", `font-size="12" text-anchor="middle"`)
	for _, n := range []float64{0, 100, 200, 300} {
		x := 260 + n/360*210
		c.line(x, 164, x, 647, c.grid, "")
		c.text(x, 156, fmt.Sprintf("%.0f", n), `font-size="12" text-anchor="middle"`)
	}
	for _, n := range []float64{0, 50, 100} {
		x := 555 + n/100*130
		c.line(x, 164, x, 647, c.grid, "")
		c.text(x, 156, fmt.Sprintf("%.0f", n), `font-size="12" text-anchor="middle"`)
	}
	for i, r := range rows {
		y := 188 + float64(i)*54
		c.text(24, y, r["scenario"].(string), `font-size="14"`)
		drain := number(r, "drain_s")
		fmt.Fprintf(c, "<rect x=\"260\" y=\"%.2f\" width=\"%.2f\" height=\"12\" fill=\"%s\"/>\n", y-11, drain/360*210, c.blue)
		c.text(532, y, fmt.Sprintf("%.2f", drain), `text-anchor="end" font-size="14"`)
		n := r["reexecuted_jobs"].(int)
		if n > 0 {
			fmt.Fprintf(c, "<rect x=\"555\" y=\"%.2f\" width=\"%.2f\" height=\"12\" fill=\"%s\"/>\n", y-11, float64(n)/100*130, c.orange)
		}
		c.text(715, y, fmt.Sprint(n), `text-anchor="end"`)
		c.text(778, y, "0", `text-anchor="middle" font-weight="650"`)
	}
	c.text(24, 666, "Different workloads per scenario; drain times are not directly comparable. Zero re-executions have no bar.", `font-size="13"`)
	return c.finish("demo.effect · kind / M3 Pro · seed 44 · leases 30s, heartbeats 10s; I1–I7 checked after drain.", chaosSource, 692)
}
