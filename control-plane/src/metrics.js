// A small Prometheus text-format registry. Every series carries an org
// label, so one organization's view can be cut out of the whole.
export class Metrics {
  constructor() {
    this.families = new Map(); // name -> { help, type, series: Map(key -> { labels, value }) }
  }

  define(name, type, help) {
    if (!this.families.has(name)) this.families.set(name, { help, type, series: new Map() });
    return this;
  }

  inc(name, labels, n = 1) {
    const s = this.#series(name, labels);
    s.value += n;
  }

  set(name, labels, v) {
    this.#series(name, labels).value = v;
  }

  // reset drops a family's series, for gauges recomputed on each scrape.
  reset(name) {
    this.families.get(name)?.series.clear();
  }

  #series(name, labels) {
    const f = this.families.get(name);
    if (!f) throw new Error(`metric ${name} is not defined`);
    const key = JSON.stringify(Object.entries(labels).sort());
    let s = f.series.get(key);
    if (!s) {
      s = { labels, value: 0 };
      f.series.set(key, s);
    }
    return s;
  }

  // render returns the text exposition format. With org set, only that
  // organization's series are included.
  render(org) {
    const out = [];
    for (const [name, f] of this.families) {
      const rows = [...f.series.values()].filter((s) => org === undefined || s.labels.org === org);
      out.push(`# HELP ${name} ${f.help}`, `# TYPE ${name} ${f.type}`);
      for (const s of rows) {
        const labels = Object.entries(s.labels)
          .map(([k, v]) => `${k}="${String(v).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`)
          .join(',');
        out.push(`${name}{${labels}} ${s.value}`);
      }
    }
    return out.join('\n') + '\n';
  }
}
