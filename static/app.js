/** @format */

// -- global
const H = window.Highcharts
const AutoComplete = window.autoComplete
const fetch = window.fetch

const plotOptions = {
  series: {
    turboThreshold: 1,
    marker: {enabled: false}
  }
}

// only do anything after 'load'
window.addEventListener('load', () => {
  // -- base.html
  new AutoComplete({
    selector: '#search',
    source(term, response) {
      if (term.length < 3) response([])

      fetch('/search?q=' + encodeURIComponent(term))
        .then(r => r.json())
        .then(res => response(res.results))
    },
    renderItem({closed, url, label, kind}) {
      return `<div class="search-item ${closed ? 'closed' : ''}">
        <a href="${url}">${kind === 'node' ? '⏺' : '⤢'} ${label}</a>
    </div>`
    },
    cache: true
  })

  // -- index.html
  if (document.getElementById('open-close')) {
    H.chart('open-close', {
      title: {text: ''},
      xAxis: {
        categories: window.blocks.map(b => b.toString().slice(0, -2) + '__')
      },
      yAxis: [
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false}
      ],
      series: [
        {
          name: 'capacity (btc)',
          type: 'area',
          data: window.capacity,
          step: 'left',
          yAxis: 2,
          color: 'var(--gold)'
        },
        {
          name: 'total',
          type: 'area',
          data: window.total,
          step: 'left',
          yAxis: 1,
          color: 'var(--blue)'
        },
        {
          name: 'openings',
          type: 'column',
          data: window.openings,
          yAxis: 0,
          color: 'var(--green)',
          borderWidth: 1
        },
        {
          name: 'closings',
          type: 'column',
          data: window.closings,
          yAxis: 0,
          color: 'var(--red)',
          borderWidth: 1
        },
        {
          name: 'onchain fee total',
          type: 'spline',
          data: window.fee_total,
          yAxis: 3,
          color: 'rgba(0, 0, 0, 0.5)',
          lineWidth: 1,
          visible: Math.random() < 0.08
        },
        {
          name: 'outstanding_htlcs',
          type: 'spline',
          data: window.outstanding_htlcs,
          yAxis: 4,
          dashStyle: 'Dash',
          color: '#c1a478',
          lineWidth: 1,
          visible: Math.random() < 0.2
        }
      ],
      plotOptions
    })
  }

  if (document.getElementById('close-types')) {
    H.chart('close-types', {
      title: {text: ''},
      chart: {type: 'areaspline'},
      xAxis: {
        categories: window.closeblocks.map(
          b => b.toString().slice(0, -2) + '__'
        )
      },
      yAxis: [
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false},
        {visible: false}
      ],
      series: [
        ['unknown', '#e4dfda'],
        ['unused', 'var(--green)'],
        ['mutual', 'var(--blue)'],
        ['force', '#f58f29'],
        ['force_unused', '#7d4600'],
        ['penalty', 'var(--red)']
      ].map(([name, color]) => ({name, data: window[name], color})),
      plotOptions: {...plotOptions, areaspline: {stacking: 'percent'}}
    })
  }
})
