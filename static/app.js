/** @format */

// -- global
var Highcharts = window.Highcharts
var autoComplete = window.autoComplete
var fetch = window.fetch
var plotOptions = {
  series: {
    turboThreshold: 1,
    marker: {enabled: false}
  }
}

// -- base.html
new autoComplete({
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
var openClose = document.getElementById('open-close')
if (openClose) {
  Highcharts.chart(openClose, {
    title: {text: ''},
    xAxis: {
      categories: window.blocks.map(b => b.toString().slice(0, -2) + '__')
    },
    yAxis: [{visible: false}, {visible: false}, {visible: false}],
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
      }
    ],
    plotOptions
  })
}

// -- node.html
var nodeHistory = document.getElementById('node-channels-history')
if (nodeHistory) {
  var blockmap = {}
  var opens = {}
  var closes = {}
  var open_sats = {}
  var close_sats = {}

  var rows = document.querySelectorAll('table.node-channels-history tbody tr')
  for (var i = rows.length - 1; i >= 0; i--) {
    var row = rows[i]
    var satoshis = parseFloat(row.children[2].innerHTML)
    var opened_at = parseInt(row.children[3].innerHTML)
    var closed_at = parseInt(row.children[4].innerHTML)

    opens[opened_at] = opens[opened_at] || 0
    open_sats[opened_at] = open_sats[opened_at] || 0
    opens[opened_at]++
    open_sats[opened_at] += satoshis
    blockmap[opened_at] = true

    if (!isNaN(closed_at)) {
      closes[opened_at] = closes[opened_at] || 0
      close_sats[opened_at] = close_sats[opened_at] || 0
      closes[opened_at]++
      close_sats[opened_at] += satoshis
      blockmap[closed_at] = true
    }
  }

  var blocks = Object.keys(blockmap).sort()
  var openings = []
  var closings = []
  var total = []
  var capacity = []

  for (i = 0; i < blocks.length; i++) {
    var b = blocks[i]
    var x = parseInt(b)
    openings.push([x, opens[b] || 0])
    closings.push([x, closes[b] || 0])
    total.push([
      x,
      (total.length > 0 ? total[i - 1][1] : 0) +
        (opens[b] || 0) -
        (closes[b] || 0)
    ])
    capacity.push([
      x,
      (capacity.length > 0 ? capacity[i - 1][1] : 0) +
        (open_sats[b] || 0) -
        (close_sats[b] || 0)
    ])
  }

  Highcharts.chart(nodeHistory, {
    title: {text: ''},
    yAxis: [{visible: false}, {visible: false}, {visible: false}],
    series: [
      {
        name: 'capacity (sat)',
        type: 'area',
        data: capacity,
        step: 'left',
        yAxis: 2,
        color: 'var(--gold)'
      },
      {
        name: 'total',
        type: 'area',
        data: total,
        step: 'left',
        yAxis: 1,
        color: 'var(--blue)'
      },
      {
        name: 'openings',
        type: 'column',
        data: openings,
        yAxis: 0,
        color: 'var(--green)',
        borderWidth: 1
      },
      {
        name: 'closings',
        type: 'column',
        data: closings,
        yAxis: 0,
        color: 'var(--red)',
        borderWidth: 1
      }
    ],
    plotOptions
  })
}
